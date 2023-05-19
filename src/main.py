""" Main app module """
from pyspark.sql.functions import col, explode
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from xdbutils import XDBUtils

spark = DatabricksSession.builder.getOrCreate()
dbutils = w = WorkspaceClient().dbutils
xdbutils = XDBUtils(spark, dbutils)

current_user = spark.sql('select current_user() as user').first().user.replace('@', '-').replace('.', '-')
print("current user:", current_user)

source_system = "eds"
source_entity = "co2emis"
path = f"dbfs:/FileStore/{current_user}/datalakehouse/raw/{source_system}/{source_entity}"

print("Contents of ", path)
xdbutils.fs.ls(path)

df = (
    spark.read
    .option("multiline", True)
    .json(path)
    .withColumn("record", explode("records"))
    .select("record.CO2Emission",
        col("record.Minutes5DK").cast("timestamp").alias("Minutes5DK"),
        "record.PriceArea")
)

df.show(truncate=False)


datalakehouse = ( 
    xdbutils.create_datalakehouse(
        raw_path=f"dbfs:/FileStore/{current_user}/datalakehouse/raw",
        bronze_path=f"dbfs:/FileStore/{current_user}/datalakehouse/deltalake/bronze",
        silver_path=f"dbfs:/FileStore/{current_user}/datalakehouse/deltalake/silver",
        gold_path=f"dbfs:/FileStore/{current_user}/datalakehouse/deltalake/gold",
    )
    .raw2bronze("eds", "co2emis")
    .read_from_json(options={"multiline": True})
    .with_transform(lambda df: (
        df.withColumn("record", explode("records"))
        .select(
            "record.CO2Emission",
            col("record.Minutes5DK").cast("timestamp").alias("Minutes5DK"),
            "record.PriceArea"
            )
        )
    )
)

# Streamming not supported on remote client
# datalakehouse.write_append(catalog=f"{current_user}_bronze", table=source_entity)
