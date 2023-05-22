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


datalakehouse = xdbutils.create_datalakehouse(
        raw_path=f"dbfs:/FileStore/{current_user}/datalakehouse/raw",
        bronze_path=f"abfss:/<bronze path>",
        silver_path=f"abfss:/<silver path>",
        gold_path=f"abfss:/<gold path>",
    )

job = (
    datalakehouse
    .raw2bronze_job("eds", "co2emis")
    .read_from_json(options={"multiline": True})
    .transform(lambda df: (
        df.withColumn("record", explode("records"))
        .select(
            "record.CO2Emission",
            col("record.Minutes5DK").cast("timestamp").alias("Minutes5DK"),
            "record.PriceArea"
            )
        )
    )
    .write_append(catalog=f"{current_user}_bronze", table=source_entity)
)
