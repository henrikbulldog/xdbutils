""" Main app module """
from pyspark.sql.functions import col, explode
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from xdbutils import XDBUtils

spark = DatabricksSession.builder.getOrCreate()
dbutils = w = WorkspaceClient().dbutils
xdbutils = XDBUtils(spark, dbutils)

print("current user:", spark.sql("select current_user()").collect()[0][0])
print("Total measurements:", spark.sql("select count(*) total from dev_helen_bronze.adx_exports.telemetry_data").collect()[0][0])

source_system = "eds"
source_entity = "co2emis"
path = f"dbfs:/FileStore/henrik/raw/{source_system}/{source_entity}"

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

dlh = xdbutils.create_datalakehouse(
    catalog="dev_helen",
    raw_path="dbfs:/FileStore/henrik/raw",
    deltalake_path="dbfs:/FileStore/henrik/deltalake")

(
    dlh
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
    .write_append(table="co2emis")
)
