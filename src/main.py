""" Main app module """
import os
from pyspark.sql.functions import col, explode
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from xdbutils import XDBUtils

spark = DatabricksSession.builder.getOrCreate()
dbutils = w = WorkspaceClient().dbutils

print("current user:", spark.sql("select current_user()").collect()[0][0])
print("Total measurements:", spark.sql("select count(*) total from dev_helen_bronze.adx_exports.telemetry_data").collect()[0][0])

raw_path = "/mnt/helen-data-platform-dev/raw"
source_system = "eds"
source_entity = "co2emis"

print("Contents of ", f"{raw_path}/{source_system}/{source_entity}")
for f in dbutils.fs.ls(f"{raw_path}/{source_system}/{source_entity}"):
    print(f.name)


df = (
    spark.read
    .option("multiline", True)
    .json(f"{raw_path}/{source_system}/{source_entity}")
    .withColumn("record", explode("records"))
    .select("record.CO2Emission",
        col("record.Minutes5DK").cast("timestamp").alias("Minutes5DK"),
        "record.PriceArea")
    )

df.show(truncate=False)

source_options ={
    "multiline": True,
}


base_path = f"abfss://helen-data-platform@{os.getenv('ADFS_DEFAULT_STORAGE_ACCOUNT')}.dfs.core.windows.net"
xdbutils = XDBUtils(spark)

dlh = xdbutils.create_datalakehouse(catalog="henrik_thomsen_test", base_path=base_path)
(
    dlh
    .raw2bronze("eds", "co2emis")
    .read_from_json(options=source_options)
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

spark.sql("select * from dev_helen_bronze.eds.co2emis limit 5").show(truncate=False)
