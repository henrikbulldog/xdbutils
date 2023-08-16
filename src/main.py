""" Main app module """
import os
from pyspark.sql.functions import explode
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from xdbutils import XDBUtils

spark = DatabricksSession.builder.getOrCreate()


# Use WorkspaceClient to get dbutils
dbutils = WorkspaceClient().dbutils

STORAGE_ACCOUNT = "henriktestdev"

configs = [
  f"spark.hadoop.fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net",
  f"spark.hadoop.fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
  f"spark.hadoop.fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
  f"spark.hadoop.fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net",
  f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
  "spark.hadoop.fs.azure.createRemoteFileSystemDuringInitialization",
]
print("ABFS config:")
for c in configs:
    print(c + ": " + spark.conf.get(c))

raw_path = "abfss://helen-data-platform@henriktestdev.dfs.core.windows.net/raw"

try:
    path = raw_path + "/eds/co2emis"
    print("Contents of", path)
    for f in dbutils.fs.ls(path):
        print(f.name)
except Exception as exc:
    print("Looks like ABFS driver doesn't work with Databricks Connect: ", exc)


try:
    path = "/mnt/henriktestdev/helen-data-platform/raw/eds/co2emis"
    print("Contents of", path)
    for f in dbutils.fs.ls(path):
        print(f.name)
except Exception as exc:
    print(exc)


raw_filesystem_path = f"/dbfs/mnt/henriktestdev/helen-data-platform/raw"

try:
    path = raw_filesystem_path + "/eds/co2emis"
    print("Contents of", path)
    for f in os.listdir(path):
        print(f)
except Exception as exc:
    print(exc)

# # You can run SQL commands against Unity catalog from the remote client:
df = spark.sql(f"""
SELECT record.*
FROM (
  SELECT EXPLODE(records) record
  FROM `json`.`{raw_path}/eds/co2emis` WITH (CREDENTIAL `helen-deltalake-uc-credential`)
  LIMIT 5
)""")
print("SQL result:")
df.show(truncate=False)


df = ( spark.read 
  .option("multiline", True) 
  .json(raw_path + "/eds/co2emis")
  .withColumn("record", explode("records"))
  .select("record.*")
  .limit(5)
)
print("Spark result:")
df.show(truncate=False)

# testdata = {
#     "total": 2,
#     "dataset": "CO2Emis",
#     "records": [
#         {
#             "Minutes5UTC": "2022-01-01T22:50:00",
#             "CO2Emission": 70.000000
#         },
#         {
#             "Minutes5UTC": "2022-01-01T22:55:00",
#             "CO2Emission": 65.000000
#         }
#     ]
# }

# dbutils.fs.put("/tmp/raw/testdata.json", json.dumps(testdata), True)

# print("Raw folder contents:", dbutils.fs.ls("dbfs:/tmp/raw"))


# Declare Data Lakehouse jobs
xdbutils = XDBUtils(spark, dbutils)

# Head's up: ABFS driver doesn't seem to work in remote client with this notation: abfss://<container>@<storage account>.dfs.core.windows.net
datalakehouse = xdbutils.create_datalakehouse(
        raw_path="dbfs:/tmp/raw",
        bronze_path="<bronze path>",
        silver_path="<silver path>",
        gold_path="<gold path>",
    )

job = (
    datalakehouse
    .raw2bronze_job("eds", "co2emis")
    .read_from_json(options={"multiline": True})
    .transform(lambda df: (
        df.withColumn("record", explode("records"))
        .select("record.*")
        )
    )
    .write_append(catalog="<catalog>", table="co2emis")
)

# This will not not work, you cannot use Auto Loader or streaming tables from a remote client
# job.run()