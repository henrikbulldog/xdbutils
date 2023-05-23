""" Main app module """
import json
from pyspark.sql.functions import explode
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from xdbutils import XDBUtils

spark = DatabricksSession.builder.getOrCreate()

# You can run SQL commands against Unity catalog from the remote client:
df = spark.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 10")
print("SQL result:")
df.show(truncate=False)


# Use WorkspaceClient to get dbutils
dbutils = WorkspaceClient().dbutils

testdata = {
    "total": 2,
    "dataset": "CO2Emis",
    "records": [
        {
            "Minutes5UTC": "2022-01-01T22:50:00",
            "CO2Emission": 70.000000
        },
        {
            "Minutes5UTC": "2022-01-01T22:55:00",
            "CO2Emission": 65.000000
        }
    ]
}

dbutils.fs.put("/tmp/raw/testdata.json", json.dumps(testdata), True)

print("Raw folder contents:", dbutils.fs.ls("dbfs:/tmp/raw"))


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