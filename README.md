# xdbutils
Extended dbutils (Databricks utilities).

This package provides a simple framework for a Databricks medallion data pipeline.

The package is built for Databricks runtime 13.0 or higher.

# Installing the library in a Databricks notebook
```
%pip install install git+https://github.com/henrikbulldog/xdputils.git
```

# Using the Data Lakehouse Framework

Copy data to raw:
```
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
```

Declare a datalakehouse
```
from xdbutils import XDBUtils

xdbutils = XDBUtils(spark, dbutils)

datalakehouse = xdbutils.create_datalakehouse(
  raw_path="dbfs:/tmp/raw",
  bronze_path="<bronze path>",
  silver_path="<silver path>",
  gold_path="<gold path>",
)
```

Declare a raw to bronze job: read from json, explode nested json to tabular and append data to bronze delta table:
```
from pyspark.sql.functions import explode

raw2bronze_job = (
    datalakehouse
    .raw2bronze_job(source_system="eds", entity="co2emis")
    .read_from_json(options={"multiline": True})
    .transform(lambda df: (
        df.withColumn("record", explode("records"))
        .select(
            "record.*",
            )
        )
    )
    .write_append(table=source_entity, catalog="dev_helen_bronze")
)

raw2bronze_job.run()
```

Declare a bronze to silver job: read from bronze delta table, harmonize column names and append data to silver delta table:
```
from pyspark.sql.functions import col

bronze2silver_job = (
  datalakehouse
  .bronze2silver_job(source_system="eds", source_entity="co2emis")
  .transform(lambda df: (
    df
    .select(
      col("CO2Emission").alias("value"),
      col("Minutes5UTC").cast("timestamp").alias("timestamp"),
      col("PriceArea").alias("price_area"),
      )
    )
  )
  .write_append(table=source_entity, catalog="dev_helen_silver")

bronze2silver_job.run()    
)
```

# Using Databricks Connect
See https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect

Install Docker.

Databricks cluster runtime must be  Databricks Runtime 13.0 or 13.0 ML.

Set environment variables on your local machine:
```
DATABRICKS_HOST=https://<Databricks instance host url>
DATABRICKS_TOKEN=<Databricks token from user settings>
DATABRICKS_CLUSTER_ID=<Databricks cluster ID>
```
