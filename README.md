# xdbutils
Extended dbutils (Databricks utilities).

This package provides a simple framework for a Databricks medallion data pipeline.

The package is built for Databricks runtime 13.0 or higher.

# Installing the library in a Databricks notebook
```
%pip install install git+https://github.com/henrikbulldog/xdputils.git
```

# Using the Delta Live Tables (DLT) Framework
In order to set up a Delta Live Tables pipeline, simply create a python notebook in Databricks and add the following commands.

## Create Batch DLT Pipeline for ingesting files
You can set up DTL to monitor a raw folder and ingest new files as they arrive.
Calling the method XDBUtils.create_dlt_batch_pipeline() will create or update a DLT workflow of type batch. 
The workflow will be named <source_system>-<entity>.
Tables will be created in Unity catalog specified in parameter catalog.

Optional parameters:
- databricks_host: If omitted, the framework will get the host from the calling notebook context
- source_path: If omitted, the framework will use the path to the calling notebook

```
from xdbutils import XDBUtils

xdbutils = XDBUtils(spark, dbutils)

pipeline = xdbutils.create_dlt_batch_pipeline(
  source_system="eds",
  entity="co2emis",
  catalog="testing_dlt",
  data_owner="Henrik Thomsen",
  databricks_token=dbutils.secrets().get(scope="<scope>", key="<key>")
)
```

## Raw to bronze
Read new raw data.
DLT will maintain checkpoints in <raw_base_path>/checkpoints so that only new data is ingested.
```
pipeline.raw_to_bronze(
  raw_base_path="dbfs:/FileStore/henrik-thomsen/data/raw",
  raw_format="json"
)
```
This will create the table <catalog>.<source system>.bronze_<entity>, for example testing_dlt.eds.bronze_co2emis.

## Bronze to Silver with append-only
Call bronze_to_silver() with  bronze to silver transformation method and expectations:

```
from pyspark.sql.functions import explode, col, lit

pipeline.bronze_to_silver(
  parse=lambda df: (
    df
    .withColumn("record", explode("records"))
    .select(
      col("record.CO2Emission").alias("value"),
      col("record.Minutes5UTC").cast("timestamp").alias("timestamp"),
      col("record.PriceArea").alias("price_area"),
      )
    ),
  expectations={
    "valid_timestamp": "timestamp IS NOT NULL",
    "valid_value": "value IS NOT NULL"
    }
  )
```
This will create the table <catalog>.<source system>.silver_<entity>, for example testing_dlt.eds.silver_co2emis.

## Bronze to Silver with upsert
In order to upsert data in silver table, call bronze_to_silver_upsert().

Set parameter key to a combination of columns that uniquely identifies a row.
Set parameter sequence_by to a column that can be used to sequence dublicates (rows with same key), DLT will pick the rows with the latest sequence.

```
from pyspark.sql.functions import col

pipeline.bronze_to_silver_upsert(
  keys=["iso_code"],
  sequence_by="date",
  parse=lambda df: (
    df    
    .select(
      col("iso_code"),
      col("total_cases").cast("float"),
      col("date").cast("Date").alias("date"),
      )
    ),
  expectations={
    "valid_timestamp": "iso_code IS NOT NULL",
    "valid_value": "date IS NOT NULL"
    }
  )
  ```

## Silver to Gold
Call silver_to_gold() with silver to gold transformation method:

```
pipeline.silver_to_gold(
  name="top_10",
  parse=lambda df: (
    df
      .where(col("price_area") == lit("DK1"))
      .orderBy(col("value").desc())
      .limit(10)
      .select("value", "timestamp")
  )
)
```
This will create the table <catalog>.<source system>.gold_<entity>_<name>, for example testing_dlt.eds.gold_co2emis_top_10.


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
