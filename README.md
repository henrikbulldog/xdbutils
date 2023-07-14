# xdbutils
Extended dbutils (Databricks utilities).

This package provides a simple framework for a Databricks medallion data pipeline.

The package is built for Databricks runtime 13.0 or higher.

# Installing the library in a Databricks notebook
```
%pip install install git+https://github.com/henrikbulldog/xdputils.git
```

# Using the Delta Live Tables (DLT) Framework
The framework sets up a Delta Live Tables pipeline that conforms to the medallion (bronze-silver-gold) data pipeline strategy by exposing 3 methods:
- XDBUtils.pipelines.raw_to_bronze()
- XDBUtils.pipelines.bronze_to_silver()
- XDBUtils.pipelines.silver_to_gold()

## Raw to bronze
Read and transform raw data using pyspark. Call XDBUtils.pipelines.raw_to_bronze() with source system, entity and raw data DataFrame:
```
from pyspark.sql.functions import explode, col, lit

xdbutils.pipeline.raw_to_bronze(
  source_system=source_system,
  entity=entity,
  raw_data=(
    spark
    .read
    .option("multiline", True)
    .json(f"{raw_path}/{source_system}/{entity}")
    .withColumn("record", explode("records"))
    .select("record.*")
  )
)
```
This will create the table <catalog>.<source system>.bronze_<entity>, for example testing_dlt.eds.bronze_co2emis.

## Bronze to Silver
Call XDBUtils.pipelines.bronze_to_silver() with source system, entity, bronze to silver transformation method and expectations:

```
xdbutils.pipeline.bronze_to_silver(
  source_system=source_system,
  entity=entity,
  transform=lambda df: (
    df
    .select(
      col("CO2Emission").alias("value"),
      col("Minutes5UTC").cast("timestamp").alias("timestamp"),
      col("PriceArea").alias("price_area"),
      )
    ),
  expectations={
    "valid_timestamp": "timestamp IS NOT NULL",
    "valid_value": "value IS NOT NULL"
    }
  )
```
This will create the table <catalog>.<source system>.silver_<entity>, for example testing_dlt.eds.silver_co2emis.

## Silver to Gold
Call XDBUtils.pipelines.silver_to_gold() with name, source system, entity and silver to gold transformation method:

```
xdbutils.pipeline.silver_to_gold(
  name="top_10",
  source_system=source_system,
  entity=entity,
  transform=lambda df: (
    df
      .where(col("price_area") == lit("DK1"))
      .orderBy(col("value").desc())
      .limit(10)
      .select("value", "timestamp")
  )
)
```
This will create the table <catalog>.<source system>.gold_<entity>_<name>, for example testing_dlt.eds.gold_co2emis_top_10.

## Create Workflow
Create a Delta Live Tables workflow by calling XDBUtils.pipelines.create_worflow() with source system, entity, unity catalog name, source path (i.e. path to notebook or python file ion repo), databricks host and databricks token:

```
path_to_current_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_databricks_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()

workflow_settings = xdbutils.pipeline.create_workflow(
  source_system=source_system,
  entity=entity,
  catalog="testing_dlt",
  source_path=path_to_current_notebook,
  databricks_host=current_databricks_host,
  databricks_token=dbutils.secrets().get(scope="<scope>", key="<key>")
)
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
