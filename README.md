xdbutils - Extended dbutils (Databricks utilities).

This package provides a simple framework for setting up data pipelines following the [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture). 

You can choose to use the [Delta Live Tables framework](#using-the-delta-live-tables-dlt-framework) or [straight up python DataLakeHouse framework](#using-the-data-lakehouse-framework).

The package is built for Databricks runtime 13.0 or higher.

Table of contents:
- [Installing the library in a Databricks notebook](#installing-the-library-in-a-databricks-notebook)
- [Using the Delta Live Tables (DLT) Framework](#using-the-delta-live-tables-dlt-framework)
  * [File-based batch ingestion of append-only data](#file-based-batch-ingestion-of-append-only-data)
    + [Create the Pipeline](#create-the-pipeline)
    + [Create test data](#create-test-data)
    + [Raw to bronze](#raw-to-bronze)
    + [Bronze to Silver with append-only](#bronze-to-silver-with-append-only)
    + [Silver to Gold](#silver-to-gold)
  * [File-based batch ingestion of a slow changing dimension](#file-based-batch-ingestion-of-a-slow-changing-dimension)
    + [Create Test data](#create-test-data-1)
    + [Raw to bronze](#raw-to-bronze-1)
    + [Bronze to Silver with Upsert](#bronze-to-silver-with-upsert)
    + [Silver to Gold](#silver-to-gold-1)
  * [Event-Based Ingestion](#event-based-ingestion)
- [Using the Data Lakehouse Framework (straight up python)](#using-the-data-lakehouse-framework)
- [Using Databricks Connect](#using-databricks-connect)

# Installing the library in a Databricks notebook
Just pip install directly from the repo:

```
%pip install install git+https://github.com/henrikbulldog/xdputils.git
```

# Using the Delta Live Tables (DLT) Framework
In order to set up a Delta Live Tables pipeline, simply create a python notebook in Databricks and, depending on the use case, call one of the methods
- XDBUtils.create_dlt_batch_pipeline()
- XDBUtils.create_dlt_event_pipeline()

Here is a couple of example pipelines based on common use cases:
- File-based batch ingestion of append-only data
- File-based batch ingestion of a slow changing dimension
- Event-based ingestion

## File-based batch ingestion of append-only data
This section shows how to set up a batch pipeline for data that never changes, i.e. measurements or logs.

See also example notebook: [dlt_eds_co2emis.ipynb](https://github.com/henrikbulldog/xdbutils/blob/main/dlt_eds_co2emis.ipynb)

### Create the Pipeline
You can set up DTL to monitor a raw folder and ingest new files as they arrive.
Calling the method XDBUtils.create_dlt_batch_pipeline() will create or update a DLT workflow of type batch. 
The workflow will be named `<source_system>-<entity>`.

```python
from xdbutils import XDBUtils

xdbutils = XDBUtils(spark, dbutils)

pipeline = xdbutils.create_dlt_batch_pipeline(
  source_system="eds",
  entity="co2emis",
  catalog="testing_dlt",
  tags={
    "data_owner": "Henrik Thomsen",
    "cost_center": "123456",
    "documentation": "https://github.com/henrikbulldog/xdbutils"
  },
  databricks_token=dbutils.secrets().get(scope="<scope>", key="<key>")
)
```

XDBUtils.create_dlt_batch_pipeline() parameters:

|Parameter|Description|
|---------|-----------|
|**source_system**|Source system, this name will be used to create a schema in Unity Catalog|
|**catalog**|Name of Unity Catalog to use|
|tags|Tags will be added to the DTL workflow configuration and to Delta table description in Data Explorer|
|databricks_token|A Databricks access token for [authentication to the Databricks API](https://docs.databricks.com/en/dev-tools/auth.html#databricks-personal-access-token-authentication)|
|databricks_host|If omitted, the framework will get the host from the calling notebook context|
|source_path|If omitted, the framework will use the path to the calling notebook|

The code above will create a DLT workflow with this definition:

```json
{
    "id": "...",
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 2,
                "mode": "ENHANCED"
            }
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "PREVIEW",
    "libraries": [
        {
            "notebook": {
                "path": ".../DLT/eds/co2emis/dlt_eds_co2emis"
            }
        }
    ],
    "name": "eds-co2emis",
    "edition": "ADVANCED",
    "catalog": "testing_dlt",
    "configuration": {
        "pipelines.enableTrackHistory": "true",
        "data_owner": "Henrik Thomsen",
        "cost_center": "123456",
        "documentation": "https://github.com/henrikbulldog/xdbutils",
        "Source system": "eds",
        "Entity": "co2emis"
    },
    "target": "eds"
}
```
### Create test data
Let's get some data from a public API and save the payload in a json file:

```
curl --location 'https://api.energidataservice.dk/dataset/CO2Emis?start=2023-01-01T00%3A00&end=2023-01-02T00%3A00'
```

Response payload should look something like this:

```json
{
    "total": 576,
    "dataset": "CO2Emis",
    "records": [
        {
            "Minutes5UTC": "2023-01-01T22:55:00",
            "Minutes5DK": "2023-01-01T23:55:00",
            "PriceArea": "DK1",
            "CO2Emission": 68.000000
        },
        {
            "Minutes5UTC": "2023-01-01T22:55:00",
            "Minutes5DK": "2023-01-01T23:55:00",
            "PriceArea": "DK2",
            "CO2Emission": 68.000000
        },
        {
            "Minutes5UTC": "2023-01-01T22:50:00",
            "Minutes5DK": "2023-01-01T23:50:00",
            "PriceArea": "DK1",
            "CO2Emission": 67.000000
        },
        {
            "Minutes5UTC": "2023-01-01T22:50:00",
            "Minutes5DK": "2023-01-01T23:50:00",
            "PriceArea": "DK2",
            "CO2Emission": 67.000000
        },
      ...
    ]
}
```

### Raw to bronze
In order to ingest data from raw to bronze, call raw_to_bronze() with parameters:

|Parameter|Description|
|---------|-----------|
|**raw_base_path**|Base path to raw data files. The framework will look for files in `raw_base_path/<source_system>/<entity>`|
|**raw_format**|Raw data format; json, csv, parquet|
|options|Read options, i.e. {"header": True}|
|expectations|[DLT expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html), rows that do not meet expectations will be marked as quarantined|


DLT will maintain checkpoints in `<raw_base_path>/checkpoints` so that only new data is ingested.

```python
pipeline.raw_to_bronze(
  raw_base_path="dbfs:/FileStore/datalakehouse/raw",
  raw_format="json",
  expectations={
    "Valid dataset": "dataset = 'CO2Emis'",
    "Valid data": "records IS NOT NULL AND array_size(records) > 1",
    }
)
```

This will create the table `<catalog>.<source system>.bronze_<entity>`, for example `testing_dlt.eds.bronze_co2emis`:

```
+-------+-------+-----+-------------+-----------------------+------------+
|dataset|records|total|_rescued_data|_ingest_time           |_quarantined|
+-------+-------+-----+-------------+-----------------------+------------+
|CO2Emis|...    |576  |null         |2023-08-10 12:07:10.079|false       |
+-------+-------+-----+-------------+-----------------------+------------+
```

Bronze metadata columns:

|Column|Description|
|---------|-----------|
|_rescued_data|DLT adds this column when inferring schema from json and csv. The column contains information about failed schema ingestion attempts|
|_ingest_time|Time of ingestion|
|_quarantined|False if expectations are met, True if not|

### Bronze to Silver with append-only
Call bronze_to_silver() with parameters:

|Parameter|Description|
|---------|-----------|
|parse|A function that transforms imput data. It takes a DataFrame as input and returns a DataFrame|
|expectations|[DLT expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)|

```python
from pyspark.sql.functions import explode, col

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

This will create the table `<catalog>.<source system>.silver_<entity>`, for example `testing_dlt.eds.silver_co2emis`.

```
+-----+-------------------+----------+
|value|timestamp          |price_area|
+-----+-------------------+----------+
|68.0 |2023-01-01 22:55:00|DK1       |
|68.0 |2023-01-01 22:55:00|DK2       |
|67.0 |2023-01-01 22:50:00|DK1       |
|67.0 |2023-01-01 22:50:00|DK2       |
...
```

### Silver to Gold
Call silver_to_gold() with silver to gold transformation method, with parameters:

|Parameter|Description|
|---------|-----------|
|**name**|Gold table name prefix as in `gold_<entity>_<name>`, i.e. gold_co2emis_top_10|
|parse|A function that transforms imput data. It takes a DataFrame as input and returns a DataFrame|
|expectations|[DLT expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)|


```python
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
This will create the table `<catalog>.<source system>.gold_<entity>_<name>`, for example `testing_dlt.eds.gold_co2emis_top_10`.

```
+-----+-------------------+
|value|timestamp          |
+-----+-------------------+
|227.0|2022-12-31 23:00:00|
|224.0|2022-12-31 23:05:00|
|222.0|2022-12-31 23:10:00|
|220.0|2022-12-31 23:15:00|
|212.0|2022-12-31 23:20:00|
|207.0|2022-12-31 23:25:00|
|203.0|2022-12-31 23:30:00|
|196.0|2022-12-31 23:35:00|
|191.0|2022-12-31 23:40:00|
|182.0|2022-12-31 23:45:00|
+-----+-------------------+
```


## File-based batch ingestion of a slow changing dimension
This section shows how to set up a batch pipeline for data that changes over time, i.e. a database of employees or other transactional business data.

See also example notebook: [dlt_testcdc_employee.ipynb](https://github.com/henrikbulldog/xdbutils/blob/main/dlt_testcdc_employee.ipynb)

### Create the Pipeline
You can set up DTL to monitor a raw folder and ingest new files as they arrive.
Calling the method XDBUtils.create_dlt_batch_pipeline() will create or update a DLT workflow of type batch. 
The workflow will be named `<source_system>-<entity>`.

```python
from xdbutils import XDBUtils

xdbutils = XDBUtils(spark, dbutils)

pipeline = xdbutils.create_dlt_batch_pipeline(
  source_system="testcdc",
  entity="employee",
  catalog="testing_dlt",
  tags={
    "data_owner": "Henrik Thomsen",
    "cost_center": "123456",
    "documentation": "https://github.com/henrikbulldog/xdbutils"
  },
  databricks_token=dbutils.secrets().get(scope="key-vault-secrets-databricks", key="databricks-access-token-for-azure-devops")
)
```

XDBUtils.create_dlt_batch_pipeline() parameters:

|Parameter|Description|
|---------|-----------|
|**source_system**|Source system, this name will be used to create a schema in Unity Catalog|
|**catalog**|Name of Unity Catalog to use|
|tags|Tags will be added to the DTL workflow configuration and to Delta table description in Data Explorer|
|databricks_token|A Databricks access token for [authentication to the Databricks API](https://docs.databricks.com/en/dev-tools/auth.html#databricks-personal-access-token-authentication)|
|databricks_host|If omitted, the framework will get the host from the calling notebook context|
|source_path|If omitted, the framework will use the path to the calling notebook|

The code above will create a DLT workflow with this definition:

```json
{
    "id": "...",
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 2,
                "mode": "ENHANCED"
            }
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "PREVIEW",
    "libraries": [
        {
            "notebook": {
                "path": ".../DLT/testcdc/employee/dlt_testcdc_employee"
            }
        }
    ],
    "name": "testcdc-employee",
    "edition": "ADVANCED",
    "catalog": "testing_dlt",
    "configuration": {
        "pipelines.enableTrackHistory": "true",
        "data_owner": "Henrik Thomsen",
        "cost_center": "123456",
        "documentation": "https://github.com/henrikbulldog/xdbutils",
        "Source system": "testcdc",
        "Entity": "employee"
    },
    "target": "testcdc"
}
```
### Create Test data
Let's create some test data:

```python
import json

# Delete any existing files
dbutils.fs.rm("FileStore/datalakehouse/raw/testcdc/employee/", True)

# Delete AutoLoader checkpoints
dbutils.fs.rm("FileStore/datalakehouse/raw/checkpoints/testcdc/employee/", True)

employees1 = [
  {
    "EmpID": 1234,
    "Name": "John Doe",
    "Function": "ADM",
    "Role": "Finance",
    "Updated": "2023-01-01T11:51:11.992334"
  },
  {
    "EmpID": 1345,
    "Name": "Jane Hansen",
    "Function": "ADM",
    "Role": "Finance",
    "Updated": "2023-01-01T11:51:11.992334"
  },
  {
    "EmpID": None,
    "Name": "Invalid Row",
    "Function": "ENG",
    "Role": "Engineer",
    "Updated": "2023-01-01T11:51:11.992334"
  },
]

dbutils.fs.put(
  "FileStore/datalakehouse/raw/testcdc/employee/employee-20230101-115111.json",
  contents=json.dumps(employees1)
  )

employees2 = [
  {
    "EmpID": 1234,
    "Name": "John Doe",
    "Function": "ADM",
    "Role": "Finance",
    "Updated": "2023-01-02T12:52:12.992334"
  },
  {
    "EmpID": 1345,
    "Name": "Jane Doe",
    "Function": "ADM",
    "Role": "Finance",
    "Updated": "2023-01-02T12:52:12.992334"
  },
  {
    "EmpID": None,
    "Name": "Invalid Row",
    "Function": "ENG",
    "Role": "Engineer",
    "Updated": "2023-01-02T12:52:12.992334"
  },
  {
    "EmpID": 9999,
    "Name": "New Guy",
    "Function": "ENG",
    "Role": "Marketing",
    "Updated": "2023-01-02T12:52:12.992334"
  }, 
]

dbutils.fs.put(
  "FileStore/datalakehouse/raw/testcdc/employee/employee-20230102-125212.json",
  contents=json.dumps(employees2)
  )  
```
Test data:
```
+-----+--------+-----------+---------+--------------------------+
|EmpID|Function|Name       |Role     |Updated                   |
+-----+--------+-----------+---------+--------------------------+
|1234 |ADM     |John Doe   |Finance  |2023-01-02T12:52:12.992334|
|1345 |ADM     |Jane Doe   |Finance  |2023-01-02T12:52:12.992334|
|null |ENG     |Invalid Row|Engineer |2023-01-02T12:52:12.992334|
|9999 |ENG     |New Guy    |Marketing|2023-01-02T12:52:12.992334|
|1234 |ADM     |John Doe   |Finance  |2023-01-01T11:51:11.992334|
|1345 |ADM     |Jane Hansen|Finance  |2023-01-01T11:51:11.992334|
|null |ENG     |Invalid Row|Engineer |2023-01-01T11:51:11.992334|
+-----+--------+-----------+---------+--------------------------+
```

### Raw to bronze
In order to ingest data from raw to bronze, call raw_to_bronze() with parameters:

|Parameter|Description|
|---------|-----------|
|**raw_base_path**|Base path to raw data files. The framework will look for files in `raw_base_path/<source_system>/<entity>`|
|**raw_format**|Raw data format; json, csv, parquet|
|options|Read options, i.e. {"header": True}|
|expectations|[DLT expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html), rows that do not meet expectations will be marked as quarantined|


DLT will maintain checkpoints in `<raw_base_path>/checkpoints` so that only new data is ingested.

```python
pipeline.raw_to_bronze(
  raw_base_path="dbfs:/FileStore/datalakehouse/raw",
  raw_format="json",
  expectations={
    "Valid EmpID": "EmpID IS NOT NULL",
    }
)
```

This will create the table <catalog>.<source system>.bronze_<entity>, for example testing_dlt.testcdc.bronze_employee.

```
+-----+--------+-----------+---------+--------------------------+-------------+-----------------------+------------+
|EmpID|Function|Name       |Role     |Updated                   |_rescued_data|_ingest_time           |_quarantined|
+-----+--------+-----------+---------+--------------------------+-------------+-----------------------+------------+
|1234 |ADM     |John Doe   |Finance  |2023-01-01T11:51:11.992334|null         |2023-08-09 12:21:56.759|false       |
|1345 |ADM     |Jane Hansen|Finance  |2023-01-01T11:51:11.992334|null         |2023-08-09 12:21:56.759|false       |
|null |ENG     |Invalid Row|Engineer |2023-01-01T11:51:11.992334|null         |2023-08-09 12:21:56.759|true        |
|1234 |ADM     |John Doe   |Finance  |2023-01-02T12:52:12.992334|null         |2023-08-09 12:21:56.759|false       |
|1345 |ADM     |Jane Doe   |Finance  |2023-01-02T12:52:12.992334|null         |2023-08-09 12:21:56.759|false       |
|null |ENG     |Invalid Row|Engineer |2023-01-02T12:52:12.992334|null         |2023-08-09 12:21:56.759|true        |
|9999 |ENG     |New Guy    |Marketing|2023-01-02T12:52:12.992334|null         |2023-08-09 12:21:56.759|false       |
+-----+--------+-----------+---------+--------------------------+-------------+-----------------------+------------+
```

Bronze metadata columns:

|Column|Description|
|------|-----------|
|_rescued_data|DLT adds this column when inferring schema from json and csv. The column contains information about failed schema ingestion attempts|
|_ingest_time|Time of ingestion|
|_quarantined|False if expectations are met, True if not|

### Bronze to Silver with Upsert
Call bronze_to_silver_upsert() with parameters (see also [DLT docs](https://docs.databricks.com/en/delta-live-tables/python-ref.html#change-data-capture-with-python-in-delta-live-tables)):

|Parameter|Description|
|---------|-----------|
|**keys**|The column or combination of columns that uniquely identify a row in the source data. This is used to identify which CDC events apply to specific records in the target table|
|**sequence_by**|The column name specifying the logical order of CDC events in the source data. Delta Live Tables uses this sequencing to handle change events that arrive out of order|
|ignore_null_updates|Allow ingesting updates containing a subset of the target columns. When a CDC event matches an existing row and ignore_null_updates is True, columns with a null will retain their existing values in the target. This also applies to nested columns with a value of null. When ignore_null_updates is False, existing values will be overwritten with null values|
|apply_as_deletes|Specifies when a CDC event should be treated as a DELETE rather than an upsert. To handle out-of-order data, the deleted row is temporarily retained as a tombstone in the underlying Delta table, and a view is created in the metastore that filters out these tombstones. The retention interval can be configured with the pipelines.cdc.tombstoneGCThresholdInSeconds table property|
|apply_as_truncates|Specifies when a CDC event should be treated as a full table TRUNCATE. Because this clause triggers a full truncate of the target table, it should be used only for specific use cases requiring this functionality|
|column_list|A subset of columns to include in the target table. Use column_list to specify the complete list of columns to include|
|except_column_list|Use except_column_list to specify the columns to exclude|
|parse|A function that transforms imput data. It takes a DataFrame as input and returns a DataFrame|
|expectations|[DLT expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)|

```python
pipeline.bronze_to_silver_upsert(
  keys=["emp_id"],
  sequence_by="updated",
  parse=lambda df: df.select(
    col("EmpID").alias("emp_id"),
    col("Function").alias("function"),
    col("Name").alias("name"),
    col("Role").alias("role"),
    col("Updated").alias("updated"),
    )
  )
```

This will create the table `<catalog>.<source system>.silver_<entity>`, for example `testing_dlt.testcdc.silver_employee`.

```
+------+--------+--------+---------+--------------------------+
|emp_id|function|name    |role     |updated                   |
+------+--------+--------+---------+--------------------------+
|1234  |ADM     |John Doe|Finance  |2023-01-02T12:52:12.992334|
|1345  |ADM     |Jane Doe|Finance  |2023-01-02T12:52:12.992334|
|9999  |ENG     |New Guy |Marketing|2023-01-02T12:52:12.992334|
+------+--------+--------+---------+--------------------------+
```

### Bronze to Silver with Change Tracking
If you want to track changes to a slow changing dimension, call bronze_to_silver_track_changes() with parameters (see also [DLT docs](https://docs.databricks.com/en/delta-live-tables/python-ref.html#change-data-capture-with-python-in-delta-live-tables)):

|Parameter|Description|
|---------|-----------|
|**keys**|The column or combination of columns that uniquely identify a row in the source data. This is used to identify which CDC events apply to specific records in the target table|
|**sequence_by**|The column name specifying the logical order of CDC events in the source data. Delta Live Tables uses this sequencing to handle change events that arrive out of order|
|stored_as_scd_type|Whether to store records as SCD type 1 or SCD type 2.Set to 1 for SCD type 1 or 2 for SCD type 2. Default is set to 2|
|ignore_null_updates|Allow ingesting updates containing a subset of the target columns. When a CDC event matches an existing row and ignore_null_updates is True, columns with a null will retain their existing values in the target. This also applies to nested columns with a value of null. When ignore_null_updates is False, existing values will be overwritten with null values|
|apply_as_deletes|Specifies when a CDC event should be treated as a DELETE rather than an upsert. To handle out-of-order data, the deleted row is temporarily retained as a tombstone in the underlying Delta table, and a view is created in the metastore that filters out these tombstones. The retention interval can be configured with the pipelines.cdc.tombstoneGCThresholdInSeconds table property|
|apply_as_truncates|Specifies when a CDC event should be treated as a full table TRUNCATE. Because this clause triggers a full truncate of the target table, it should be used only for specific use cases requiring this functionality|
|column_list|A subset of columns to include in the target table. Use column_list to specify the complete list of columns to include|
|except_column_list|Use except_column_list to specify the columns to exclude|
|parse|A function that transforms imput data. It takes a DataFrame as input and returns a DataFrame|
|expectations|[DLT expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)|

```python
from pyspark.sql.functions import *

pipeline.bronze_to_silver_track_changes(
  keys=["emp_id"],
  sequence_by="updated",
  parse=lambda df: df.select(
    col("EmpID").alias("emp_id"),
    col("Function").alias("function"),
    col("Name").alias("name"),
    col("Role").alias("role"),
    col("Updated").alias("updated"),
    )
  )
  ```

This will create the table `<catalog>.<source system>.silver_<entity>_changes`, for example `testing_dlt.testcdc.silver_employee_changes`. In this example changes are tracked using SCD type 2, meaning the columns __START_AT and __END_AT specifies the time period for the row.

```
+------+--------+-----------+---------+--------------------------+--------------------------+--------------------------+
|emp_id|function|name       |role     |updated                   |__START_AT                |__END_AT                  |
+------+--------+-----------+---------+--------------------------+--------------------------+--------------------------+
|1234  |ADM     |John Doe   |Finance  |2023-01-02T12:52:12.992334|2023-01-01T11:51:11.992334|null                      |
|1345  |ADM     |Jane Hansen|Finance  |2023-01-01T11:51:11.992334|2023-01-01T11:51:11.992334|2023-01-02T12:52:12.992334|
|1345  |ADM     |Jane Doe   |Finance  |2023-01-02T12:52:12.992334|2023-01-02T12:52:12.992334|null                      |
|9999  |ENG     |New Guy    |Marketing|2023-01-02T12:52:12.992334|2023-01-02T12:52:12.992334|null                      |
+------+--------+-----------+---------+--------------------------+--------------------------+--------------------------+
```

### Silver to Gold
Call silver_to_gold() with silver to gold transformation method, with parameters:

|Parameter|Description|
|---------|-----------|
|**name**|Gold table name prefix as in `gold_<entity>_<name>`, i.e. gold_co2emis_top_10|
|parse|A function that transforms imput data. It takes a DataFrame as input and returns a DataFrame|
|expectations|[DLT expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)|

```python
pipeline.silver_to_gold(
  name="by_function",
  parse=lambda df: df.groupBy("function").agg(count("*").alias("total")),
  )

pipeline.silver_to_gold(
  name="by_role",
  parse=lambda df: df.groupBy("role").agg(count("*").alias("total")),
  )  
```

This will create the tables `<catalog>.<source system>.gold_<entity>_<name>`, for example `testing_dlt.textcdc.gold_employee_by_function` and `testing_dlt.textcdc.gold_employee_by_role`.

```
+--------+-----+
|function|total|
+--------+-----+
|ADM     |2    |
|ENG     |1    |
+--------+-----+

+---------+-----+
|role     |total|
+---------+-----+
|Finance  |2    |
|Marketing|1    |
+---------+-----+
```

## Event-Based ingestion


# Using the Data Lakehouse Framework

Copy data to raw:

```python
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

```python
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

```python
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

```python
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
