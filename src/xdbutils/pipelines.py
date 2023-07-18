""" Delta Live Tables Pipelines """

from typing import Callable
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
import dlt  # pylint: disable=import-error


class Pipeline():
    """ Delta Live Tables Pipeline """

    def __init__(self, spark):
        self.spark = spark   

    def raw_to_bronze(
        self,
        source_system,
        entity,
        raw_base_path,
        raw_format,
        options = None
        ):
        """ Raw to bronze """

        if not options:
            options = {}

        @dlt.table(
            comment=f"Raw to Bronze, {source_system}.{entity}",
            name=f"bronze_{entity}"
        )
        def raw_to_bronze_table():
            return ( self.spark.readStream.format("cloudFiles")
                .options(**options)
                .option("cloudFiles.format", raw_format)
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.schemaLocation", f"{raw_base_path}/checkpoints/{source_system}/{entity}")
                .load(f"{raw_base_path}/{source_system}/{entity}")
                .withColumn("sys_ingest_time", current_timestamp())
            )

    def bronze_to_silver(
        self,
        source_system,
        entity,
        transform: Callable[[DataFrame], DataFrame] = None,
        expectations = None
        ):
        """ Bronze to Silver, append-only """

        if not expectations:
            expectations = {}

        @dlt.table(
            comment=f"Bronze to Silver, {source_system}.{entity}",
            name=f"silver_{entity}"
        )
        @dlt.expect_all(expectations)
        def bronze_to_silver_table():
            df = dlt.read(f"bronze_{entity}")
            if transform:
                df = transform(df)
            return df
        
    def bronze_to_silver_upsert(
        self,
        source_system,
        entity,
        keys,
        sequence_by,
        transform: Callable[[DataFrame], DataFrame] = None,
        expectations = None
        ):
        """ Bronze to Silver, upsert """

        if not expectations:
            expectations = {}

        @dlt.view(name=f"view_silver_{entity}")
        @dlt.expect_all(expectations)
        def view_silver():
            df = dlt.read_stream(f"bronze_{entity}")
            if transform:
                df = transform(df)
            return df

        dlt.create_streaming_table(
            name=f"silver_{entity}",
            comment=f"Bronze to Silver, {source_system}.{entity}",
            )

        dlt.apply_changes(
            target=f"silver_{entity}",
            source=f"view_silver_{entity}",
            keys=keys,
            sequence_by=col(sequence_by)
            )

    def silver_to_gold(
        self,
        name,
        source_system,
        entity,
        transform: Callable[[DataFrame], DataFrame] = None,
        expectations = None
        ):
        """ Bronze to Silver """

        if not expectations:
            expectations = {}

        @dlt.table(
            comment=f"Silver to Gold, {source_system}.{entity}_{name}",
            name=f"gold_{entity}_{name}"
        )
        @dlt.expect_all(expectations)
        def silver_table():
            df = dlt.read(f"silver_{entity}")
            if transform:
                df = transform(df)
            return df

    def create_workflow(
        self,
        source_system,
        entity,
        catalog,
        source_path,
        databricks_host,
        databricks_token
        ):
        """ Create Delta Live Tables Workflow """

        workflow_settings = {
            "name": f"{source_system}-{entity}",
            "edition": "Advanced",
            "development": True,
            "clusters": [
                {
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 5,
                    "mode": "ENHANCED"
                }
                }
            ],
            "libraries": [
                {
                "notebook": {
                    "path": source_path
                }
                }
            ],
            "catalog": catalog,
            "target": source_system,
            "continuous": False
        }

        response = requests.post(
            url=f"https://{databricks_host}/api/2.0/pipelines",
            json=workflow_settings,
            headers={"Authorization": f"Bearer {databricks_token}"},
            timeout=60
            )

        response.raise_for_status()
