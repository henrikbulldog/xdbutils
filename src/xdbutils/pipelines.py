""" Delta Live Tables Pipelines """

from typing import Callable
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, input_file_name
import dlt  # pylint: disable=import-error


class Pipeline():
    """ Delta Live Tables Pipeline """

    def raw_to_bronze(
        self,
        source_system,
        entity,
        raw_data
        ):
        """ Raw to bronze """

        @dlt.table(
            comment=f"Raw to Bronze, {source_system}.{entity}",
            name=f"bronze_{entity}"
        )
        def raw_to_bronze_table():
            return ( raw_data
                .withColumn("sys_ingest_time", current_timestamp())
            )

    def bronze_to_silver(
        self,
        source_system,
        entity,
        transform: Callable[[DataFrame], DataFrame] = None,
        expectations={}
        ):
        """ Bronze to Silver, append-only """

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
        expectations={}
    ):
        """ Bronze to Silver, upsert """

        @dlt.view(name=f"view_silver_{entity}")
        @dlt.expect_all(expectations)
        def view_silver():
            df = dlt.read(f"bronze_{entity}")
            if transform:
                df = transform(df)
            return df

        dlt.create_target_table(
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
        expectations={}):
        """ Bronze to Silver """

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
