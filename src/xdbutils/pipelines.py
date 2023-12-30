""" Delta Live Tables Pipelines """

import time
from functools import reduce
from typing import Callable
import urllib
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, expr, lit

from xdbutils.deprecation import deprecated

try:
    import dlt  # type: ignore
except ImportError:
    from unittest.mock import MagicMock
    class MockDlt:
        """ Mock dlt module, only works in Databricks notebooks """
        def __getattr__(self, name):
            return MagicMock()

        def __call__(self, *args, **kwargs):
            return MagicMock()
    dlt = MockDlt()


class DLTPipeline():
    """ Delta Live Tables Pipeline """

    def __init__(
        self,
        spark,
        dbutils,
        source_system,
        entity,
        catalog,
        tags = None,
        continuous_workflow = False,
        databricks_token = None,
        databricks_host = None,
        source_path = None,
        create_or_update = True,
        ):

        self.spark = spark
        self.dbutils = dbutils
        self.source_system = source_system
        self.source_path = source_path
        self.entity = entity
        self.catalog = catalog
        self.continuous_workflow = continuous_workflow
        self.tags = tags
        self.databricks_token = databricks_token
        self.databricks_host = databricks_host

        try:
            if not self.databricks_host:
                self.databricks_host = spark.conf.get("spark.databricks.workspaceUrl")
        except Exception as exc: # pylint: disable=broad-exception-caught
            print("Could not get databricks host from notebook context,", 
                "please specify databricks_host.", exc)
            return

        try:
            if not self.source_path:
                self.source_path = (
                    dbutils.notebook.entry_point.getDbutils()
                    .notebook().getContext()
                    .notebookPath().get()
                )
        except Exception as exc: # pylint: disable=broad-exception-caught
            print("Could not get source path from notebook context,",
                "please specify source_path.", exc)
            return


        if not self.tags:
            self.tags = {}
        self.tags["Source system"] = self.source_system
        self.tags["Entity"] = self.entity

        if create_or_update:
            self.create_or_update()

    def raw_to_bronze(
        self,
        raw_base_path,
        raw_format,
        source_entity = None,
        target_entity = None,
        options = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
        ):
        """ Raw to bronze """

        if not source_entity:
            source_entity = self.entity
        if not target_entity:
            target_entity = self.entity
        if not options:
            options = {}
        if not partition_cols:
            partition_cols = []
        if not expectations:
            expectations = {}
            quarantine_rules = None
        else:
            quarantine_rules = f'NOT(({") AND (".join(expectations.values())}))'

        @dlt.table(
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            name=f"bronze_{target_entity}",
            table_properties={
                "quality": "bronze",
                "pipeline.reset.allow": "false"
            },
            partition_cols=partition_cols,
        )
        def dlt_table():
            result_df = ( self.spark.readStream.format("cloudFiles")
                .options(**options)
                .option("cloudFiles.format", raw_format)
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option(
                    "cloudFiles.schemaLocation",
                    f"{raw_base_path}/checkpoints/{self.source_system}/{source_entity}"
                    )
                .load(f"{raw_base_path}/{self.source_system}/{source_entity}")
                .transform(parse)
                .withColumn("_ingest_time", current_timestamp())
                .withColumn("_quarantined", lit(False))
            )

            if quarantine_rules:
                result_df = result_df.withColumn("_quarantined", expr(quarantine_rules))

            return result_df

    def event_to_bronze(
        self,
        eventhub_namespace,
        eventhub_group_id,
        eventhub_name,
        eventhub_connection_string,
        starting_offsets = None,
        target_entity = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
    ):
        """ Event to bronze """

        if not starting_offsets:
            starting_offsets = "latest"
        if not target_entity:
            target_entity = self.entity
        if not partition_cols:
            partition_cols = []
        if not expectations:
            expectations = {}
            quarantine_rules = None
        else:
            quarantine_rules = f'NOT(({") AND (".join(expectations.values())}))'

        kafka_options = {
            "kafka.bootstrap.servers"  : f"{eventhub_namespace}.servicebus.windows.net:9093",
            "subscribe"                : eventhub_name,
            "kafka.group.id"           : eventhub_group_id,
            "kafka.sasl.mechanism"     : "PLAIN",
            "kafka.security.protocol"  : "SASL_SSL",
            "kafka.sasl.jaas.config"   : 
                "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule" +
                " required username=\"$ConnectionString\"" +
                f" password=\"{eventhub_connection_string}\";",
            "kafka.request.timeout.ms" : "6000",
            "kafka.session.timeout.ms" : "6000",
            "maxOffsetsPerTrigger"     : "600",
            "failOnDataLoss"           : 'true',
            "startingOffsets"          : starting_offsets,
        }

        @dlt.create_table(
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            name=f"bronze_{target_entity}",
            table_properties={
                "quality": "bronze",
                "pipeline.reset.allow": "false"
            },
            partition_cols=partition_cols,
        )
        def dlt_table():
            result_df = (
                self.spark.readStream
                .format("kafka")
                .options(**kafka_options)
                .load()
                .transform(parse)
                .withColumn("_ingest_time", current_timestamp())
                .withColumn("_quarantined", lit(False))
            )

            if quarantine_rules:
                result_df = result_df.withColumn("_quarantined", expr(quarantine_rules))

            return result_df

    def bronze_to_silver_append(
        self,
        source_entities = None,
        target_entity = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
        ):
        """ Bronze to Silver, append only """

        if not partition_cols:
            partition_cols = []
        if not source_entities:
            source_entities = [self.entity]
        if not target_entity:
            target_entity = self.entity

        if not partition_cols:
            partition_cols = []

        if not expectations:
            expectations = {}

        @dlt.table(
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            name=f"silver_{target_entity}",
            table_properties={
                "quality": "silver"
            },
            partition_cols=partition_cols,
        )
        @dlt.expect_all(expectations)
        def dlt_table():

            silver_df = (
                self.__union_streams([f"bronze_{t}" for t in source_entities])
                .transform(parse)
                .where(col("_quarantined") == lit(False))
                .drop("_quarantined")
            )

            if "_rescued_data" in silver_df.schema.fieldNames():
                silver_df = silver_df.drop("_rescued_data")

            return silver_df

    def bronze_to_silver_upsert(
        self,
        keys,
        sequence_by,
        source_entities = None,
        target_entity = None,
        ignore_null_updates = False,
        apply_as_deletes = None,
        apply_as_truncates = None,
        column_list = None,
        except_column_list = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
        ):
        """ Bronze to Silver, upsert """

        if not partition_cols:
            partition_cols = []
        if not source_entities:
            source_entities = [self.entity]
        if not target_entity:
            target_entity = self.entity

        if not partition_cols:
            partition_cols = []

        if not expectations:
            expectations = {}

        @dlt.view(name=f"view_silver_{target_entity}")
        @dlt.expect_all(expectations)
        def dlt_view():
            silver_df = (
                self.__union_streams([f"bronze_{t}" for t in source_entities])
                .transform(parse)
                .where(col("_quarantined") == lit(False))
                .drop("_quarantined")
            )

            if "_rescued_data" in silver_df.schema.fieldNames():
                silver_df = silver_df.drop("_rescued_data")

            return silver_df
        
        dlt.create_streaming_table(
            name=f"silver_{target_entity}",
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            table_properties={
                "quality": "bronze"
            },
            partition_cols=partition_cols,
            )

        dlt.apply_changes(
            target=f"silver_{target_entity}",
            source=f"view_silver_{target_entity}",
            keys=keys,
            sequence_by=col(sequence_by),
            ignore_null_updates=ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=column_list,
            except_column_list=except_column_list,
            )

    def bronze_to_silver_track_changes(
        self,
        keys,
        sequence_by,
        source_entities = None,
        target_entity = None,
        ignore_null_updates = False,
        apply_as_deletes = None,
        apply_as_truncates = None,
        column_list = None,
        except_column_list = None,
        track_history_column_list = None,
        track_history_except_column_list = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
        ):
        """ Bronze to Silver, change data capture,
        see https://docs.databricks.com/en/delta-live-tables/cdc.html """

        if not source_entities:
            source_entities = [self.entity]
        if not target_entity:
            target_entity = self.entity
        if not partition_cols:
            partition_cols = []

        if not partition_cols:
            partition_cols = []

        if not expectations:
            expectations = {}

        if not track_history_except_column_list:
            track_history_except_column_list = [sequence_by]

        @dlt.view(name=f"view_silver_{target_entity}_changes")
        @dlt.expect_all(expectations)
        def dlt_view():
            silver_df = (
                self.__union_streams([f"bronze_{t}" for t in source_entities])
                .transform(parse)
                .where(col("_quarantined") == lit(False))
                .drop("_quarantined")
            )

            if "_rescued_data" in silver_df.schema.fieldNames():
                silver_df = silver_df.drop("_rescued_data")

            return silver_df

        dlt.create_streaming_table(
            name=f"silver_{target_entity}_changes",
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            table_properties={
                "quality": "bronze"
            },
            partition_cols=partition_cols,
            )

        dlt.apply_changes(
            target=f"silver_{target_entity}_changes",
            source=f"view_silver_{target_entity}_changes",
            keys=keys,
            sequence_by=col(sequence_by),
            stored_as_scd_type="2",
            ignore_null_updates=ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=column_list,
            except_column_list=except_column_list,
            track_history_column_list=track_history_column_list,
            track_history_except_column_list=track_history_except_column_list
            )

    @deprecated
    def bronze_to_silver(
        self,
        source_entities = None,
        target_entity = None,
        keys = None,
        sequence_by = None,
        ignore_null_updates = False,
        apply_as_deletes = None,
        apply_as_truncates = None,
        column_list = None,
        except_column_list = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
        ):
        """ Bronze to Silver, append (if no keys) or upsert
        (if keys and sequence_by is specified) """

        if not partition_cols:
            partition_cols = []
        if not source_entities:
            source_entities = [self.entity]
        if not target_entity:
            target_entity = self.entity

        if keys and len(keys) > 0:

            if not sequence_by:
                raise ValueError("sequence_by must be specified for upserts")

            self.bronze_to_silver_upsert(
                source_entities=source_entities,
                target_entity=target_entity,
                keys=keys,
                sequence_by=sequence_by,
                ignore_null_updates=ignore_null_updates,
                apply_as_deletes=apply_as_deletes,
                apply_as_truncates=apply_as_truncates,
                column_list=column_list,
                except_column_list=except_column_list,
                parse=parse,
                partition_cols=partition_cols,
                expectations=expectations,
            )

        else:
            self.bronze_to_silver_append(
                source_entities=source_entities,
                target_entity=target_entity,
                parse=parse,
                partition_cols=partition_cols,
                expectations=expectations,
            )

    def silver_to_gold(
        self,
        name,
        source_entities = None,
        target_entity = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        expectations = None
        ):
        """ Bronze to Silver """

        if not source_entities:
            source_entities = [self.entity]
        if not target_entity:
            target_entity = self.entity
        if not expectations:
            expectations = {}

        @dlt.table(
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            name=f"gold_{target_entity}_{name}",
            table_properties={
                "quality": "gold"
            },
        )
        @dlt.expect_all(expectations)
        def dlt_table():
            return (
                self.__union_tables([f"silver_{t}" for t in source_entities])
                .transform(parse)
            )

    def create_or_update(
        self,
        ):
        """ Create Delta Live Tables Workflow """

        try:
            workflow_settings = self.__compose_settings(
                continuous_workflow=self.continuous_workflow,
                )

            pipeline_id = self.__get_id()

            if pipeline_id:
                self.__update(
                    pipeline_id=pipeline_id,
                    workflow_settings=workflow_settings,
                    )
            else:
                self.__create(
                    workflow_settings=workflow_settings,
                    )
        except Exception as exc: # pylint: disable=broad-exception-caught
            # Cannot get information from notebook context, give up
            print("Could not create DLT workflow.", exc)
            return

        self.__wait_until_completed(pipeline_id)

    def delete_persons(
        self,
        id_column,
        ids,
    ):
        pipeline_id = self.__get_id()
        assert pipeline_id, f"Pipeline {self.source_system}-{self.entity} not found"

        update_id = self.__get_latest_update(
            pipeline_id=pipeline_id,
        )
        assert update_id, f"Pipeline {self.source_system}-{self.entity}: latest update not found"

        self.__stop(pipeline_id=pipeline_id)

        save_continuous_workflow = self.continuous_workflow
        if self.continuous_workflow:
            print(f"Stopping pipeline {self.source_system}-{self.entity} and setting pipeline mode to triggered instead of continuous")
            self.continuous_workflow = False
            self.create_or_update()

        datasets = self.__get_datasets(
            pipeline_id=pipeline_id,
            update_id=update_id,
        )
        assert datasets, f"Pipeline {self.source_system}-{self.entity}: cannot find datasets"
        bronze_tables = [d for d in list(datasets) if d.startswith("bronze_")]
        non_bronze_tables = [
            d
            for d in list(datasets)
            if not d.startswith("bronze_") and not d.startswith("view_")
        ]

        for bronze_table in bronze_tables:
            ids_string = ",".join([f"'{id}'" for id in ids])
            statement = f"delete from {self.catalog}.{self.source_system}.{bronze_table} where `{id_column}` in ({ids_string})"
            print(statement)
            self.spark.sql(statement)
        
        print(f"Running pipeline {self.source_system}-{self.entity} with full refresh of: {', '.join(non_bronze_tables)}")
        self.__refresh(
            pipeline_id=pipeline_id,
            full_refresh_selection=non_bronze_tables,
        )

        self.__wait_until_completed(pipeline_id)

        if self.continuous_workflow != save_continuous_workflow:
            print(f"Updating pipeline {self.source_system}-{self.entity}, setting pipeline mode back to continuous")
            self.continuous_workflow = save_continuous_workflow
            self.create_or_update()

        tables = [d for d in list(datasets) if not d.startswith("view_")]
        for table in tables:
            df = self.spark.sql(f"select * from {self.catalog}.{self.source_system}.{table}")
            if any(column == id_column for column in df.columns):
                print(f"Checking table {table} for {id_column} in {ids}")
                assert (
                    df.where(col(id_column).isin(ids)).count() == 0
                ), f"{table} contains {id_column} in {ids}"

    def expose_tables(self, silver_catalog, gold_catalog):
        """ Expose DLT silver and gold tables from DLT catalog to other catalogs. """

        schemas = [r[0] for r in (
                   self.spark.sql(f"show schemas in {self.catalog}")
                   .select("databaseName"))
                   .collect()]
        for schema in schemas:
            tables = [r[0] for r in (
                self.spark.sql(f"show tables in {self.catalog}.{schema}")
                .select("tableName")
                ).collect()]
            for table in tables:
                parts = table.split("_")
                if len(parts) > 1:
                    if parts[0] == "silver":
                        self.spark.sql(f"create schema if not exists {silver_catalog}.{schema}")
                        self.spark.sql(f"""
                        create view if not exists {silver_catalog}.{schema}.{table.replace("silver_", "")}
                        as
                        select *
                        from {self.catalog}.{schema}.{table}
                        """)
                    if parts[0] == "gold":
                        self.spark.sql(f"create schema if not exists {gold_catalog}.{schema}")
                        self.spark.sql(f"""
                        create view if not exists {silver_catalog}.{schema}.{table.replace("gold_", "")}
                        as
                        select *
                        from {self.catalog}.{schema}.{table}
                        """)

    def __union_streams(self, sources):
        source_tables = [dlt.read_stream(t) for t in sources]
        unioned = reduce(lambda x,y: x.unionAll(y), source_tables)
        return unioned

    def __union_tables(self, sources):
        source_tables = [dlt.read(t) for t in sources]
        unioned = reduce(lambda x,y: x.unionAll(y), source_tables)
        return unioned

    def __wait_until_completed(self, pipeline_id):
        update_id = self.__get_latest_update(
            pipeline_id=pipeline_id,
        )
        assert update_id, f"Pipeline {self.source_system}-{self.entity}: latest update not found"

        for x in range(60):
            time.sleep(10)
            progress = self.__get_progress(
                pipeline_id=pipeline_id,
                update_id=update_id,
            )
            print(f"{self.source_system}-{self.entity}, update_id: {update_id}, progress: {progress}")
            if progress:
                assert progress.lower() != "failed", f"Pipeline {self.source_system}-{self.entity}: update failed"
                if progress.lower() in ["completed", "canceled", "running"]:
                    break

    def __get_id(
        self,
        ):
        name = f"{self.source_system}-{self.entity}"
        params = urllib.parse.urlencode(
            {"filter": f"name LIKE '{name}'"},
            quote_via=urllib.parse.quote)

        response = requests.get(
            url=f"https://{self.databricks_host}/api/2.0/pipelines",
            params=params,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60
            )

        if response.status_code == 200:
            payload = response.json()
            if "statuses" in payload.keys():
                if len(payload["statuses"]) == 1:
                    return payload["statuses"][0]["pipeline_id"]

        return None

    def __compose_settings(
        self,
        continuous_workflow,
        ):

        settings = {
            "name": f"{self.source_system}-{self.entity}",
            "edition": "Advanced",
            "development": True,
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
            "channel": "PREVIEW",
            "libraries": [
                {
                "notebook": {
                    "path": self.source_path
                }
                }
            ],
            "catalog": self.catalog,
            "target": self.source_system,
            "configuration": {
                "pipelines.enableTrackHistory": "true"
            },
            "continuous": continuous_workflow
        }

        settings["configuration"].update(self.tags)

        return settings

    def __update(
        self,
        pipeline_id,
        workflow_settings,
        ):

        response = requests.put(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}",
            json=workflow_settings,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60
            )

        response.raise_for_status()

    def __create(
        self,
        workflow_settings,
        ):

        response = requests.post(
            url=f"https://{self.databricks_host}/api/2.0/pipelines",
            json=workflow_settings,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60
            )

        response.raise_for_status()

    def __get_latest_update(
        self,
        pipeline_id,
    ):
        params = urllib.parse.urlencode(
            {
                "order_by": "timestamp desc",
                "max_results": 100,
            },
            quote_via=urllib.parse.quote,
        )

        response = requests.get(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/events",
            params=params,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        payload = response.json()

        updates = [
            e["origin"]["update_id"]
            for e in payload["events"]
            if e["event_type"] == "create_update"
        ]

        if not updates:
            return None

        return next(iter(updates), None)

    def __get_datasets(
        self,
        pipeline_id,
        update_id,
    ):
        params = urllib.parse.urlencode(
            {
                "order_by": "timestamp desc",
                "max_results": 100,
            },
            quote_via=urllib.parse.quote,
        )

        response = requests.get(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/events",
            params=params,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        payload = response.json()

        return [
            e["details"]["flow_definition"]["output_dataset"]
            for e in payload["events"]
            if e["event_type"] == "flow_definition"
            and e["origin"]["update_id"] == update_id
        ]

    def __refresh(
        self,
        pipeline_id,
        full_refresh=False,
        refresh_selection=[],
        full_refresh_selection=[],
    ):
        refresh_settings = {
            "full_refresh": full_refresh,
            "refresh_selection": refresh_selection,
            "full_refresh_selection": full_refresh_selection,
        }

        response = requests.post(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/updates",
            json=refresh_settings,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        response.raise_for_status()

    def __get_progress(
        self,
        pipeline_id,
        update_id,
    ):
        params = urllib.parse.urlencode(
            {
                "order_by": "timestamp desc",
                "max_results": 100,
            },
            quote_via=urllib.parse.quote,
        )

        response = requests.get(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/events",
            params=params,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        payload = response.json()

        if not "events" in payload:
            return None

        updates = [
            e["details"]["update_progress"]["state"]
            for e in payload["events"]
            if e["event_type"] == "update_progress"
            and e["origin"]["update_id"] == update_id
        ]

        if not updates:
            return None

        return next(iter(updates), None)

    def __stop(self, pipeline_id):
        response = requests.post(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/stop",
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        response.raise_for_status()
