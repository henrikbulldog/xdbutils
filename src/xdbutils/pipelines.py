""" Delta Live Tables Pipelines """

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

def _create_or_update_workflow(
    dbutils,
    source_system,
    entity,
    catalog,
    continuous_workflow,
    databricks_token,
    databricks_host = None,
    source_path = None,
    tags = None
    ):
    """ Create Delta Live Tables Workflow """

    if not tags:
        tags = {}

    try:
        is_job = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook().getContext().currentRunId().isDefined()
            )

        if is_job:
            return
    except Exception as exc: # pylint: disable=broad-exception-caught
        print("Could not determine if this is a job run from notebook context.", exc)
        return

    try:
        if not databricks_host:
            databricks_host = (
                dbutils
                .notebook.entry_point.getDbutils()
                .notebook().getContext()
                .tags().get("browserHostName").get()
            )
    except Exception as exc: # pylint: disable=broad-exception-caught
        print("Could not get databricks host from notebook context, please specify databricks_host.", exc)
        return

    try:
        if not source_path:
            source_path = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook().getContext()
                .notebookPath().get()
            )
    except Exception as exc: # pylint: disable=broad-exception-caught
        print("Could not get source path from notebook context, please specify source_path.", exc)
        return

    try:
        workflow_settings = _get_workflow_settings(
            source_system=source_system,
            entity=entity,
            catalog=catalog,
            source_path=source_path,
            continuous_workflow=continuous_workflow,
            tags=tags
            )

        pipeline_id = _get_workflow_id(
            source_system=source_system,
            entity=entity,
            databricks_host=databricks_host,
            databricks_token=databricks_token
            )

        if pipeline_id:
            _update_workflow(
                pipeline_id=pipeline_id,
                workflow_settings=workflow_settings,
                databricks_host=databricks_host,
                databricks_token=databricks_token
                )
        else:
            _create_workflow(
                workflow_settings=workflow_settings,
                databricks_host=databricks_host,
                databricks_token=databricks_token
                )
    except Exception as exc: # pylint: disable=broad-exception-caught
        # Cannot get information from notebook context, give up
        print("Could not create DLT workflow.", exc)
        return

def _get_workflow_settings(
    source_system,
    entity,
    catalog,
    source_path,
    continuous_workflow,
    tags = None
    ):

    if not tags:
        tags = {}

    settings = {
        "name": f"{source_system}-{entity}",
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
                "path": source_path
            }
            }
        ],
        "catalog": catalog,
        "target": source_system,
        "configuration": {
            "pipelines.enableTrackHistory": "true"
        },
        "continuous": continuous_workflow
    }

    settings["configuration"].update(tags)

    return settings

def _get_workflow_id(
    source_system,
    entity,
    databricks_host,
    databricks_token
    ):
    name = f"{source_system}-{entity}"
    params = urllib.parse.urlencode(
        {"filter": f"name LIKE '{name}'"},
        quote_via=urllib.parse.quote)

    response = requests.get(
        url=f"https://{databricks_host}/api/2.0/pipelines",
        params=params,
        headers={"Authorization": f"Bearer {databricks_token}"},
        timeout=60
        )

    if response.status_code == 200:
        payload = response.json()
        if "statuses" in payload.keys():
            if len(payload["statuses"]) == 1:
                return payload["statuses"][0]["pipeline_id"]

    return None

def _update_workflow(
    pipeline_id,
    workflow_settings,
    databricks_host,
    databricks_token
    ):

    response = requests.put(
        url=f"https://{databricks_host}/api/2.0/pipelines/{pipeline_id}",
        json=workflow_settings,
        headers={"Authorization": f"Bearer {databricks_token}"},
        timeout=60
        )

    response.raise_for_status()

def _create_workflow(
    workflow_settings,
    databricks_host,
    databricks_token
    ):

    response = requests.post(
        url=f"https://{databricks_host}/api/2.0/pipelines",
        json=workflow_settings,
        headers={"Authorization": f"Bearer {databricks_token}"},
        timeout=60
        )

    response.raise_for_status()

def _union_streams(sources):
    source_tables = [dlt.read_stream(t) for t in sources]
    unioned = reduce(lambda x,y: x.unionAll(y), source_tables)
    return unioned

def _union_tables(sources):
    source_tables = [dlt.read(t) for t in sources]
    unioned = reduce(lambda x,y: x.unionAll(y), source_tables)
    return unioned

def _bronze_to_silver_append(
    source_entities,
    target_entity,
    parse: Callable[[DataFrame], DataFrame] = lambda df: df,
    partition_cols = None,
    expectations = None,
    tags = None,
    ):
    """ Bronze to Silver, append-only """

    if not partition_cols:
        partition_cols = []

    if not tags:
        tags = {}

    if not expectations:
        expectations = {}

    @dlt.table(
        comment=", ".join([f"{e}: {tags[e]}" for e in tags.keys()]),
        name=f"silver_{target_entity}",
        table_properties={
            "quality": "silver",
            "pipelines.reset.allowed": "false"
        },
        partition_cols=partition_cols,
    )
    @dlt.expect_all(expectations)
    def dlt_table():

        silver_df = (
            _union_streams([f"bronze_{t}" for t in source_entities])
            .transform(parse)
            .where(col("_quarantined") == lit(False))
            .drop("_quarantined")
        )

        if "_rescued_data" in silver_df.schema.fieldNames():
            silver_df = silver_df.drop("_rescued_data")

        return silver_df

def _bronze_to_silver_upsert(
    source_entities,
    target_entity,
    keys,
    sequence_by,
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    parse: Callable[[DataFrame], DataFrame] = lambda df: df,
    partition_cols = None,
    expectations = None,
    tags = None,
    ):
    """ Bronze to Silver, upsert """

    if not partition_cols:
        partition_cols = []

    if not tags:
        tags = {}

    if not expectations:
        expectations = {}

    @dlt.view(name=f"view_silver_{target_entity}")
    @dlt.expect_all(expectations)
    def dlt_view():
        silver_df = (
            _union_streams([f"bronze_{t}" for t in source_entities])
            .transform(parse)
            .where(col("_quarantined") == lit(False))
            .drop("_quarantined")
        )

        if "_rescued_data" in silver_df.schema.fieldNames():
            silver_df = silver_df.drop("_rescued_data")

        return silver_df

    dlt.create_streaming_table(
        name=f"silver_{target_entity}",
        comment=", ".join([f"{e}: {tags[e]}" for e in tags.keys()]),
        table_properties={
            "quality": "bronze",
            "pipelines.reset.allowed": "false"
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

def _bronze_to_silver_track_changes(
    source_entities,
    target_entity,
    keys,
    sequence_by,
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
    tags = None,
    ):
    """ Bronze to Silver, change data capture,
    see https://docs.databricks.com/en/delta-live-tables/cdc.html """

    if not partition_cols:
        partition_cols = []

    if not tags:
        tags = {}

    if not expectations:
        expectations = {}

    if not track_history_except_column_list:
        track_history_except_column_list = [sequence_by]

    @dlt.view(name=f"view_silver_{target_entity}_changes")
    @dlt.expect_all(expectations)
    def dlt_view():
        silver_df = (
            _union_streams([f"bronze_{t}" for t in source_entities])
            .transform(parse)
            .where(col("_quarantined") == lit(False))
            .drop("_quarantined")
        )

        if "_rescued_data" in silver_df.schema.fieldNames():
            silver_df = silver_df.drop("_rescued_data")

        return silver_df

    dlt.create_streaming_table(
        name=f"silver_{target_entity}_changes",
        comment=", ".join([f"{e}: {tags[e]}" for e in tags.keys()]),
        table_properties={
            "quality": "bronze",
            "pipelines.reset.allowed": "false"
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
        ):

        self.spark = spark
        self.dbutils = dbutils
        self.source_system = source_system
        self.entity = entity
        self.continuous_workflow = continuous_workflow
        self.tags = tags

        if not self.tags:
            self.tags = {}
        self.tags["Source system"] = self.source_system
        self.tags["Entity"] = self.entity

        _create_or_update_workflow(
            dbutils=self.dbutils,
            source_system=self.source_system,
            entity=self.entity,
            catalog=catalog,
            continuous_workflow=self.continuous_workflow,
            databricks_token=databricks_token,
            databricks_host=databricks_host,
            source_path=source_path,
            tags=self.tags
            )

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
                "pipelines.reset.allowed": "false"
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
                "pipelines.reset.allowed": "false"
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

        _bronze_to_silver_append(
            source_entities=source_entities,
            target_entity=target_entity,
            parse=parse,
            partition_cols=partition_cols,
            expectations=expectations,
            tags=self.tags,
        )

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

        _bronze_to_silver_upsert(
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
            tags=self.tags,
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

        _bronze_to_silver_track_changes(
            source_entities=source_entities,
            target_entity=target_entity,
            keys=keys,
            sequence_by=sequence_by,
            tags=self.tags,
            ignore_null_updates=ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=column_list,
            except_column_list=except_column_list,
            track_history_column_list=track_history_column_list,
            track_history_except_column_list=track_history_except_column_list,
            parse=parse,
            partition_cols=partition_cols,
            expectations=expectations,
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

            _bronze_to_silver_upsert(
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
                tags=self.tags,
            )

        else:
            _bronze_to_silver_append(
                source_entities=source_entities,
                target_entity=target_entity,
                parse=parse,
                partition_cols=partition_cols,
                expectations=expectations,
                tags=self.tags,
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
                "quality": "gold",
                "pipelines.reset.allowed": "false"
            },
        )
        @dlt.expect_all(expectations)
        def dlt_table():
            return (
                _union_tables([f"silver_{t}" for t in source_entities])
                .transform(parse)
            )
