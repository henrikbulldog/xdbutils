try:
    import dlt  # type: ignore
except: # pylint: disable=bare-except
    from unittest.mock import MagicMock
    class MockDlt:
        """ Mock dlt module, since dlt module could not be loaded """
        def __getattr__(self, name):
            return MagicMock()

        def __call__(self, *args, **kwargs):
            return MagicMock()
    dlt = MockDlt()

from functools import reduce
from typing import Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, expr, lit
from xdbutils.pipelines.management import DLTPipelineManager

class DLTPipeline(DLTPipelineManager):
    """ Delta Live Tables Pipeline """

    def raw_to_bronze(
        self,
        raw_base_path,
        raw_format,
        source_entity = None,
        target_entity = None,
        options = None,
        schema = None,
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
            reader = (
                self.spark.readStream
                .format("cloudFiles")
                .options(**options)
            )
            reader.option("cloudFiles.format", raw_format)
            if "json" in raw_format.lower():
                reader.option("cloudFiles.schemaLocation",
                    f"{raw_base_path}/checkpoints/{self.source_system}/{source_entity}")
                reader.option("cloudFiles.inferColumnTypes", "true")
            if schema:
                reader.schema(schema)
            else:
                reader.option("cloudFiles.schemaEvolutionMode", "addNewColumns")

            result_df = (
                reader
                .load(f"{raw_base_path}/{self.source_system}/{source_entity}")
                .transform(parse)
                .withColumn("_ingest_time", current_timestamp())
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
            quarantine_rules = None
        else:
            quarantine_rules = f'NOT(({") AND (".join(expectations.values())}))'

        @dlt.table(
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            name=f"silver_{target_entity}",
            table_properties={
                "quality": "silver"
            },
            partition_cols=partition_cols,
        )
        def dlt_table():

            silver_df = (
                self.__union_streams([f"bronze_{t}" for t in source_entities])
                .where(col("_quarantined") == lit(False))
                .drop("_quarantined")
                .transform(parse)
                .withColumn("_quarantined", lit(False))
            )

            if quarantine_rules:
                silver_df = silver_df.withColumn("_quarantined", expr(quarantine_rules))

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
            quarantine_rules = None
        else:
            quarantine_rules = f'NOT(({") AND (".join(expectations.values())}))'

        @dlt.view(name=f"view_silver_{target_entity}")
        def dlt_view():
            silver_df = (
                self.__union_streams([f"bronze_{t}" for t in source_entities])
                .where(col("_quarantined") == lit(False))
                .drop("_quarantined")
                .transform(parse)
                .withColumn("_quarantined", lit(False))
            )

            if quarantine_rules:
                silver_df = silver_df.withColumn("_quarantined", expr(quarantine_rules))

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
            quarantine_rules = None
        else:
            quarantine_rules = f'NOT(({") AND (".join(expectations.values())}))'

        if not track_history_except_column_list:
            track_history_except_column_list = [sequence_by]

        @dlt.view(name=f"view_silver_{target_entity}_changes")
        def dlt_view():
            silver_df = (
                self.__union_streams([f"bronze_{t}" for t in source_entities])
                .where(col("_quarantined") == lit(False))
                .drop("_quarantined")
                .transform(parse)
                .withColumn("_quarantined", lit(False))
            )

            if quarantine_rules:
                silver_df = silver_df.withColumn("_quarantined", expr(quarantine_rules))

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
        @dlt.expect_all_or_fail(expectations)
        def dlt_table():
            return (
                self.__union_tables([f"silver_{t}" for t in source_entities])
                .transform(parse)
            )

    def __union_streams(self, sources):
        source_tables = [dlt.read_stream(t) for t in sources]
        unioned = reduce(lambda x,y: x.unionAll(y), source_tables)
        return unioned

    def __union_tables(self, sources):
        source_tables = [dlt.read(t) for t in sources]
        unioned = reduce(lambda x,y: x.unionAll(y), source_tables)
        return unioned
