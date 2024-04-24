try:
    import dlt  # type: ignore
except:  # pylint: disable=bare-except
    from unittest.mock import MagicMock

    class MockDlt:
        """Mock dlt module, since dlt module could not be loaded"""

        def __getattr__(self, name):
            return MagicMock()

        def __call__(self, *args, **kwargs):
            return MagicMock()

    dlt = MockDlt()

from functools import reduce
from typing import Callable
import warnings
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, expr, lit
from xdbutils.pipelines.management import DLTPipelineManager

class DLTPipeline(DLTPipelineManager):
    """Delta Live Tables Pipeline"""

    def raw_to_bronze(
        self,
        data_type = None,
        environment = None,
        raw_format = None,
        source_class = None,
        target_class = None,
        options = None,
        schema = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
        ):
        """ Raw to bronze """

        if data_type:
            warnings.warn("Parameter data_type is deprecated",
                category=DeprecationWarning,
                stacklevel=2)
        if environment:
            warnings.warn("Parameter environment is deprecated",
                category=DeprecationWarning,
                stacklevel=2)
        if not raw_format:
            raise Exception("Raw format must be specified")
        if not source_class:
            source_class = self.source_class
        if not target_class:
            target_class = self.source_class
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
            name=f"bronze_{target_class}",
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
                    f"{self.raw_base_path}/checkpoints/{self.source_system}/{source_class}")
                reader.option("cloudFiles.inferColumnTypes", "true")
            if schema:
                reader.schema(schema)
            else:
                reader.option("cloudFiles.schemaEvolutionMode", "addNewColumns")

            result_df = (
                reader
                .load(f"{self.raw_base_path}/{self.source_system}/{source_class}")
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
        client_id = None,
        client_secret = None,
        azure_tenant_id = None,
        eventhub_connection_string = None,
        max_offsets_per_trigger = None,
        starting_offsets = None,
        target_class = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
    ):
        """ Event to bronze """

        if not max_offsets_per_trigger:
            max_offsets_per_trigger = "600"
        if not starting_offsets:
            starting_offsets = "latest"
        if not target_class:
            target_class = self.source_class
        if not partition_cols:
            partition_cols = []
        if not expectations:
            expectations = {}
            quarantine_rules = None
        else:
            quarantine_rules = f'NOT(({") AND (".join(expectations.values())}))'

        kafka_options = {
            "kafka.bootstrap.servers"  : f"{eventhub_namespace}.servicebus.windows.net:9093",
            "kafka.group.id"           : eventhub_group_id,
            "kafka.security.protocol"  : "SASL_SSL",
            "kafka.request.timeout.ms" : "6000",
            "kafka.session.timeout.ms" : "6000",
            "maxOffsetsPerTrigger"     : "600",
            "failOnDataLoss"           : 'false',
            "startingOffsets"          : starting_offsets,
        }

        if eventhub_connection_string:
            kafka_options["kafka.sasl.jaas.config"] = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{eventhub_connection_string}\";"
            kafka_options["kafka.sasl.mechanism"] = "PLAIN"
            kafka_options["subscribe"] = eventhub_name
        else:
            kafka_options["kafka.sasl.jaas.config"] = f'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="{client_id}" clientSecret="{client_secret}" scope="https://{eventhub_namespace}.servicebus.windows.net/.default" ssl.protocol="SSL";'
            kafka_options["kafka.sasl.mechanism"] = "OAUTHBEARER"
            kafka_options["subscribePattern"] = eventhub_name
            kafka_options["kafka.sasl.login.callback.handler.class"] = "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler"
            kafka_options["kafka.sasl.oauthbearer.token.endpoint.url"] = f"https://login.microsoft.com/{azure_tenant_id}/oauth2/v2.0/token"

        @dlt.create_table(
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            name=f"bronze_{target_class}",
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
            )

            if quarantine_rules:
                result_df = result_df.withColumn("_quarantined", expr(quarantine_rules))

            return result_df

    def bronze_to_silver_append(
        self,
        source_classes = None,
        target_class = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
        ):
        """ Bronze to Silver, append only """

        if not partition_cols:
            partition_cols = []
        if not source_classes:
            source_classes = [self.source_class]
        if not target_class:
            target_class = self.source_class

        if not partition_cols:
            partition_cols = []

        if not expectations:
            expectations = {}
            quarantine_rules = None
        else:
            quarantine_rules = f'NOT(({") AND (".join(expectations.values())}))'

        @dlt.table(
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            name=f"silver_{target_class}",
            table_properties={
                "quality": "silver"
            },
            partition_cols=partition_cols,
        )
        def dlt_table():
            silver_df = self.__union_streams([f"bronze_{t}" for t in source_classes])

            if "_quarantined".upper() in (name.upper() for name in silver_df.columns):
                silver_df = (
                    silver_df
                    .where(col("_quarantined") == lit(False))
                    .drop("_quarantined")
                )

            silver_df = silver_df.transform(parse)

            if quarantine_rules:
                silver_df = silver_df.withColumn("_quarantined", expr(quarantine_rules))

            if "_rescued_data" in silver_df.schema.fieldNames():
                silver_df = silver_df.drop("_rescued_data")

            return silver_df

    def bronze_to_silver_upsert(
        self,
        keys,
        sequence_by,
        source_classes = None,
        target_class = None,
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
        if not source_classes:
            source_classes = [self.source_class]
        if not target_class:
            target_class = self.source_class

        if not partition_cols:
            partition_cols = []

        if not expectations:
            expectations = {}
            quarantine_rules = None
        else:
            quarantine_rules = f'NOT(({") AND (".join(expectations.values())}))'

        @dlt.view(name=f"view_silver_{target_class}")
        def dlt_view():
            silver_df = self.__union_streams([f"bronze_{t}" for t in source_classes])

            if "_quarantined".upper() in (name.upper() for name in silver_df.columns):
                silver_df = (
                    silver_df
                    .where(col("_quarantined") == lit(False))
                    .drop("_quarantined")
                )

            silver_df = silver_df.transform(parse)

            if quarantine_rules:
                silver_df = silver_df.withColumn("_quarantined", expr(quarantine_rules))

            if "_rescued_data" in silver_df.schema.fieldNames():
                silver_df = silver_df.drop("_rescued_data")

            return silver_df
        
        dlt.create_streaming_table(
            name=f"silver_{target_class}",
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            table_properties={
                "quality": "bronze"
            },
            partition_cols=partition_cols,
            )

        dlt.apply_changes(
            target=f"silver_{target_class}",
            source=f"view_silver_{target_class}",
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
        source_classes = None,
        target_class = None,
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

        if not source_classes:
            source_classes = [self.source_class]
        if not target_class:
            target_class = self.source_class
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

        @dlt.view(name=f"view_silver_{target_class}_changes")
        @dlt.expect_all(expectations)
        def dlt_view():
            silver_df = self.__union_streams([f"bronze_{t}" for t in source_classes])

            if "_quarantined".upper() in (name.upper() for name in silver_df.columns):
                silver_df = (
                    silver_df
                    .where(col("_quarantined") == lit(False))
                    .drop("_quarantined")
                )

            silver_df = silver_df.transform(parse)

            if quarantine_rules:
                silver_df = silver_df.withColumn("_quarantined", expr(quarantine_rules))

            if "_rescued_data" in silver_df.schema.fieldNames():
                silver_df = silver_df.drop("_rescued_data")

            return silver_df

        dlt.create_streaming_table(
            name=f"silver_{target_class}_changes",
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            table_properties={
                "quality": "bronze"
            },
            partition_cols=partition_cols,
            )

        dlt.apply_changes(
            target=f"silver_{target_class}_changes",
            source=f"view_silver_{target_class}_changes",
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
        source_classes = None,
        target_class = None,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        expectations = None
        ):
        """ Bronze to Silver """

        if not source_classes:
            source_classes = [self.source_class]
        if not target_class:
            target_class = self.source_class
        if not expectations:
            expectations = {}

        @dlt.table(
            comment=", ".join([f"{e}: {self.tags[e]}" for e in self.tags.keys()]),
            name=f"gold_{target_class}_{name}",
            table_properties={
                "quality": "gold"
            },
        )
        @dlt.expect_all_or_fail(expectations)
        def dlt_table():
            gold_df = self.__union_tables([f"silver_{t}" for t in source_classes])

            if "_quarantined".upper() in (name.upper() for name in gold_df.columns):
                gold_df = (
                    gold_df
                    .where(col("_quarantined") == lit(False))
                    .drop("_quarantined")
                )

            return (
                gold_df.transform(parse)
            )

    def __union_streams(self, sources):
        source_tables = [dlt.read_stream(t) for t in sources]
        unioned = reduce(lambda x, y: x.unionAll(y), source_tables)
        return unioned

    def __union_tables(self, sources):
        source_tables = [dlt.read(t) for t in sources]
        unioned = reduce(lambda x, y: x.unionAll(y), source_tables)
        return unioned