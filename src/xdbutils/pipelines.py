""" Delta Live Tables Pipelines """

from typing import Callable
import urllib
import requests
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
import dlt  # pylint: disable=import-error


class DLTPipeline():
    """ Delta Live Tables Pipeline """

    def __init__(
        self,
        spark,
        dbutils,
        source_system,
        entity,
        catalog,
        data_owner,
        databricks_token,
        databricks_host = None,
        source_path = None,
        continuous_workflow = False
        ):
        
        self.spark = spark
        self.dbutils = dbutils
        self.source_system = source_system
        self.entity = entity
        self.data_owner = data_owner
        self.continuous_workflow = continuous_workflow

        self.__create_or_update_workflow(
            catalog=catalog,
            databricks_token=databricks_token,
            databricks_host=databricks_host,
            source_path=source_path
            )

    def bronze_to_silver(
        self,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        expectations = None
        ):
        """ Bronze to Silver, append-only """

        if not expectations:
            expectations = {}

        @dlt.table(
            comment=f"Source system: {self.source_system}, entity: {self.entity}, data owner: {self.data_owner}",
            name=f"silver_{self.entity}",
            table_properties={
                "quality": "silver",
                "pipelines.reset.allowed": "false"
            },
        )
        @dlt.expect_all(expectations)
        def dlt_table():
            return (
                dlt.read(f"bronze_{self.entity}")
                .transform(parse)
            )

    def bronze_to_silver_upsert(
        self,
        keys,
        sequence_by,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        expectations = None
        ):
        """ Bronze to Silver, upsert """

        if not expectations:
            expectations = {}

        @dlt.view(name=f"view_silver_{self.entity}")
        @dlt.expect_all(expectations)
        def dlt_view():
            return (
                dlt.read_stream(f"bronze_{self.entity}")
                .transform(parse)
            )

        dlt.create_streaming_table(
            name=f"silver_{self.entity}",
            comment=f"Source system: {self.source_system}, entity: {self.entity}, data owner: {self.data_owner}",
            table_properties={
                "quality": "bronze",
                "pipelines.reset.allowed": "false"
            },
            )

        dlt.apply_changes(
            target=f"silver_{self.entity}",
            source=f"view_silver_{self.entity}",
            keys=keys,
            sequence_by=col(sequence_by)
            )

    def silver_to_gold(
        self,
        name,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        expectations = None
        ):
        """ Bronze to Silver """

        if not expectations:
            expectations = {}

        @dlt.table(
            comment=f"Source system: {self.source_system}, entity: {self.entity}, data owner: {self.data_owner}",
            name=f"gold_{self.entity}_{name}",
            table_properties={
                "quality": "gold",
                "pipelines.reset.allowed": "false"
            },
        )
        @dlt.expect_all(expectations)
        def dlt_table():
            return (
                dlt.read(f"silver_{self.entity}")
                .transform(parse)
            )

    def __create_or_update_workflow(
        self,
        catalog,
        databricks_token,
        databricks_host = None,
        source_path = None
        ):
        """ Create Delta Live Tables Workflow """

        try:
            if not databricks_host:
                databricks_host = (
                    self.dbutils
                    .notebook.entry_point.getDbutils()
                    .notebook().getContext()
                    .tags().get("browserHostName").get()
                )

            if not source_path:
                source_path = (
                    self.dbutils.notebook.entry_point.getDbutils()
                    .notebook().getContext()
                    .notebookPath().get()
                )
        except:
            # Cannot get information from notebook context, give up
            return

        workflow_settings = self.__get_workflow_settings(
            catalog=catalog,
            source_path=source_path,
            )

        pipeline_id = self.__get_workflow_id(
            databricks_host=databricks_host,
            databricks_token=databricks_token
            )

        if pipeline_id:
            self.__update_workflow(
                pipeline_id,
                workflow_settings,
                databricks_host,
                databricks_token
                )
        else:
            self.__create_workflow(
                workflow_settings,
                databricks_host,
                databricks_token
                )
            
    def __get_workflow_settings(
        self,
        catalog,
        source_path,
        ):
        return {
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
                    "path": source_path
                }
                }
            ],
            "catalog": catalog,
            "target": self.source_system,
            "configuration": {
                "data_owner": self.data_owner
            },
            "continuous": self.continuous_workflow
        }

    def __get_workflow_id(
        self,
        databricks_host,
        databricks_token
        ):
        name = f"{self.source_system}-{self.entity}"
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

    def __update_workflow(
        self,
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

    def __create_workflow(
        self,
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


class DLTFilePipeline(DLTPipeline):
    """ Delta Live Tables File Pipeline """

    def raw_to_bronze(
        self,
        raw_base_path,
        raw_format,
        options = None
        ):
        """ Raw to bronze """

        if not options:
            options = {}

        @dlt.table(
            comment=f"Source system: {self.source_system}, entity: {self.entity}, data owner: {self.data_owner}",
            name=f"bronze_{self.entity}",
            table_properties={
                "quality": "bronze",
                "pipelines.reset.allowed": "false" # preserves the data in the delta table if you do full refresh
            },
        )
        def dlt_table():
            return ( self.spark.readStream.format("cloudFiles")
                .options(**options)
                .option("cloudFiles.format", raw_format)
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option(
                    "cloudFiles.schemaLocation",
                    f"{raw_base_path}/checkpoints/{self.source_system}/{self.entity}"
                    )
                .load(f"{raw_base_path}/{self.source_system}/{self.entity}")
                .withColumn("sys_ingest_time", current_timestamp())
            )


class DLTEventPipeline(DLTPipeline):
    """ Delta Live Tables Event Pipeline """

    def __init__(
        self,
        spark,
        dbutils,
        source_system,
        entity,
        catalog,
        data_owner,
        databricks_token,
        databricks_host = None,
        source_path = None
        ):

        super().__init__(
            spark,
            dbutils,
            source_system,
            entity,
            catalog,
            data_owner,
            databricks_token,
            databricks_host,
            source_path,
            continuous_workflow = True
            )

    def event_to_bronze(
        self,
        eventhub_namespace,
        eventhub_group_id,
        eventhub_name,
        eventhub_connection_string,
        parse: Callable[[DataFrame], DataFrame] = lambda df: df,
        partition_cols = None,
        expectations = None,
    ):
        """ Event to bronze """

        if not partition_cols:
            partition_cols = []
        if not expectations:
            expectations = {}

        kafka_options = {
            "kafka.bootstrap.servers"  : f"{eventhub_namespace}.servicebus.windows.net:9093",
            "subscribe"                : eventhub_name,
            "kafka.group.id"           : eventhub_group_id,
            "kafka.sasl.mechanism"     : "PLAIN",
            "kafka.security.protocol"  : "SASL_SSL",
            "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{eventhub_connection_string}\";",
            "kafka.request.timeout.ms" : "6000",
            "kafka.session.timeout.ms" : "6000",
            "maxOffsetsPerTrigger"     : "600",
            "failOnDataLoss"           : 'true',
            "startingOffsets"          : "latest"
        }

        @dlt.create_table(
            comment=f"Source system: {self.source_system}, entity: {self.entity}, data owner: {self.data_owner}",
            name=f"bronze_{self.entity}",
            table_properties={
                "quality": "bronze",
                "pipelines.reset.allowed": "false"
            },
            partition_cols=partition_cols        )
        @dlt.expect_all(expectations)
        def dlt_table():
            return (
                self.spark.readStream
                .format("kafka")
                .options(**kafka_options)
                .load()
                .transform(parse)
            )
