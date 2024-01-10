""" Unit tests """

import unittest
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from xdbutils.pipelines import DLTPipeline

spark = DatabricksSession.builder.getOrCreate()
# Use WorkspaceClient to get dbutils
dbutils = WorkspaceClient().dbutils


class PipelinesTestCase(unittest.TestCase):
    """ Test DLT pipelines """

    def test_create_file_pipeline(self):
        """ Test create file pipeline """

        pipeline = DLTPipeline(
            spark=spark,
            dbutils=dbutils,
            source_system="testcdc",
            entity="employee",
            catalog="testing_dlt",
            tags={
                "data_owner": "Henrik Thomsen",
                "cost_center": "123456",
                "documentation": "https://github.com/henrikbulldog/xdbutils"
            },
            )

        self.assertIsNotNone(pipeline)

        pipeline.raw_to_bronze(
            raw_base_path="...",
            raw_format="json"
            )

        pipeline.bronze_to_silver()

        pipeline.bronze_to_silver_append()

        pipeline.bronze_to_silver_upsert(
            keys=["id"],
            sequence_by="timestamp"
        )

        pipeline.bronze_to_silver_track_changes(
            keys=["id"],
            sequence_by="timestamp"
        )

        pipeline.silver_to_gold(
            name="test_gold"
        )


    def test_create_event_pipeline(self):
        """ Test create event pipeline """

        pipeline = DLTPipeline(
            spark=spark,
            dbutils=dbutils,
            source_system="testcdc",
            entity="employee",
            catalog="testing_dlt",
            tags={
                "data_owner": "Henrik Thomsen",
                "cost_center": "123456",
                "documentation": "https://github.com/henrikbulldog/xdbutils"
            },
            continuous_workflow=True,
            )

        self.assertIsNotNone(pipeline)

        pipeline.event_to_bronze(
            eventhub_namespace="ns",
            eventhub_group_id="gr",
            eventhub_name="name",
            client_id="client id",
            client_secret="client secret",
            azure_tenant_id="azure_tenant_id",
            )

        pipeline.bronze_to_silver()

        pipeline.bronze_to_silver_append()

        pipeline.silver_to_gold(
            name="test_gold"
        )


if __name__ == '__main__':
    unittest.main()
