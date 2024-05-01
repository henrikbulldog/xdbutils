""" Unit tests """

import unittest
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from xdbutils.pipelines.source import DLTPipeline


spark = DatabricksSession.builder.getOrCreate()
# Use WorkspaceClient to get dbutils
dbutils = WorkspaceClient().dbutils


class DLTPipelineTestCase(unittest.TestCase):
    """ Test DLT pipelines """

    def test_create_file_pipeline(self):
        """ Test create file pipeline """

        pipeline = DLTPipeline(
            spark=spark,
            dbutils=dbutils,
            source_system="testcdc",
            source_class="employee",
            raw_base_path="...",
            tags={
                "data_owner": "Henrik Thomsen",
                "cost_center": "123456",
                "documentation": "https://github.com/henrikbulldog/xdbutils"
            },
            )

        self.assertIsNotNone(pipeline)

        pipeline.raw_to_bronze(
            raw_format="json"
            )

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
            source_class="employee",
            raw_base_path="...",
            tags={
                "data_owner": "Henrik Thomsen",
                "cost_center": "123456",
                "documentation": "https://github.com/henrikbulldog/xdbutils"
            },
            )

        self.assertIsNotNone(pipeline)

        pipeline.event_to_bronze(
            eventhub_namespace="ns",
            eventhub_group_id="gr",
            eventhub_name="name",
            eventhub_connection_string="conn str"
            )

        pipeline.bronze_to_silver_append()

        pipeline.silver_to_gold(
            name="test_gold"
        )


if __name__ == '__main__':
    unittest.main()
