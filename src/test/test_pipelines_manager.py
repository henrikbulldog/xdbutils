""" Unit tests """

import unittest
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from xdbutils.pipelines.management import DLTPipelineManager

spark = DatabricksSession.builder.getOrCreate()
# Use WorkspaceClient to get dbutils
dbutils = WorkspaceClient().dbutils


class DLTPipelineManagerTestCase(unittest.TestCase):
    """ Test DLT pipelines manager """

    def test_create_file_pipeline(self):
        """ Test create file pipeline """

        pipeline = DLTPipelineManager(
            spark=spark,
            dbutils=dbutils,
            source_system="testcdc",
            source_class="employee",
            catalog="testing_dlt",
            tags={
                "data_owner": "Henrik Thomsen",
                "cost_center": "123456",
                "documentation": "https://github.com/henrikbulldog/xdbutils"
            },
            databricks_host="...",
            source_path="...",
            )

        self.assertIsNotNone(pipeline)


    def test_create_event_pipeline(self):
        """ Test create event pipeline """

        pipeline = DLTPipelineManager(
            spark=spark,
            dbutils=dbutils,
            source_system="testcdc",
            source_class="employee",
            catalog="testing_dlt",
            tags={
                "data_owner": "Henrik Thomsen",
                "cost_center": "123456",
                "documentation": "https://github.com/henrikbulldog/xdbutils"
            },
            continuous_workflow=True,
            databricks_host="...",
            source_path="...",
            )

        self.assertIsNotNone(pipeline)


if __name__ == '__main__':
    unittest.main()
