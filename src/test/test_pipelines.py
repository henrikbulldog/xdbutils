""" Unit tests """

import unittest
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from xdbutils.pipelines import DLTEventPipeline, DLTFilePipeline

spark = DatabricksSession.builder.getOrCreate()
# Use WorkspaceClient to get dbutils
dbutils = WorkspaceClient().dbutils


class PipelinesTestCase(unittest.TestCase):
    """ Test DLT pipelines """

    def test_create_file_pipeline(self):
        """ Test create file pipeline """

        pipeline = DLTFilePipeline(
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

    def test_create_event_pipeline(self):
        """ Test create event pipeline """

        pipeline = DLTEventPipeline(
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


if __name__ == '__main__':
    unittest.main()
