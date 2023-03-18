""" Unit tests """

import unittest
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from xdbutils.fs import PartitionedFolder

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class PartitionedFolderTestCase(unittest.TestCase):
    """ Test PartitionedFolder """

    def test_constructor(self):
        """ Test transforms.scd.diff without existing history data """

        base_path = "base_path"
        self.assertEqual(PartitionedFolder().path, "")
        self.assertEqual(
            PartitionedFolder(base_path=base_path).path,
            base_path)
        self.assertEqual(
            PartitionedFolder(base_path=base_path, stage="bronze").path,
            f"{base_path}/bronze")
        self.assertEqual(
            PartitionedFolder(base_path=base_path, stage="bronze", data_source="datasource1").path,
            f"{base_path}/bronze/datasource1")
        self.assertEqual(
            PartitionedFolder(base_path=base_path, stage="bronze", 
                data_source="datasource1", dataset="dataset1").path,
            f"{base_path}/bronze/datasource1/dataset1")
        self.assertEqual(
            PartitionedFolder(base_path=base_path, stage="bronze",
                data_source="datasource1", version="version1", dataset="dataset1").path,
            f"{base_path}/bronze/datasource1/version1/dataset1")


if __name__ == '__main__':
    unittest.main()
