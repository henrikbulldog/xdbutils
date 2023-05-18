""" Unit tests """

import os
import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.dbutils import DBUtils
from xdbutils.datalakehouse import Stage

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class StageTestCase(unittest.TestCase):
    """ Test Stage """

    def test_start(self):
        """ Test Stage.start() """

        def read() -> DataFrame:
            return spark.createDataFrame(
                data=[
                    (1, "Finance"),
                    (2, "Marketing"),
                    (3, "Sales")
                ],
                schema=["id", "name"]
            )

        data_source = "teststage"
        dataset = "testset"
        base_path = f"dbfs:/FileStore/{os.environ.get('HOSTNAME')}"

        s = Stage(base_path=base_path, dataset=dataset, data_source=data_source) \
            .with_read(read=read)
        s.start()

        self.assertIsNotNone(s.latest_partition(s.path))

        df = spark.read.parquet(f"{base_path}/{data_source}/{dataset}")
        df.show()
        self.assertEqual(df.count(), 3)
        dbutils.fs.rm(s.path, recurse=True)


if __name__ == '__main__':
    unittest.main()
