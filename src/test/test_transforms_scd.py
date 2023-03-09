""" Unit tests """

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, lit, col

from xdbutils.transforms import scd

spark = SparkSession.builder.getOrCreate()


class TransformsScdTestCase(unittest.TestCase):
    """ Test Batch """

    def test_diff_first(self):
        """ Test transforms.scd.diff without existing history data """

        current_data_df = spark.sparkContext.parallelize([
                (1, "Allan Adams"),
                (2, "Beatrice Bolton"),
            ]).toDF(["id", "name"])

        result_df = scd.diff(current_data_df, ["id"])

        result_df.show(truncate=False)

        expected_df = spark.sparkContext.parallelize([
                (1, "Allan Adams", "i"),
                (2, "Beatrice Bolton","i"),
            ]).toDF(["id", "name", "meta_action"])

        result_harmonized_df = result_df \
            .select("id", "name", "meta_action") \
            .orderBy("id")

        self.assertEqual(expected_df.collect(), result_harmonized_df.collect())


    def test_diff_update(self):
        """ Test transforms.scd.diff """

        current_data_df = spark.sparkContext.parallelize([
                (1, "Allan Adams"),
                (2, "Beatrice Bolton"),
            ]).toDF(["id", "name"])

        first_diff_df = scd.diff(current_data_df, ["id"])
        latest_version_df = spark.sparkContext.parallelize(first_diff_df.collect()).toDF()

        latest_version_df.show(truncate=False)

        current_data_updated_df = spark.sparkContext.parallelize([
                (2, "Beatrice Adams"),
                (3, "Camillla Adams"),
            ]).toDF(["id", "name"]).cache()

        result_df = scd.diff(
            current_data_df=current_data_updated_df,
            key_columns= ["id"],
            latest_version_df= latest_version_df,
            handle_deletions= True)

        result_df.show(truncate=False)

        expected_df = spark.sparkContext.parallelize([
                (1, "Allan Adams", "d"),
                (2, "Beatrice Adams","u"),
                (3, "Camillla Adams", "i"),
            ]).toDF(["id", "name", "meta_action"])

        result_latest_df = result_df \
            .select("id", "name", "meta_action") \
            .where(col("meta_valid_to") == to_timestamp(lit("9999-12-31"))) \
            .orderBy("id")

        self.assertEqual(expected_df.collect(), result_latest_df.collect())


if __name__ == '__main__':
    unittest.main()
