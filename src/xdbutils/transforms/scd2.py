""" Slowly changing dimension type 2 transfromations """

from pyspark.sql import DataFrame
from pyspark.sql.functions import md5, isnull \
    ,col, when, concat_ws, current_timestamp, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, TimestampType, StringType


def diff(spark, 
         current_data_df: DataFrame,
         key_columns: list[str],
         latest_version_df: DataFrame = None,
         handle_deletions: bool = False,
         value_columns: list[str] = None,
         ) -> DataFrame:
    """ Calculate the changes in a slow changing dimension type 2 """

    if latest_version_df is None:
        latest_version_df = __get_empty(spark, current_data_df)

    if value_columns is None:
        all_columns = [f.name for f in  current_data_df.schema.fields]
        value_columns = set(all_columns).difference(set(key_columns))

    hashed_current_data_df = current_data_df \
        .withColumn("meta_hash_key", md5(concat_ws("", *key_columns))) \
        .withColumn("meta_hash_values", md5(concat_ws("", *value_columns)))

    changeset_df = hashed_current_data_df.alias("c") \
        .join(latest_version_df.alias("h"), ["meta_hash_key"], "outer") \
        .select(col("c.meta_hash_key"),
                col("c.meta_hash_values"),
                col("h.meta_hash_key"), col("h.meta_hash_values")) \
        .withColumn("meta_action",
            when(isnull(col("h.meta_hash_key")), lit("i")) \
            .when(isnull(col("c.meta_hash_key")), lit("d")) \
            .when(col("c.meta_hash_values") != col("h.meta_hash_values"), lit("u")) \
            .otherwise(lit(None))) \
        .where(col("meta_action").isNotNull()) \
        .select(
            when(isnull(col("h.meta_hash_key")),
                 col("c.meta_hash_key")).otherwise(col("h.meta_hash_key")).alias("meta_hash_key"),
            col("meta_action")
            )

    obsoletions_df = latest_version_df \
        .join(changeset_df.where(col("meta_action") != lit("i")) \
              .select("meta_hash_key"), ["meta_hash_key"]) \
        .withColumn("meta_valid_to", current_timestamp())

    inserts_and_updates_df = hashed_current_data_df \
        .join(changeset_df.where(col("meta_action") != lit("d")) \
              .select("meta_hash_key", "meta_action"), ["meta_hash_key"]) \
        .withColumn("meta_valid_from", current_timestamp()) \
        .withColumn("meta_valid_to", to_timestamp(lit("9999-12-31")))

    if handle_deletions:
        deletions_df = latest_version_df.drop("meta_action").alias("l") \
            .join(changeset_df.where(col("meta_action") == lit("d")) \
                  .select("meta_hash_key", "meta_action"), ["meta_hash_key"]) \
            .withColumn("meta_valid_from", current_timestamp()) \
            .withColumn("meta_valid_to", to_timestamp(lit("9999-12-31")))
    else:
        deletions_df = __get_empty(spark, current_data_df)

    return __harmonize(inserts_and_updates_df, key_columns, value_columns) \
        .union(__harmonize(deletions_df, key_columns, value_columns)) \
        .union(__harmonize(obsoletions_df, key_columns, value_columns))


def __get_empty(spark, current_data_df: DataFrame) -> DataFrame:
    """ Creates an empty slow changing dimension type 2 """

    schema = StructType(current_data_df.schema.fields + 
        [
        StructField("meta_hash_key", TimestampType()),
        StructField("meta_hash_values", TimestampType()),
        StructField("meta_valid_from", TimestampType()),
        StructField("meta_valid_to", TimestampType()),
        StructField("meta_action", StringType()),
        ])

    return spark.createDataFrame([], schema)


def __harmonize(df: DataFrame,
         key_columns: list[str],
         value_columns: list[str]) -> DataFrame:
    """ Harmonize projection of slow changing dimension """

    return df.select(*key_columns, *value_columns, 
        "meta_hash_key", "meta_hash_values", "meta_valid_from", "meta_valid_to", "meta_action")
