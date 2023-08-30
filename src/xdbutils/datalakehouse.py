""" Data Lakehouse """

import json
from typing import Callable
import logging
from delta import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.utils import AnalysisException
from pyspark.dbutils import DBUtils
from py4j.protocol import Py4JJavaError

class DataLakehouse():
    """ 
    Data Lake House 
    """

    def __init__(self, spark, raw_path, bronze_path, silver_path, gold_path):
        self._spark = spark
        self._dbutils = DBUtils(spark)
        self._raw_path = raw_path
        self._bronze_path = bronze_path
        self._silver_path = silver_path
        self._gold_path = gold_path

    def raw2bronze_job(self, source_system, entity):
        """ Read raw changes and write to bronze """
        class_path = f"{source_system}/{entity}"
        source_path = f"{self._raw_path}/{class_path}"
        checkpoint_path = f"{self._raw_path}/checkpoints/{class_path}"

        return RawJob(
            spark=self._spark,
            source_path=source_path,
            checkpoint_path=checkpoint_path,
            source_system=source_system,
            target_path=self._bronze_path
            )

    def bronze2silver_job(self, source_system, entity):
        """ Read bronze changes and write to silver """
        class_path = f"{source_system}/{entity}"
        source_path = f"{self._bronze_path}/{class_path}"
        checkpoint_path = f"{self._bronze_path}/checkpoints/{class_path}"

        return Job(
            spark=self._spark,
            source_path=source_path,
            checkpoint_path=checkpoint_path,
            source_system=source_system,
            target_path=self._silver_path
            ).read_from_delta().deduplicate()

    def silver2gold_job(self, source_system, entity):
        """ Read silver changes and write to gold """
        class_path = f"{source_system}/{entity}"
        source_path = f"{self._silver_path}/{class_path}"
        checkpoint_path = f"{self._silver_path}/checkpoints/{class_path}"

        return Job(
            spark=self._spark,
            source_path=source_path,
            checkpoint_path=checkpoint_path,
            source_system=source_system,
            target_path=self._gold_path
            ).read_from_delta()

class Job():
    """ Data Lakehouse Medallion stage job """

    def __init__(self, spark, source_path, checkpoint_path, source_system, target_path):
        self._spark = spark
        self._source_system = source_system
        self._source_path = source_path
        self._checkpoint_path = checkpoint_path
        self._target_path = target_path
        self._source_options = None
        self._source_format = None
        self._source_connection_string = None
        self._transform_callback = None
        self._write_callback = None
        self._catalog = None
        self._target_table = None
        self._target_keys = None
        self._partitions = []
        self._do_deduplicate = False
        self._deduplicate_keys = None
        self._deduplicate_order_by = None
        self._spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
        self._spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")
        self._spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        self._spark.conf.set("spark.databricks.io.cache.enabled", "true")

        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', "%Y-%m-%dT%H:%M:%S")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        if len(self._logger.handlers) == 0:
            # Only add handler the first time
            self._logger.addHandler(handler)

    def _get_schema(self, catalog, schema):
        if catalog:
            return f"{catalog}.{schema}"

        return schema

    def run(self):
        """
        Reads source data as stream and passes it to the microbatch handler function.
        Handles stream checkpointing.
        """
        stream = self._get_stream_reader(
            source_path=self._source_path,
            source_format=self._source_format,
            checkpoint_location=self._checkpoint_path,
            reader_options=self._source_options)
        stream = (stream
                .writeStream
                .option("checkpointLocation", self._checkpoint_path)
                .trigger(once=True)
                .foreachBatch(self._batch_handler)
                .start()
                )
        stream.awaitTermination()

    def read_from_delta(self, options=None):
        """ Read delta tables """
        self._source_options = options
        self._source_format = "delta"
        return self

    def transform(self, callback: Callable[[DataFrame], DataFrame]):
        """ Transform data before writing, provide a callback to do transformation logic """
        self._transform_callback = callback
        return self
    
    def deduplicate(self, keys = None, order_by = None):
        """ Deduplicate """
        self._do_deduplicate = True
        self._deduplicate_keys = keys
        self._deduplicate_order_by = order_by
        return self

    def write_merge(self, table, keys, catalog = None):
        """ Merge changes with existing data, useful with slow changing dimensions """
        self._target_table = table
        self._target_keys = keys
        self._catalog = catalog
        self._write_callback = self._merge
        return self

    def write_append(self, table, catalog = None, partitions=None):
        """ Append changes with existing data, useful for timeseries data """
        self._target_table = table
        self._catalog = catalog
        if partitions:
            self._partitions = partitions
        self._write_callback = self._append
        return self

    def write_overwrite(self, table, catalog = None):
        """ Overwrite existing data """
        self._target_table = table
        self._catalog = catalog
        self._write_callback = self._overwrite
        return self

    def write(self, callback: Callable[[DataFrame, str], None], table, catalog = None):
        """ Custom write, provide a callback with write logic """
        self._write_callback = callback
        self._target_table = table
        self._catalog = catalog
        return self

    def _transform(self, dataframe):
        """ Transform before write """
        if self._transform_callback:
            return self._transform_callback(dataframe)
        return dataframe

    def _deduplicate(self, dataframe):
        """ Deduplicate """
        if self._deduplicate_keys and self._deduplicate_order_by:
            window_function = (Window
                .partitionBy(*self._deduplicate_keys)
                .orderBy(col(self._deduplicate_order_by).desc())
            )
            return (dataframe
                .withColumn('rank', row_number().over(window_function))
                .where(col('rank') == 1)
                .drop('rank')
            )

        return dataframe.dropDuplicates()

    def _write(self, dataframe):
        self._logger.info("Write")
        target_path = f"{self._target_path}/{self._source_system}/{self._target_table}"
        if not self._is_delta_table(target_path):
            self._init_delta_table(
                dataframe=dataframe,
                target_path=target_path,
                catalog=self._catalog,
                schema=self._source_system,
                table=self._target_table)
        else:
            if not self._is_current_catalog_struct(
                catalog=self._catalog,
                schema=self._source_system,
                table=self._target_table
            ):
                self._update_catalog_struct(
                    target_path=target_path,
                    catalog=self._catalog,
                    schema=self._source_system,
                    table=self._target_table
                    )
        if not self._write_callback:
            raise ValueError("No write method provided")
        self._write_callback(dataframe, target_path)

    def _merge(self, dataframe, target_path):
        self._logger.info("Merge %s", target_path)
        key_condition =  " AND ".join(
            [f"source.{c} <=> target.{c}" for c in self._target_keys])
        # We don't want to update matched row unless any of the values (i.e. column that
        # is not part of the primary key nor a metadata column) has not changed.
        value_columns = [col for col in dataframe.columns if col not in self._target_keys]
        if len(value_columns) == 0:
            update_condition = None
        else:
            update_condition = " OR ".join(
                [f"NOT (source.{c} <=> target.{c})" for c in value_columns])
        table = DeltaTable.forPath(self._spark, target_path)
        (
            table.alias("target")
            .merge(source=dataframe.alias("source"), condition=key_condition)
            .whenMatchedUpdateAll(update_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

    def _append(self, dataframe, target_path):
        self._logger.info("Append %s", target_path)
        (
            dataframe.write
            .partitionBy(*self._partitions)
            .format("delta")
            .mode("append")
            .save(target_path)
         )

    def _overwrite(self, dataframe, target_path):
        self._logger.info("Overwrite %s", target_path)
        (
            dataframe.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
         )

    def _batch_handler(self, batch, epoch_id):
        self._logger.info("Processing epoch %s", epoch_id)
        batch = self._transform(batch)
        if self._do_deduplicate:
            batch = self._deduplicate(batch)
        self._write(batch)

    def _get_stream_reader(
        self,
        source_path,
        source_format,
        checkpoint_location,
        reader_options = None):
        """ Wrapper for getting a stream reader """
        self._logger.info("Get stream reader %s, %s, %s, %s",
            source_path, source_format, checkpoint_location, reader_options)
        if not reader_options:
            reader_options = {}
        if source_format.lower() == "delta":
            options = {
                **reader_options
            }
            reader = (self._spark
                        .readStream
                        .format("delta")
                        .options(**reader_options)
                        .load(source_path)
                    )
            return reader
        elif source_format.lower() in ["json", "csv", "parquet"]:
            options = {
                "cloudFiles.format": source_format,
                "cloudFiles.schemaLocation": checkpoint_location,
                "cloudFiles.inferColumnTypes": True,
                "cloudFiles.includeExistingFiles": True,
                "cloudFiles.schemaEvolutionMode": "addNewColumns",
                **reader_options
            }
            # Failed to find data source: cloudFiles.
            reader = (self._spark
                        .readStream
                        .format("cloudFiles")
                        .options(**options)
                        .load(source_path)
                        )
            return reader
        elif source_format.lower() == 'eventhubs':
            connection_string = self._source_connection_string
            options = {
                "eventhubs.consumerGroup": reader_options["eventhubs.consumerGroup"],
                #Added to increase microbatch sizes if needed
                "eventhubs.maxEventsPerTrigger": reader_options["eventhubs.maxEventsPerTrigger"],
                "eventhubs.connectionString": connection_string,
                # NOTE: This will start reading the eventHub topic from the earliest
                #  available offset. However, once a checkpoint has been written,
                #  this configuration will be automatically overwritten by the offsets
                #  found from the checkpoint.
                "eventhubs.startingPosition": json.dumps({
                    "offset": "-1",
                    "seqNo": -1,          # Must be defined, -1 means "not in use"
                    "enqueuedTime": None, # Must be defined, None means "not in use"
                    "isInclusive": True
                })
            }
            reader = (self._spark
                        .readStream
                        .format("eventhubs")
                        .options(**options)
                        .load()
                        )
            return reader
        else:
            raise NotImplementedError(f"Stream reader not implemented for {source_format}!")

    def _is_delta_table(self, path):
        """
        Function to check if the given path is a path for a Delta table.
        Delta API does not work correctly with
        Unity Catalog abfss:// paths, hence this function.
        """
        self._logger.info("Is delta table %s", path)
        try:
            return (self._spark
                        .sql(f"DESCRIBE DETAIL '{path}'")
                        .collect()[0]
                        .asDict()
                    )['format'] == 'delta'
        except Py4JJavaError:
            # File path may not exists at all
            return False

    def _init_delta_table(self, dataframe, target_path, catalog, schema, table):
        """ Initialize delta table """
        self._logger.info("Init delta table %s, %s, %s. %s", target_path, catalog, schema, table)
        (
            dataframe
            .limit(0)
            .write
            .mode("overwrite")
            .format("delta")
            .save(target_path)
        )

        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._get_schema(catalog, schema)}")
        self._spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self._get_schema(catalog, schema)}.{table}
        USING DELTA LOCATION '{target_path}'
        """)
        default_table_properties = [
            "delta.enableChangeDataFeed = true",
            "delta.autoOptimize.optimizeWrite = true",
            "delta.autoOptimize.autoCompact = true",
            "delta.targetFileSize = '32mb'"
        ]
        self._spark.sql(f"""
        ALTER TABLE {self._get_schema(catalog, schema)}.{table}
        SET TBLPROPERTIES ({','.join(default_table_properties)})
        """)

    def _is_current_catalog_struct(self, catalog, schema, table):
        """ 
        Function to check if the table cataloging convention is current, 
        i.e. company_env.medallion_source.tablename.
        """
        self._logger.info("Is current catalog structure %s, %s, %s", catalog, schema, table)
        try:
            type(self._spark.table(f"{self._get_schema(catalog, schema)}.{table}"))
            return True
        except AnalysisException as exc:
            if str(exc).startswith("Table or view not found"):
                return False
            else:
                raise exc

    def _update_catalog_struct(self, target_path, catalog, schema, table):
        """
        Changes catalog structure to current if a table exists in old 
        bronze/silver/gold.schema.table -structure
        """
        self._logger.info("Update catalog %s, %s, %s", target_path, catalog, table)
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._get_schema(catalog, schema)}")
        self._spark.sql(f"DROP TABLE IF EXISTS {self._get_schema(catalog.split('_')[-1], schema)}.{table}")
        self._spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self._get_schema(catalog, schema)}.{table}
        USING DELTA LOCATION '{target_path}'
        """)
        default_table_properties = [
            "delta.enableChangeDataFeed = true",
            "delta.autoOptimize.optimizeWrite = true",
            "delta.autoOptimize.autoCompact = true",
            "delta.targetFileSize = '32mb'"
        ]
        self._spark.sql(f"""
        ALTER TABLE {self._get_schema(catalog, schema)}.{table}
        SET TBLPROPERTIES ({','.join(default_table_properties)})
        """)

class RawJob(Job):
    """ Raw to bronze job """

    def read_from_json(self, options=None):
        """ Read json files """
        self._source_options = options
        self._source_format = "json"
        return self

    def read_from_csv(self, options=None):
        """ Read csv files """
        self._source_options = options
        self._source_format = "csv"
        return self

    def read_from_parquet(self, options=None):
        """ Read parquet files """
        self._source_options = options
        self._source_format = "parquet"
        return self

    def read_from_eventhub(self, connection_string, options=None):
        """ Subscribe to event hub """
        self._source_options = options
        self._source_connection_string = connection_string
        self._source_format = "eventhubs"
        return self
