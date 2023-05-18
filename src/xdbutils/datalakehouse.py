""" Data Lakehouse """

import json
from typing import Callable
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.dbutils import DBUtils
from py4j.protocol import Py4JJavaError
import logging

class DataLakehouse():
    """ 
    Data Lake House 
    TODO: with_hive_catalog(), with_unity_calatog(), with_adsl(), with_awss3()
    """

    def __init__(self, spark, base_catalog, base_path):
        self.spark = spark
        self.dbutils = DBUtils(spark)
        self.base_catalog = base_catalog
        self.raw_path = f"{base_path}/raw"
        self.bronze_path = f"{base_path}/bronze"
        self.silver_path = f"{base_path}/silver"
        self.gold_path = f"{base_path}/gold"

    def raw2bronze(self, source_system, entity):
        """ Read raw changes and write to bronze """
        return Raw2BronzeJob(
            spark=self.spark,
            catalog=f"{self.base_catalog}_bronze",
            source_path=self.raw_path,
            source_system=source_system,
            entity=entity,
            target_path=self.bronze_path
            )

    def bronze2silver(self, source_system, entity):
        """ Read bronze changes and write to silver """
        return Bronze2SilverJob(
            spark=self.spark,
            catalog=f"{self.base_catalog}_silver",
            source_path=self.bronze_path,
            source_system=source_system,
            entity=entity,
            target_path=self.silver_path
            ).read_from_parquet()

    def silver2gold(self, source_system, entity):
        """ Read silver changes and write to gold """
        return Silver2GoldJob(
            spark=self.spark,
            catalog=f"{self.base_catalog}_gold",
            source_path=self.silver_path,
            source_system=source_system,
            entity=entity,
            target_path=self.gold_path
            ).read_from_parquet()

class Job():
    """ Data Lakehouse Medallion stage job """

    def __init__(self, spark, catalog, source_path, source_system, entity, target_path, unity_catalog=True):
        self.spark = spark
        self.catalog = catalog
        self.source_system = source_system
        self.entity = entity
        self.source_path = source_path
        self.target_path = target_path
        self.unity_catalog = unity_catalog
        self.source_options = None
        self.source_format = None
        self.source_connection_string = None
        self.transform_callback = None
        self.write_callback = None
        self.target_table = None
        self.target_keys = None
        self.partitions = []
        self.spark.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")
        self.spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        self.spark.conf.set("spark.databricks.io.cache.enabled", "true")

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', "%Y-%m-%dT%H:%M:%S")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        if (len(self.logger.handlers) == 0):
            # Only add handler the first time
            self.logger.addHandler(handler)

    def _get_schema(self, catalog, schema):
        if self.unity_catalog:
            return f"{catalog}.{schema}"
        return f"{catalog}__{schema}"

    def __run(self):
        """
        Reads source data as stream and passes it to the microbatch handler function.
        Handles stream checkpointing.
        """
        checkpoint_path = f"{self.source_path}/checkpoints"
        stream = self._get_stream_reader(
            source_path=self.source_path,
            source_format=self.source_format,
            checkpoint_location=checkpoint_path,
            reader_options=self.source_options)
        stream = (stream
                .writeStream
                .option("checkpointLocation", checkpoint_path)
                .trigger(once=True)
                .foreachBatch(self._batch_handler)
                .start()
                )
        stream.awaitTermination()

    def read_from_json(self, options=None):
        """ Read json files """
        self.source_options = options
        self.source_format = "json"
        return self

    def read_from_csv(self, options=None):
        """ Read csv files """
        self.source_options = options
        self.source_format = "csv"
        return self

    def read_from_parquet(self, options=None):
        """ Read parquet files """
        self.source_options = options
        self.source_format = "parquet"
        return self

    def read_from_eventhub(self, connection_string, options=None):
        """ Subscribe to event hub """
        self.source_options = options
        self.source_connection_string = connection_string
        self.source_format = "eventhubs"
        return self

    def with_transform(self, callback: Callable[[DataFrame], DataFrame]):
        """ Transform data before writing, provide a callback to do transformation logic """
        self.transform_callback = callback
        return self

    def write_merge(self, table, keys):
        """ Merge changes with existing data, useful with slow changing dimensions """
        self.target_table = table
        self.target_keys = keys
        self.write_callback = self._merge
        self.__run()

    def write_append(self, table, partitions=None):
        """ Append changes with existing data, useful for timeseries data """
        self.target_table = table
        if partitions:
            self.partitions = partitions
        self.write_callback = self._append
        self.__run()

    def write_overwrite(self, table):
        """ Overwrite existing data """
        self.target_table = table
        self.write_callback = self._overwrite
        self.__run()

    def with_write(self, table, callback: Callable[[DataFrame, str], None]):
        """ Custom write, provide a callback with write logic """
        self.target_table = table
        self.write_callback = callback
        self.__run()

    def _transform(self, dataframe):
        """ Transform before write """
        if self.transform_callback:
            return self.transform_callback(dataframe)
        return dataframe

    def _write(self, dataframe):
        self.logger.info(f"Write")
        target_path = f"{self.target_path}/{self.source_system}/{self.target_table}"
        if not self._is_delta_table(target_path):
            self._init_delta_table(
                dataframe=dataframe,
                target_path=target_path,
                catalog=self.catalog,
                schema=self.source_system,
                table=self.target_table)
        else:
            if not self._is_current_catalog_struct(
                catalog=self.catalog,
                schema=self.source_system,
                table=self.target_table
            ):
                self._update_catalog_struct(
                    target_path=target_path,
                    catalog=self.catalog,
                    schema=self.source_system,
                    table=self.target_table
                    )
        if not self.write_callback:
            raise ValueError("No write method provided")
        self.write_callback(dataframe, target_path)

    def _merge(self, dataframe, target_path):
        self.logger.info(f"Merge {target_path}")
        key_condition =  " AND ".join(
            [f"source.{c} <=> target.{c}" for c in self.target_keys])
        # We don't want to update matched row unless any of the values (i.e. column that
        # is not part of the primary key nor a metadata column) has not changed.
        value_columns = [col for col in dataframe.columns if col not in self.target_keys]
        update_condition = " OR ".join(
            [f"NOT (source.{c} <=> target.{c})" for c in value_columns])
        table = DeltaTable.forPath(self.spark, target_path)
        (
            table.alias("target")
            .merge(source=dataframe.alias("source"), condition=key_condition)
            .whenMatchedUpdateAll(update_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

    def _append(self, dataframe, target_path):
        self.logger.info(f"Append {target_path}")
        (
            dataframe.write
            .partitionBy(*self.partitions)
            .format("delta")
            .mode("append")
            .save(target_path)
         )

    def _overwrite(self, dataframe, target_path):
        self.logger.info(f"Overwrite {target_path}")
        (
            dataframe.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
         )

    def _batch_handler(self, batch, epoch_id):
        pass

    def _get_stream_reader(
        self,
        source_path,
        source_format,
        checkpoint_location,
        reader_options = None,
        change_types = None):
        """ Wrapper for getting a stream reader """
        self.logger.info(f"Get stream reader {source_path}, {source_format}, {checkpoint_location}, {reader_options}")
        if not reader_options:
            reader_options = {}
        if not change_types:
            change_types = ['insert', 'update_postimage']
        if source_format.lower() == 'delta':
            options = {
                "readChangeFeed": "true",
                **reader_options
            }
            reader = (self.spark
                        .readStream
                        .format("delta")
                        .options(**reader_options)
                        .load(source_path)
                        # This is now parameterized for custom CDC reader jobs
                        .where("_change_type IN ('" + "', '".join(change_types) + "')")
                    )
            return reader
        elif source_format.lower() in ["json", "csv", "parquet"]:
            options = {
                "cloudFiles.format": source_format,
                "cloudFiles.schemaLocation": checkpoint_location,
                "cloudFiles.inferColumnTypes": True,
                "cloudFiles.includeExistingFiles": True,
                **reader_options
            }
            # Failed to find data source: cloudFiles.
            reader = (self.spark
                        .readStream
                        .format("cloudFiles")
                        .options(**options)
                        .load(source_path)
                        )
            return reader
        elif source_format.lower() == 'eventhubs':
            connection_string = self.source_connection_string
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
            reader = (self.spark
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
        self.logger.info(f"Is delta table {path}")
        try:
            return (self.spark
                        .sql(f"DESCRIBE DETAIL '{path}'")
                        .collect()[0]
                        .asDict()
                    )['format'] == 'delta'
        except Py4JJavaError:
            # File path may not exists at all
            return False

    def _init_delta_table(self, dataframe, target_path, catalog, schema, table):
        """ Initialize delta table """
        self.logger.info(f"Init delta table {target_path}, {catalog}, {schema}, {table}")
        (
            dataframe
            .limit(0)
            .write
            .mode("overwrite")
            .format("delta")
            .save(target_path)
        )

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._get_schema(catalog, schema)}")
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self._get_schema(catalog, schema)}.{table}
        USING DELTA LOCATION '{target_path}'
        """)
        default_table_properties = [
            "delta.enableChangeDataFeed = true",
            "delta.autoOptimize.optimizeWrite = true",
            "delta.autoOptimize.autoCompact = true",
            "delta.targetFileSize = '32mb'"
        ]
        self.spark.sql(f"""
        ALTER TABLE {self._get_schema(catalog, schema)}.{table}
        SET TBLPROPERTIES ({','.join(default_table_properties)})
        """)

    def _is_current_catalog_struct(self, catalog, schema, table):
        """ 
        Function to check if the table cataloging convention is current, 
        i.e. company_env.medallion_source.tablename.
        """
        self.logger.info(f"Is current catalog structure {catalog}, {schema}, {table}")
        try:
            type(self.spark.table(f"{self._get_schema(catalog, schema)}.{table}"))
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
        self.logger.info(f"Update catalog {target_path}, {catalog}, {table}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._get_schema(catalog, schema)}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self._get_schema(catalog.split('_')[-1], schema)}.{table}")
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self._get_schema(catalog, schema)}.{table}
        USING DELTA LOCATION '{target_path}'
        """)
        default_table_properties = [
            "delta.enableChangeDataFeed = true",
            "delta.autoOptimize.optimizeWrite = true",
            "delta.autoOptimize.autoCompact = true",
            "delta.targetFileSize = '32mb'"
        ]
        self.spark.sql(f"""
        ALTER TABLE {self._get_schema(catalog, schema)}.{table}
        SET TBLPROPERTIES ({','.join(default_table_properties)})
        """)

class Raw2BronzeJob(Job):
    """ Data Lakehouse Medallion raw to bronze job """
    def _batch_handler(self, batch, epoch_id):
        self.logger.info("Processing epoch %s", epoch_id)
        batch = self._transform(batch)
        batch_to_write = batch.dropDuplicates()
        self._write(batch_to_write)

class Bronze2SilverJob(Job):
    """ Data Lakehouse Medallion bronze to silver job """
    def _batch_handler(self, batch, epoch_id):
        self.logger.info("Processing epoch %s", epoch_id)

class Silver2GoldJob(Job):
    """ Data Lakehouse Medallion silver to gold job """
    def _batch_handler(self, batch, epoch_id):
        self.logger.info("Processing epoch %s", epoch_id)
