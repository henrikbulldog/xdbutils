""" Package xdbutils """

from pyspark.sql import DataFrame
from xdbutils.datalakehouse import DataLakehouse
from xdbutils.pipelines.management import DLTPipelineManager
from xdbutils.pipelines.source import DLTPipelineSource
from xdbutils.pipelines.data import DLTPipelineDataManager
from xdbutils.transforms import scd2
from xdbutils.deprecation import deprecated
from xdbutils.deprecation import deprecated

class XDBUtils():
    """ Extended Databricks Utilities """

    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils
        self._fs = FileSystem(self.spark, self.dbutils)
        self._transforms = Transforms(self.spark)

    @property
    def fs(self): # pylint: disable=invalid-name
        """ File system """
        return self._fs

    @property
    def transforms(self):
        """ Transforms """
        return self._transforms

    def help():
        print("This module exposes a collection of utilities for building enterprise data pipelines")
        print("create_dlt_pipeline_source(source_system, source_class, raw_base_path, tags): DLTPipelineSource -> Create a class with Delta Live Tables source functions")
        print("create_dlt_pipeline_manager(source_system, source_class, catalog, tags, continuous_workflow, serverless, databricks_token, databricks_host): DLTPipelineManager -> Create a Delta Live Table pipeline manager")
        print("create_dlt_data_manager(catalog, source_system, source_class, tags, continuous_workflow, databricks_token, databricks_host, source_path): DLTPipelineDataManager -> Create a Delta Live Tables data manager")
        print("create_datalakehouse(raw_path, bronze_path, silver_path, gold_path): DataLakeHouse -> Create a class with utility functions for a Data Lakehouse data pipeline")
        print("fs: FileSystem -> File system utilities")
        print("transforms: Transforms -> Useful transforms")

    def create_datalakehouse(self, raw_path, bronze_path, silver_path, gold_path):
        return DataLakehouse(self.spark, raw_path, bronze_path, silver_path, gold_path)

    def create_dlt_pipeline_source(
        self,
        source_system,
        source_class,
        raw_base_path,
        tags = None,
        ):

        return DLTPipelineSource(
            spark=self.spark,
            dbutils=self.dbutils,
            source_system=source_system,
            source_class=source_class,
            raw_base_path= raw_base_path,
            tags=tags,
            )

    def create_dlt_pipeline_manager(
        self,
        source_system,
        source_class,
        catalog,
        tags = None,
        continuous_workflow = False,
        serverless = False,
        databricks_token = None,
        databricks_host = None,
        source_path = None,
        ):

        return DLTPipelineManager(
            spark=self.spark,
            dbutils=self.dbutils,
            source_system=source_system,
            source_class=source_class,
            catalog=catalog,
            tags=tags,
            continuous_workflow=continuous_workflow,
            serverless=serverless,
            databricks_token=databricks_token,
            databricks_host=databricks_host,
            source_path=source_path,
            )

    def create_dlt_data_manager(
        self,
        catalog,
        source_system = None,
        source_class = None,
        tags = None,
        continuous_workflow = False,
        databricks_token = None,
        databricks_host = None,
        source_path = None,
        ):

        return DLTPipelineDataManager(
            spark=self.spark,
            dbutils=self.dbutils,
            source_system=source_system,
            source_class=source_class,
            catalog=catalog,
            raw_base_path= None,
            tags=tags,
            continuous_workflow=continuous_workflow,
            databricks_token=databricks_token,
            databricks_host=databricks_host,
            source_path=source_path,
            create_or_update = False,
            )


class Transforms():
    """ Transforms """

    def __init__(self, spark):
        self.spark = spark

    def update_slow_changing_dimension_type2(
        self,
        current_data_df: DataFrame,
        key_columns: list[str],
        latest_version_df: DataFrame = None,
        handle_deletions: bool = False,
        value_columns: list[str] = None,
    ) -> DataFrame:
        """ Calculate the changes in a slow changing dimension type 2 """
        return scd2.diff(
            self.spark,
            current_data_df,
            key_columns,
            latest_version_df,
            handle_deletions,
            value_columns,
            )


class FileSystem():
    """ File system """

    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils

    def ls( # pylint: disable=invalid-name
            self,
            path: str = None,
            print_files: bool = False,
            data_file_extension: str = "*",
            indent: str = "") -> list[str]:
        """ List folder contents """

        lines = []
        try:
            files = self.dbutils.fs.ls(path)
        except: # pylint: disable=bare-except
            lines.append("Not found: " + path)
            return lines
        file_count = 0
        file_size = 0
        if len(files) > 0:
            min_size = 0
            max_size = 0
            for file_info in files:
                if not file_info.name.endswith("/") \
                    and (file_info.name.endswith(data_file_extension)
                         or data_file_extension == "*"):
                    file_count = file_count + 1
                    size = file_info.size
                    file_size += size
                    min_size = min(size, min_size)
                    max_size = max(size, max_size)
            if file_count > 0:
                lines.append(
                    f"{indent}{path}: {file_count}"
                    + f" files avg size: {file_size / file_count},"
                    + f" min: {min_size}, max: {max_size}")
            for file_info in files:
                if not file_info.name.endswith("/") \
                    and (file_info.name.endswith(data_file_extension)
                         or data_file_extension == "*"):
                    if print_files:
                        lines.append(
                            f"{indent}- {file_info.path}, size: {size}")
                elif file_info.path != path:
                    lines = lines + self.ls(file_info.path, print_files,
                                            data_file_extension, indent + "")
        else:
            lines.append(f"{indent}{path}: {file_count}")
        return lines

    def exists(self, path: str) -> bool:
        """ Check if path exists """

        try:
            self.dbutils.fs.ls(path)
        except: # pylint: disable=bare-except
            return False
        return True
