""" Package xdbutils """

from pyspark.sql import DataFrame
from xdbutils.datalakehouse import DataLakehouse
from xdbutils.pipelines import Pipeline
from xdbutils.transforms import scd2

class XDBUtils():
    """ Extended Databricks Utilities """

    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils

    @property
    def fs(self):
        """ File system """
        return FileSystem(self.spark, self.dbutils)

    @property
    def pipeline(self):
        """ Delta Live Tables Pipeline """
        return Pipeline()

    @property
    def transforms(self):
        """ Transforms """
        return Transforms(self.spark)

    def create_datalakehouse(self, raw_path, bronze_path, silver_path, gold_path):
        """ Create data Lake House """
        return DataLakehouse(self.spark, raw_path, bronze_path, silver_path, gold_path)


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
        return scd2.diff(self.spark, current_data_df, key_columns, latest_version_df, handle_deletions, value_columns)


class FileSystem():
    """ File system """

    def __init__(self, spark, dbutils):
        self.spark = spark
        self.dbutils = dbutils

    def ls(
            self,
            path: str = None,
            print_files: bool = False,
            data_file_extension: str = "*",
            indent: str = "") -> list[str]:
        """ List folder contents """

        lines = []
        try:
            files = self.dbutils.fs.ls(path)
        except:
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
                lines.append(f"{indent}{path}: {file_count}"
                             + f" files avg size: {file_size / file_count}, min: {min_size}, max: {max_size}")
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
        except:
            return False
        return True
