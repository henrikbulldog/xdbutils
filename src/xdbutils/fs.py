""" File System Utils """

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


def ls(
        path: str = None,
        print_files: bool = False,
        data_file_extension: str = "*",
        indent: str = "") -> list[str]:
    """ List folder contents """

    lines = []
    try:
        files = dbutils.fs.ls(path)
    except:
        print("Not found: " + path)
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
                    lines.append(f"{indent}- {file_info.path}, size: {size}")
            elif file_info.path != path:
                lines = lines + ls(file_info.path, print_files,
                                   data_file_extension, indent + "")
    else:
        lines.append(f"{indent}{path}: {file_count}")
    return lines


def exists(path: str) -> bool:
    """ Check if path exists """

    try:
        dbutils.fs.ls(path)
    except:
        return False
    return True


class PartitionedFolder():

    """ Manages files partitioned organised by stage, data source, version, dataset and partitioned by yyyy, mm, dd, time """

    def __init__(self,
                 base_path: str = None,
                 stage: str = None,
                 data_source: str = None,
                 version: str = None,
                 dataset: str = None):

        self.base_path = base_path
        self.stage = stage
        self.data_source = data_source
        self.version = version
        self.dataset = dataset
        path = []
        if self.base_path:
            path.append(self.base_path)
        if self.stage:
            path.append(self.stage)
        if self.data_source:
            path.append(self.data_source)
        if self.version:
            path.append(self.version)
        if self.dataset:
            path.append(self.dataset)
        self.path = "/".join(path)


    def purge(self):
        """ Remove all content from folder """
        if exists(self.path):
            dbutils.fs.rm(dir=self.path, recurse=True)


    def new(self):
        """ New prtition path string """

        dt = datetime.now()
        return f"{self.path}/yyyy={dt.year}/mm={dt.month:02}/dd={dt.day:02}/time={dt.isoformat()}/"


    def ls(self, path: str = None) -> list[str]:
        """ List files recursively """

        if not path:
            path = self.path
        folders = []
        files = self.ls(path)
        for f in files:
            if path.endswith("/") and f != path:
                subfolders = self.ls(f)
                if not subfolders:
                    folders.append(f)
                else:
                    folders += subfolders
        return folders


    def latest(self) -> str:
        """ Get latest partition """

        partitions = self.ls()
        if len(partitions) == 0:
            return None
        partitions.sort(reverse=True)
        return partitions[0]


    def previous(self) -> str:
        """ Get previous partition """
        partitions = self.ls()
        if len(partitions) < 2:
            return None
        partitions.sort(reverse=True)
        return partitions[1]
