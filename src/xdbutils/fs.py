""" File System Utils """

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
                and (file_info.name.endswith(data_file_extension) or data_file_extension == "*"):
                file_count = file_count + 1
                size = file_info.size
                file_size += size
                min_size = min(size, min_size)
                max_size = max(size, max_size)
        if file_count > 0:
            lines.append(f"{indent}{path}: {file_count}"
                + " files avg size: {file_size / file_count}, min: {min_size}, max: {max_size}")
        for file_info in files:
            if not file_info.name.endswith("/") \
                and (file_info.name.endswith(data_file_extension) or data_file_extension == "*"):
                if print_files:
                    lines.append(f"{indent}- {file_info.path}, size: {size}")
            elif file_info.path != path:
                lines = lines + ls(file_info.path, print_files, data_file_extension, indent + "")
    else:
        lines.append(f"{indent}{path}: {file_count}")
    return lines
