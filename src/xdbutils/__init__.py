""" Package xdbutils """

from xdbutils import fs

class XDBUtils():
    """ Extended Databricks Utilities """


    def __init__(self):
        self.fs = FileSystem()


class FileSystem():
    """ File system """

    def ls(self,
        path: str = None,
        print_files: bool = False,
        data_file_extension: str = "*") -> list[str]:
        """ List folder contents """

        return fs.ls(path, print_files, data_file_extension)


    def exists(self, path: str) -> bool:
        """ Check if path exists """

        return fs.exists(path)


    def create_partitioned_folder(self,
        base_path: str = None,
        stage: str = None,
        data_source: str = None,
        version: str = None,
        dataset: str = None) -> fs.PartitionedFolder:
        """ Create a partitioned folder """

        return fs.PartitionedFolder(base_path, stage,
            data_source, version, dataset)
