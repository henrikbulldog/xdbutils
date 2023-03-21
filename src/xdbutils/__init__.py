""" Package xdbutils """

from xdbutils import fs
from xdbutils.ingestion.http import get as http_get, post as http_post
from xdbutils.streaming import StreamConnector
from xdbutils.streaming.aws import S3StreamConnector
from xdbutils.streaming.az import AdfsStreamConnector
from xdbutils.streaming.string import StringStreamConnector

class XDBUtils():
    """ Extended Databricks Utilities """


    def __init__(self):
        self.fs = FileSystem()
        self.ingestion = Ingestion()


    def create_string_stream_connector(self) -> StringStreamConnector:
        """ Create a string stream connector """

        return StringStreamConnector()


    def create_aws_s3_stream_connector(self,
        bucket_name: str,
        file_path: str,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        aws_session_token: str = None,
        aws_region_name: str = None) -> S3StreamConnector:
        """ Create an AWS S3 stream connector """

        return S3StreamConnector(bucket_name, file_path,
            aws_access_key_id, aws_secret_access_key,
            aws_session_token, aws_region_name)


    def create_adfs_stream_connector(self,
        container: str,
        directory: str,
        file_name: str,
        storage_account_name: str = None) -> AdfsStreamConnector:
        """ Create an Azure Data File System stream connector """

        return AdfsStreamConnector(container, directory,
            file_name, storage_account_name)


class Ingestion():
    """ Ingestion utilities """

    def __init__(self):
        self.http = HttpIngestor()


class HttpIngestor():
    """ HTTP ingestor """

    def get(self,
        url: str,
        connector: StreamConnector,
        params: dict = None,
        headers: dict = None,
        auth = None) -> None:
        """ HTTP GET request """

        http_get(url, connector, params, headers, auth)


    def post(self,
        url: str,
        body: dict,
        connector: StreamConnector,
        params: dict = None,
        headers: dict = None,
        auth = None) -> None:
        """ HTTP POST request """

        http_post(url, body, connector,
            params, headers, auth)


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
