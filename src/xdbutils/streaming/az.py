""" Azure Data Lake Storage Account Streaming Connector """

from typing import Any
import os
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from xdbutils.streaming import StreamConnector


class AdfsStreamConnector(StreamConnector):
    """ Azure data Lake Storage stream connector """

    def __init__(self,
                 container: str,
                 directory: str,
                 file_name: str,
                 storage_account_name: str = os.environ['ADFS_DEFAULT_STORAGE_ACCOUNT']):

        self.container = container
        self.directory = directory
        self.file_name = file_name
        default_credential = DefaultAzureCredential()
        self.adfs_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=default_credential)


    def read(self, output_stream: Any):
        """ Read from stream """

        file_system_client = self.adfs_client.get_file_system_client(file_system=self.container)
        directory_client = file_system_client.get_directory_client(self.directory)
        file_client = directory_client.get_file_client(self.file_name)
        downloader = file_client.download_file()
        downloader.readinto(output_stream)


    def write(self, input_stream: Any):
        """ Write to stream """

        file_system_client = self.adfs_client.get_file_system_client(file_system=self.container)
        directory_client = file_system_client.get_directory_client(self.directory)
        file_client = directory_client.create_file(self.file_name)
        file_client.upload_data(input_stream, overwrite=True)
