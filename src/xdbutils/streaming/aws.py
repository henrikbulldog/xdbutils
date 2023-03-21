""" String writer """

import os
from typing import Any
import boto3
from boto3.s3.transfer import TransferConfig

from xdbutils.streaming import StreamConnector


class S3StreamConnector(StreamConnector):
    """ AWS S3 stream connector """


    def __init__(self,
                 bucket_name: str,
                 file_path: str,
                 aws_access_key_id: str = None,
                 aws_secret_access_key: str = None,
                 aws_session_token: str = None,
                 aws_region_name: str = None):

        self.s3_client = boto3.Session(
            aws_access_key_id=aws_access_key_id if aws_access_key_id
                else os.getenv("AWS_ACCESS_KEY_ID", None),
            aws_secret_access_key=aws_secret_access_key if aws_secret_access_key
                else os.getenv("AWS_SECRET_ACCESS_KEY", None),
            aws_session_token=aws_session_token if aws_session_token
                else os.getenv("AWS_SESSION_TOKEN", None),
            region_name=aws_region_name if aws_region_name
                else os.getenv("AWS_DEFAULT_REGION", None)
            ).client('s3')
        self.bucket_name = bucket_name
        self.file_path = file_path


    def read(self, output_stream: Any):
        """ Read from stream """
        self.s3_client.download_fileobj(self.bucket_name, self.file_path, output_stream)


    def write(self, input_stream: Any):
        """ Write to stream """
        conf = TransferConfig(multipart_threshold=10000, max_concurrency=4)
        self.s3_client.upload_fileobj(input_stream, self.bucket_name, self.file_path, Config=conf)
 