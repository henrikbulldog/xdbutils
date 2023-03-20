""" Unit tests """

import datetime
from io import BytesIO
import os
import unittest

from xdbutils.streaming.aws import S3StreamConnector


class TestS3StreamConnector(unittest.TestCase):
    """ Test xdbutils.streaming.aws.S3StreamConnector """


    def test_write_read(self):
        """ xdbutils.streaming.aws.S3StreamConnector write and read """

        bucket_name = os.environ['AWS_S3_BUCKET']
        file_path = "openbrainn/test/testing.txt"
        s3_connector = S3StreamConnector(bucket_name, file_path)
        payload = f"Testing.\n123.\næøåÆØÅ\n{datetime.datetime.now()}"
        with BytesIO() as stream:
            stream.write(payload.encode('utf-8'))
            stream.seek(0)
            s3_connector.write(stream)

        actual_payload = s3_connector.read_str()

        self.assertEqual(actual_payload, payload)


if __name__ == '__main__':
    unittest.main()
