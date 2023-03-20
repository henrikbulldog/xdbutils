""" Unit tests """

import os
import unittest
import json
from requests.auth import HTTPBasicAuth
from xdbutils.ingestion import http
from xdbutils.streaming.aws import S3StreamConnector
from xdbutils.streaming.string import StringStreamConnector


class TestIngestHttp(unittest.TestCase):
    """ Test xdbutils.ingest.http """


    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.connectors = [
            StringStreamConnector(),
            S3StreamConnector(
                bucket_name=os.environ['AWS_S3_BUCKET'],
                file_path="openbrainn/test/testing.txt"
                )
        ]


    def test_get(self):
        """ Test xdbutils.ingest.http.get """

        for connector in self.connectors:
            with self.subTest():
                http.get(url="https://postman-echo.com/ip", connector=connector)
                result = connector.read_str()
                self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


    def test_get_html(self):
        """ Test xdbutils.ingest.http.get HTML """

        for connector in self.connectors:
            with self.subTest():
                http.get(url="https://postman-echo.com", connector=connector)
                result = connector.read_str()
                self.assertEqual(result[0:15], "<!DOCTYPE html>")


    def test_auth(self):
        """ Test xdbutils.ingest.http.get authentication """

        for connector in self.connectors:
            with self.subTest():
                http.get(url="https://postman-echo.com/basic-auth", connector=connector, auth=HTTPBasicAuth('postman', 'password'))
                result = json.loads(connector.read_str())
                self.assertEqual(result["authenticated"], True, result)


    def test_post(self):
        """ Test xdbutils.ingest.http.post """

        for connector in self.connectors:
            with self.subTest():
                expected = {"test_key": "test_value"}
                http.post(
                    url="https://postman-echo.com/post",
                    body={"test_key": "test_value"},
                    connector=connector)
                result = json.loads(connector.read_str())
                self.assertEqual(
                    result["data"],
                    expected,
                    f"Expected to contain data: {expected}: {result}")


if __name__ == '__main__':
    unittest.main()
