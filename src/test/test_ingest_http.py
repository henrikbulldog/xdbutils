""" Unit tests """

import unittest
import json
from requests.auth import HTTPBasicAuth
from xdbutils.ingest.http import HttpGet, HttpPost
from xdbutils.writers.string import StringWriter


class HttpRequestTestCase(unittest.TestCase):
    """ Test HttpRequest """


    def test_get(self):
        """ Test get HTML """

        writer = StringWriter()
        get_ip = HttpGet("https://postman-echo.com/ip", writer)
        get_ip()
        self.assertTrue("ip" in writer.value, f"key 'ip' not in: {writer.value}")


    def test_get_html(self):
        """ Test get HTML """

        writer = StringWriter()
        get_site = HttpGet("https://postman-echo.com", writer)
        get_site()
        self.assertEqual(writer.value[0:15], "<!DOCTYPE html>")


    def test_post(self):
        """ Test get HTML """

        writer = StringWriter()
        expected = {"test_key": "test_value"}
        post = HttpPost(
            url="https://postman-echo.com/post",
            body={"test_key": "test_value"},
            write=writer)
        post()
        result = json.loads(writer.value)
        self.assertEqual(
            result["data"],
            expected,
            f"Expected to contain data: {expected}: {result}")


    def test_auth(self):
        """ Test authentication """

        writer = StringWriter()
        basic_auth = HttpGet("https://postman-echo.com/basic-auth", writer)
        basic_auth.auth=HTTPBasicAuth('postman', 'password')
        basic_auth()
        result = json.loads(writer.value)
        self.assertEqual(result["authenticated"], True, writer.value)


if __name__ == '__main__':
    unittest.main()
