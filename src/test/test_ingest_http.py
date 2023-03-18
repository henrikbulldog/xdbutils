""" Unit tests """

import unittest
from requests.auth import HTTPBasicAuth
from xdbutils.ingest.http import HttpGet, HttpPost


class HttpRequestTestCase(unittest.TestCase):
    """ Test HttpRequest """


    def test_get(self):
        """ Test get HTML """

        get_ip = HttpGet("https://postman-echo.com/ip")
        result = get_ip()
        print(result)
        self.assertTrue("ip" in result, f"key 'ip' not in: {result}")


    def test_get_html(self):
        """ Test get HTML """

        get_site = HttpGet("https://google.com")
        get_site.headers = {"Content-Type": "text/html"}
        expected = "<!doctype html>"
        get_site.on_response = lambda r: r.text[0:15]
        result = get_site()
        self.assertEqual(result, expected, f"Not expected: {result}")


    def test_post(self):
        """ Test get HTML """

        expected = {"test_key": "test_value"}
        post = HttpPost(
            url="https://postman-echo.com/post",
            body={"test_key": "test_value"})
        result = post()
        self.assertEqual(
            result["data"],
            expected,
            f"Expected to contain data: {expected}: {result}")


    def test_auth(self):
        """ Test authentication """

        basic_auth = HttpGet("https://postman-echo.com/basic-auth")
        basic_auth.auth=HTTPBasicAuth('postman', 'password')
        result = basic_auth()
        self.assertEqual(result["authenticated"], True)


if __name__ == '__main__':
    unittest.main()
