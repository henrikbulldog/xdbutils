""" HTTP Request callable """

from __future__ import annotations
from typing import Any
import json
import requests


class HttpRequest():
    """ HTTP Request callable"""

    def __init__(self, url: str, write):
        self.url = url
        self.params = None
        self.headers = {"Content-Type": "application/json"}
        self.auth = None
        self.write = write


    def __call__(self) -> Any:
        pass


class HttpGet(HttpRequest):
    """ HTTP get callable"""


    def _write(self, response: requests.Response):
        response.raise_for_status()
        with response as part:
            part.raw.decode_content = True
            self.write(part.raw)


    def __call__(self) -> Any:
        with requests.Session() as session:
            response = session.get(url=self.url, params=self.params,
                                    headers=self.headers, auth=self.auth, stream=True)
            self._write(response)



class HttpPost(HttpGet):
    """ HTTP get callable"""

    def __init__(self, url: str, body: dict, write):
        super().__init__(url, write)
        self.body = body


    def __call__(self) -> Any:
        with requests.Session() as session:
            response = session.post(url=self.url, params=self.params, data=json.dumps(self.body),
                headers=self.headers, auth=self.auth, stream=True)
            self._write(response)
