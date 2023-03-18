""" HTTP Request callable """

from __future__ import annotations
from typing import Any
import json
import requests


class HttpRequest():
    """ HTTP Request callable"""

    def __init__(self, url: str, stream=False):
        self.url = url
        self.params = None
        self.headers = {"Content-Type": "application/json"}
        self.auth = None
        self.stream = stream


    def on_response(self, response) -> Any:
        """ Callback on response """


    def __call__(self) -> Any:
        pass


class HttpGet(HttpRequest):
    """ HTTP get callable"""

    def on_response(self, response) -> Any:
        """ Callback on response """
        return response.json()


    def __call__(self) -> Any:
        with requests.Session() as session:
            response = session.get(url=self.url, params=self.params,
                                    headers=self.headers, auth=self.auth, stream=self.stream)
            response.raise_for_status()
            return self.on_response(response)




class HttpPost(HttpRequest):
    """ HTTP get callable"""

    def __init__(self, url: str, body: dict, stream=False):
        super().__init__(url, stream)
        self.body = body


    def on_response(self, response) -> Any:
        """ Callback on response """
        return response.json()


    def __call__(self) -> Any:
        with requests.Session() as session:
            response = session.post(url=self.url, params=self.params, data=json.dumps(self.body),
                headers=self.headers, auth=self.auth, stream=self.stream)
            response.raise_for_status()
            return self.on_response(response)
