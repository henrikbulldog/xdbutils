""" HTTP Request callable """

import json
import requests

from xdbutils.streaming import StreamConnector


def get(url: str,
        connector: StreamConnector,
        params: dict = None,
        headers: dict = None,
        auth = None):
    """ HTTP GET request """

    with requests.Session() as session:
        response = session.get(url=url,
                               params=params,
                               headers=headers,
                               auth=auth,
                               stream=True)
        response.raise_for_status()
        with response as part:
            part.raw.decode_content = True
            connector.write(part.raw)


def post(url: str,
        body: dict,
        connector: StreamConnector,
        params: dict = None,
        headers: dict = None,
        auth = None):
    """ HTTP POST request """

    with requests.Session() as session:
        response = session.post(url=url,
                                data=json.dumps(body),
                                params=params,
                                headers=headers,
                                auth=auth,
                                stream=True)
        response.raise_for_status()
        with response as part:
            part.raw.decode_content = True
            connector.write(part.raw)
