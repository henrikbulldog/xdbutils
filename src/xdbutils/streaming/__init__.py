""" Connectors to read and write streams """

import tempfile
from typing import Any


class StreamConnector():
    """ StreamConnector base class """


    def read(self, output_stream: Any):
        """ Read from stream """


    def read_str(self) -> str:
        """ Read from stream to a string """

        with tempfile.TemporaryFile() as data:
            self.read(data)
            data.seek(0)
            return data.read().decode('utf-8')


    def write(self, input_stream: Any):
        """ Write from stream """      
