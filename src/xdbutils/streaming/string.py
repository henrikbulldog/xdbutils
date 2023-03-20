""" String writer """

from typing import Any
from xdbutils.streaming import StreamConnector


class StringStreamConnector(StreamConnector):
    """ String writer """


    def __init__(self):
        self.value = ""


    def read(self, output_stream: Any):
        """ Read from stream """
        output_stream.write(bytes(self.value, 'utf-8'))


    def write(self, input_stream):
        """ Write to stream """
        self.value += input_stream.read().decode('utf-8')
