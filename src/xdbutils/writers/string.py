""" String writer """

class StringWriter():
    """ String writer """

    def __init__(self):
        self.value = ""


    def __call__(self, stream):
        self.value += stream.read().decode('utf-8')
