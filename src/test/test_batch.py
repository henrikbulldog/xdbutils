""" Unit tests """

import unittest
from xdbutils.batch import Batch, async_wrap


class BatchTestCase(unittest.TestCase):
    """ Test Batch """

    def test_batch(self):
        """ Test Batch append and callable """
        inc = lambda x: x + 1

        @async_wrap
        def inc_async(x):
            return x+1

        batch = Batch() \
            .append(inc, x=1) \
            .append_async(inc_async(x=2)) \
            .append_async(async_wrap(inc)(x=3))

        result = batch()
        self.assertEqual(result, [2, 3, 4])


if __name__ == '__main__':
    unittest.main()
