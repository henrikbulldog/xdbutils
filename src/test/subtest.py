from unittest import TestCase

param_list = [('a', 'a'), ('a', 'a'), ('b', 'b')]

class TestDemonstrateSubtest(TestCase):
    def test_works_as_expected(self):
        for p1, p2 in param_list:
            with self.subTest():
                self.assertEqual(p1, p2)