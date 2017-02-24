import unittest
import pandas as pd
import numpy as np

from querygraph.evaluation.expr_funcs import Uppercase, Lowercase


uppercase = Uppercase()
lowercase = Lowercase()


class UppercaseTests(unittest.TestCase):

    def test_str_execute(self):
        test_str = 'abc'
        self.assertEquals('ABC', uppercase(test_str))

    def test_list_execute(self):
        test_list = ['abc', 'abc']
        self.assertEquals(['ABC', 'ABC'], uppercase(test_list))

    def test_series_execute(self):
        test_series = pd.Series(['abc', 'abc'])
        self.assertEquals(True, pd.Series(['ABC', 'ABC']).equals(uppercase(test_series)))

    def _test_np_array(self):
        test_np_array = np.array(['abc', 'abc'])
        self.assertEquals(True, np.array_equal(uppercase(test_np_array), np.array(['ABC', 'ABC'])))


class LowercaseTests(unittest.TestCase):

    def test_str_execute(self):
        test_str = 'ABC'
        self.assertEquals('abc', lowercase(test_str))

    def test_list_execute(self):
        test_list = ['ABC', 'ABC']
        self.assertEquals(['abc', 'abc'], lowercase(test_list))

    def test_series_execute(self):
        test_series = pd.Series(['ABC', 'ABC'])
        self.assertEquals(True, pd.Series(['abc', 'abc']).equals(lowercase(test_series)))

    def _test_np_array(self):
        test_np_array = np.array(['ABC', 'ABC'])
        self.assertEquals(True, np.array_equal(lowercase(test_np_array), np.array(['abc', 'abc'])))