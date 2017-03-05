import unittest
import datetime

from querygraph.query.db_types.sqlite.template_parameter import SqliteParameter
from querygraph.query.db_types.sqlite.query_template import SqliteTemplate


class ParameterTests(unittest.TestCase):

    def test_datetime(self):
        param_str = "test_datetime|value:datetime"
        test_param = SqliteParameter(parameter_type='independent', parameter_str=param_str)
        test_param_vals = {'test_datetime': datetime.datetime(2009, 1, 6, 1, 1, 1)}
        expected_val = "datetime(2009-01-06 01:01:01)"
        self.assertEquals(expected_val, test_param.query_value(independent_params=test_param_vals))

    def test_date(self):
        param_str = "test_date|value:date"
        test_param = SqliteParameter(parameter_type='independent', parameter_str=param_str)
        test_param_vals = {'test_date': datetime.datetime(2009, 1, 6, 1, 1, 1)}
        expected_val = "date(2009-01-06)"
        self.assertEquals(expected_val, test_param.query_value(independent_params=test_param_vals))

    def test_time(self):
        param_str = "test_time|value:time"
        test_param = SqliteParameter(parameter_type='independent', parameter_str=param_str)
        test_param_vals = {'test_time': datetime.datetime(2009, 1, 6, 1, 1, 1)}
        expected_val = "time(01:01:01)"
        self.assertEquals(expected_val, test_param.query_value(independent_params=test_param_vals))