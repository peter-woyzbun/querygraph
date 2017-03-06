import unittest
import datetime

from querygraph.query.templates.sqlite.query_template import SqliteParameter, SqliteTemplate
from querygraph.db.test_data.connectors import daily_ts_connector, hourly_ts_connector


class ParameterTests(unittest.TestCase):

    def test_datetime(self):
        param_str = "test_datetime|value:datetime"
        test_param = SqliteParameter(parameter_type='independent', parameter_str=param_str)
        test_param_vals = {'test_datetime': datetime.datetime(2009, 1, 6, 1, 1, 1)}
        expected_val = "'2009-01-06 01:01:01'"
        self.assertEquals(expected_val, test_param.query_value(independent_params=test_param_vals))

    def test_date(self):
        param_str = "test_date|value:date"
        test_param = SqliteParameter(parameter_type='independent', parameter_str=param_str)
        test_param_vals = {'test_date': datetime.datetime(2009, 1, 6, 1, 1, 1)}
        expected_val = "'2009-01-06'"
        self.assertEquals(expected_val, test_param.query_value(independent_params=test_param_vals))

    def test_time(self):
        param_str = "test_time|value:time"
        test_param = SqliteParameter(parameter_type='independent', parameter_str=param_str)
        test_param_vals = {'test_time': datetime.datetime(2009, 1, 6, 1, 1, 1)}
        expected_val = "'01:01:01'"
        self.assertEquals(expected_val, test_param.query_value(independent_params=test_param_vals))


class TemplateTests(unittest.TestCase):

    def test_date_values_format_a(self):

        template_str = """
        SELECT *
        FROM daily_ts
        WHERE date(date_a) IN {% dates|value_list:date %}
        """

        query_template = SqliteTemplate(template_str=template_str, db_connector=daily_ts_connector)

        df = query_template.execute(dates=[datetime.datetime(2016, 1, 1), datetime.datetime(2016, 1, 2)])
        expected_value = ['2016-01-01', '2016-01-02']

        self.assertEquals(expected_value, df['date_a'].tolist())