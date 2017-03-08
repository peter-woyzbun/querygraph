import unittest
import datetime

from querygraph.query.templates import postgres
from tests.db import connectors


class ParameterTests(unittest.TestCase):

    def test_datetime(self):
        param_str = "test_datetime|value:datetime"
        test_param = postgres.PostgresParameter(parameter_type='independent', parameter_str=param_str)
        test_param_vals = {'test_datetime': datetime.datetime(2009, 1, 6, 1, 1, 1)}
        expected_val = "'2009-01-06 01:01:01'"
        self.assertEquals(expected_val, test_param.query_value(independent_params=test_param_vals))

    def test_date(self):
        param_str = "test_date|value:date"
        test_param = postgres.PostgresParameter(parameter_type='independent', parameter_str=param_str)
        test_param_vals = {'test_date': datetime.datetime(2009, 1, 6, 1, 1, 1)}
        expected_val = "'2009-01-06'"
        self.assertEquals(expected_val, test_param.query_value(independent_params=test_param_vals))

    def test_time(self):
        param_str = "test_time|value:time"
        test_param = postgres.PostgresParameter(parameter_type='independent', parameter_str=param_str)
        test_param_vals = {'test_time': datetime.datetime(2009, 1, 6, 1, 1, 1)}
        expected_val = "'01:01:01'"
        self.assertEquals(expected_val, test_param.query_value(independent_params=test_param_vals))


class TemplateTests(unittest.TestCase):

    def test_simple_query(self):

        query = """
        SELECT *
        FROM Album
        WHERE AlbumId = {% album_id|value:int %}
        """

        query_template = postgres.QueryTemplate(template_str=query,
                                                db_connector=connectors.sqlite_chinook)

        df = query_template.execute(album_id=4)

        self.assertEquals(df['Title'].unique()[0], 'Let There Be Rock')


