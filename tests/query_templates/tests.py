import unittest
import datetime

import pandas as pd

from querygraph.db.test_data.connectors import daily_ts_connector, hourly_ts_connector
from querygraph.query.templates import QueryTemplate, IndependentParameterException


class DateTests(unittest.TestCase):

    def test_date_values_format_a(self):

        template_str = """
        SELECT *
        FROM daily_ts
        WHERE date(date_a) IN {% dates|value_list:date %}
        """

        query_template = QueryTemplate(query=template_str, db_connector=daily_ts_connector)

        df = query_template.execute(dates=[datetime.datetime(2016, 1, 1), datetime.datetime(2016, 1, 2)])
        expected_value = ['2016-01-01', '2016-01-02']

        self.assertEquals(expected_value, df['date_a'].tolist())

    def test_date_to_str(self):

        template_str = """
        SELECT *
        FROM daily_ts
        WHERE date_b IN {% [datetime.to_str(dates, format='%m/%d/%Y')]|value_list:str %}
        """

        query_template = QueryTemplate(query=template_str, db_connector=daily_ts_connector)
        df = query_template.execute(dates=[datetime.datetime(2016, 1, 1), datetime.datetime(2016, 1, 2)])
        expected_value = ['2016-01-01', '2016-01-02']

        self.assertEquals(expected_value, df['date_a'].tolist())


class ParameterCheckTests(unittest.TestCase):

    def test_dependent_parameter_check(self):

        template_str = """
        SELECT *
        FROM daily_ts
        WHERE date_b IN {{ dependent_parameter }}
        """

        query_template = QueryTemplate(query=template_str, db_connector=daily_ts_connector)

        self.assertTrue(query_template.has_dependent_parameters())


class MissingDataTests(unittest.TestCase):

    def test_no_independent_param_vals(self):

        template_str = """
        SELECT *
        FROM daily_ts
        WHERE date(date_a) IN {% dates|value_list:date %}
        """

        query_template = QueryTemplate(query=template_str, db_connector=daily_ts_connector)

        self.assertRaises(IndependentParameterException, query_template.execute)




def main():
    unittest.main()

if __name__ == '__main__':
    main()