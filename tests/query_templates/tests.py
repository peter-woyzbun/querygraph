import unittest
import datetime

import pandas as pd

from querygraph.db.test_data.connectors import daily_ts_connector, hourly_ts_connector
from querygraph.query_templates.query_template import QueryTemplate


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

