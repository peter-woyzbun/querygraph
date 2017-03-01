import unittest
import datetime

from querygraph.query_node import QueryNode, CycleException, AddColumnException
from querygraph.db.test_data import connectors


class ConfigurationExceptionTests(unittest.TestCase):

    def test_cycle_exception(self):
        test_node = QueryNode(name="test_node", query="", db_connector=connectors.daily_ts_connector)
        self.assertRaises(CycleException, test_node.join, child_node=test_node, on_columns={'col': 'col'}, how='left')

    def test_multi_col_add_exception(self):
        test_node = QueryNode(name="test_node", query="", db_connector=connectors.daily_ts_connector)
        self.assertRaises(AddColumnException, test_node.add_column, col_1="expr", col_2="expr")


class ExecutionTests(unittest.TestCase):

    def test_daily_ts_join(self):

        mo_seasons_query = """
        SELECT *
        FROM month_seasons
        WHERE season IN {% seasons|value_list:str %}
        """

        mo_seasons_node = QueryNode(query=mo_seasons_query,
                                    db_connector=connectors.mo_seasons_connector,
                                    name='mo_seasons')

        daily_ts_query = """
        SELECT *
        FROM daily_ts
        WHERE month_name IN {{ [datetime.to_str(str.to_datetime(str.capitalize(month), format='%b'), format='%B')]|value_list:str }}
        """

        daily_ts_node = QueryNode(query=daily_ts_query, db_connector=connectors.daily_ts_connector, name='daily_ts')
        daily_ts_node.add_column(month_abr="str.lowercase(str.slice(month_name, s=0, e=4))")
        mo_seasons_node.join(child_node=daily_ts_node, on_columns={'month': 'month_abr'}, how='right')

        test_graph = mo_seasons_node
        test_graph.execute(seasons=['winter', 'spring'])
        df = test_graph.df
        # There should be 7 unique month names.
        self.assertEquals(7, len(df['month_name'].unique()))

    def test_datetime_delta_execute(self):
        daily_ts_query = """
        SELECT *
        FROM daily_ts
        WHERE date(date_a) IN {% [datetime.add_delta(dates, days=20)]|value_list:date %}
        """
        daily_ts_node = QueryNode(query=daily_ts_query, db_connector=connectors.daily_ts_connector, name='daily_ts')
        daily_ts_node.execute(dates=[datetime.datetime(2016, 1, 15), datetime.datetime(2016, 1, 16)])
        df = daily_ts_node.df
        self.assertEquals(['February'], df['month_name'].unique())


class JoinOnTests(unittest.TestCase):

    def test_on_columns(self):
        parent_node = QueryNode(query='', db_connector=connectors.daily_ts_connector, name='parent_node')
        child_node = QueryNode(query='', db_connector=connectors.daily_ts_connector, name='child_node')

        print parent_node['parent_col'] >> child_node['child_col']




def main():
    unittest.main()


if __name__ == '__main__':
    main()