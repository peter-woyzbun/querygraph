import unittest
import datetime


from querygraph.query.node import QueryNode
from querygraph.db.test_data import connectors
from querygraph.graph import QueryGraph


class ConfigurationExceptionTests(unittest.TestCase):

    test_node = QueryNode(name="test_node", query="", db_connector=connectors.daily_ts_connector)


class ExecutionTests(unittest.TestCase):

    def test_join(self):
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

        query_graph = QueryGraph()
        query_graph.add_node(mo_seasons_node)
        query_graph.add_node(daily_ts_node)

        query_graph.right_join(daily_ts_node, mo_seasons_node, on_columns=[{'mo_seasons': 'month', 'daily_ts': 'month_abr'}])

        df = query_graph.execute(seasons=['winter', 'spring'])
        self.assertEquals(7, len(df['month_name'].unique()))


def main():
    unittest.main()


if __name__ == '__main__':
    main()