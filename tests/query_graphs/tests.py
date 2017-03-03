import unittest
import datetime


from querygraph.query.node import QueryNode
from querygraph.db.test_data import connectors
from querygraph.graph import QueryGraph, DisconnectedNodes
from querygraph.manipulation.set import Create


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
        daily_ts_node.manipulation_set += Create(new_col_name='month_abr',
                                                 new_col_expression="str.lowercase(str.slice(month_name, s=0, e=4))")

        query_graph = QueryGraph()
        query_graph.add_node(mo_seasons_node)
        query_graph.add_node(daily_ts_node)

        query_graph.right_join(daily_ts_node, mo_seasons_node,
                               on_columns=[daily_ts_node['month_abr'] >> mo_seasons_node['month']])
        # query_graph.right_join(daily_ts_node, mo_seasons_node, on_columns=[{'mo_seasons': 'month', 'daily_ts': 'month_abr'}])

        # query_graph.render_viz(save_path='query_graph_test')

        df = query_graph.execute(seasons=['winter', 'spring'])
        self.assertEquals(7, len(df['month_name'].unique()))


class MalformedGraphTests(unittest.TestCase):

    def test_disconnected_nodes(self):
        node_1 = QueryNode(query='', db_connector=connectors.daily_ts_connector, name='node_1')
        node_2 = QueryNode(query='', db_connector=connectors.daily_ts_connector, name='node_2')

        query_graph = QueryGraph()
        query_graph.add_node(node_1)
        query_graph.add_node(node_2)
        self.assertRaises(DisconnectedNodes, query_graph.execute)


class ParallelExecutionTests(unittest.TestCase):

    def test_generations(self):
        node_1 = QueryNode(query='', db_connector=connectors.daily_ts_connector, name='node_1')
        node_2 = QueryNode(query='', db_connector=connectors.daily_ts_connector, name='node_2')
        node_3 = QueryNode(query='', db_connector=connectors.daily_ts_connector, name='node_3')

        query_graph = QueryGraph()
        query_graph.add_node(node_1)
        query_graph.add_node(node_2)
        query_graph.add_node(node_3)

        query_graph.left_join(child_node=node_2, parent_node=node_1, on_columns=[])
        query_graph.left_join(child_node=node_3, parent_node=node_1, on_columns=[])

        graph_generations = list(query_graph.node_generations())
        self.assertEquals(set(graph_generations[1]), {node_2, node_3})


def main():
    unittest.main()


if __name__ == '__main__':
    main()