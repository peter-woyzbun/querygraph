import unittest

from querygraph.query_node import QueryNode, CycleException, AddColumnException
from querygraph.db.test_data.connectors import daily_ts_connector, hourly_ts_connector


class ConfigurationExceptionTests(unittest.TestCase):

    def test_cycle_exception(self):
        test_node = QueryNode(name="test_node", query="", db_connector=daily_ts_connector)
        self.assertRaises(CycleException, test_node.join, child_node=test_node, on_columns={'col': 'col'}, how='left')

    def test_multi_col_add_exception(self):
        test_node = QueryNode(name="test_node", query="", db_connector=daily_ts_connector)
        self.assertRaises(AddColumnException, test_node.add_column, col_1="expr", col_2="expr")

def main():
    unittest.main()

if __name__ == '__main__':
    main()