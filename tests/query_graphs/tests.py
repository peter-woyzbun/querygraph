import unittest
import datetime

from querygraph.graph import QueryGraph, GraphConfigException, GraphException, CycleException
from querygraph.query_node import QueryNode, CycleException, AddColumnException
from querygraph.db.test_data import connectors


class ConfigurationExceptionTests(unittest.TestCase):

    test_node = QueryNode(name="test_node", query="", db_connector=connectors.daily_ts_connector)




def main():
    unittest.main()


if __name__ == '__main__':
    main()