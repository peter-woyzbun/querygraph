from querygraph.query.node import QueryNode
from querygraph.graph.exceptions import GraphException, GraphConfigException


class QueryGraph(object):

    def __init__(self):
        self.nodes = dict()

    def add_node(self, query_node):
        """
        Add a QueryNode to the QueryGraph.

        Parameters
        ----------

        query_node : QueryNode
            The QueryNode instance to add to graph.

        """
        if not isinstance(query_node, QueryNode):
            raise GraphConfigException("When adding a graph node, the 'query_node' argument must be a "
                                       "QueryNode instance.")
        self.nodes[query_node.name] = query_node

    def inner_join(self, child_node, parent_node, *args):
        pass