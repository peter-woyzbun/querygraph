from querygraph.query.node import QueryNode
from querygraph.graph.exceptions import GraphException, GraphConfigException, CycleException


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

    @property
    def root_node(self):
        arbitrary_node = self.nodes.values()[0]
        return arbitrary_node.root_node()

    def __contains__(self, item):
        if not isinstance(item, QueryNode):
            raise GraphException("Can only check if graph __contains__ a QueryNode instance.")
        return item in self.nodes.values()

    def _join(self, child_node, parent_node, join_type, on_columns):
        if not isinstance(child_node, QueryNode):
            raise GraphConfigException("Can't join an instance that is not a QueryNode - %s." % child_node)
        if not isinstance(parent_node, QueryNode):
            raise GraphConfigException("Can't join an instance that is not a QueryNode.")
        # Check for bad join conditions.
        self._join_checks(child_node, parent_node)

        parent_node.children.append(child_node)
        child_node.parent = parent_node
        child_node.join_context.join_type = join_type
        for pair_dict in on_columns:
            child_node.join_context.add_on_column_pair(parent_col_name=pair_dict[parent_node.name],
                                                       child_col_name=pair_dict[child_node.name])

    def inner_join(self, child_node, parent_node, *on_columns):
        self._join(child_node, parent_node, join_type='inner', *on_columns)

    def outer_join(self, child_node, parent_node, *on_columns):
        self._join(child_node=child_node, parent_node=parent_node, join_type='outer', *on_columns)

    def left_join(self, child_node, parent_node, on_columns):
        self._join(child_node, parent_node, 'left', on_columns)

    def right_join(self, child_node, parent_node, *on_columns):
        self._join(child_node=child_node, parent_node=parent_node, join_type='right', *on_columns)

    def _join_checks(self, child_node, parent_node):
        if child_node not in self:
            raise GraphConfigException("The child node attempting to be joined, '%s',"
                                       " is not a node in this graph." % child_node.name)
        if parent_node not in self:
            raise GraphConfigException("The parent node attempting to be joined,"
                                       " '%s', is not a node in this graph." % parent_node.name)
        if child_node.parent is not None:
            raise GraphConfigException("The child node attempting to be joined,"
                                       " '%s', already has a parent node." % child_node.name)
        if parent_node in child_node:
            raise CycleException("Joining parent node '%s' with child node '%s' would"
                                 " create a cycle in the graph." % (parent_node.name, child_node.name))

    def execute(self, **independent_param_vals):
        root_node = self.root_node
        root_node.execute(**independent_param_vals)
        return root_node.df

