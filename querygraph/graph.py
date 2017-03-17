import inspect

import yaml
from graphviz import Digraph

from querygraph.exceptions import QueryGraphException
from querygraph.language.compiler import QGLCompiler
from querygraph.query_node import QueryNode
from querygraph.db import connectors


# =================================================
# Exceptions
# -------------------------------------------------

class GraphException(QueryGraphException):
    pass


class GraphConfigException(GraphException):
    pass


class CycleException(GraphConfigException):
    pass


class DisconnectedNodes(GraphException):
    pass


# =================================================
# Query Graph Class
# -------------------------------------------------

class QueryGraph(object):

    """
    QueryGraph class containing core logic for creating and executing
    query graphs.


    """

    def __init__(self, qgl_str=None):
        # Dictionary that maps node names to their instances.
        self.nodes = dict()
        if qgl_str is not None:
            compiler = QGLCompiler(qgl_str=qgl_str, query_graph=self)
            compiler.compile()

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

    def __iter__(self):
        for node in self.root_node:
            yield node

    @property
    def is_spanning_tree(self):
        """ Check if the graph is a spanning tree - i.e. all nodes connected. """
        return self.num_edges == self.num_nodes - 1

    @property
    def num_nodes(self):
        return len(self.nodes.keys())

    @property
    def num_edges(self):
        num_edges = 0
        for node in self:
            num_edges += node.num_edges
        return num_edges

    @property
    def root_node(self):
        """ Return the root QueryNode instance - the node with no parent."""
        arbitrary_node = self.nodes.values()[0]
        return arbitrary_node.root_node()

    def __contains__(self, item):
        """ Check if graph contains given QueryNode instance. """
        if not isinstance(item, QueryNode):
            raise GraphException("Can only check if graph __contains__ a QueryNode instance.")
        return item in self.nodes.values()

    def join(self, child_node, parent_node, join_type, on_columns):
        """
        Join a child QueryNode to a parent QueryNode. The actual 'edge' data is handled
        by the QueryNode class - the child node is assigned the parent node, and the
        child node is added to the parent node's list of 'children' nodes. The data
        pertaining to how the child node's dataframe is to be joined with the parent
        node's dataframe is stored in the child node's 'join context'.

        Parameters
        ----------
        child_node : QueryNode
            The node to be joined with the parent node after its query is executed
            and resulting dataframe obtained.
        parent_node : QueryNode
            Node child will be joined with.
        join_type : str {'left' or 'right' or 'inner' or 'outer'}
            The type of join to use.
        on_columns : list of dictionaries
            A list of dictionaries, each dictionary containing two key-value pairs:
            (1) a key mapping the parent node's name to a single column to join on,
            and (2) a key mapping the child node's name to a single column to be
            joined with the given parent column. E.g:

                {'parent_node_name': 'col_x', 'child_node_name': 'col_y'}

        """
        if not isinstance(child_node, QueryNode):
            raise GraphConfigException("Can't join an instance that is not a QueryNode - %s." % child_node)
        if not isinstance(parent_node, QueryNode):
            raise GraphConfigException("Can't join an instance that is not a QueryNode.")
        # Check for bad join conditions.
        self._join_checks(child_node, parent_node)

        parent_node.children.append(child_node)
        child_node.parent = parent_node
        child_node.join_context.join_type = join_type
        child_node.join_context.parent_node_name = parent_node.name
        for pair_dict in on_columns:
            child_node.join_context.add_on_column_pair(parent_col_name=pair_dict[parent_node.name],
                                                       child_col_name=pair_dict[child_node.name])

    def inner_join(self, child_node, parent_node, on_columns):
        self.join(child_node=child_node, parent_node=parent_node, join_type='inner', on_columns=on_columns)

    def outer_join(self, child_node, parent_node, on_columns):
        self.join(child_node=child_node, parent_node=parent_node, join_type='outer', on_columns=on_columns)

    def left_join(self, child_node, parent_node, on_columns):
        self.join(child_node=child_node, parent_node=parent_node, join_type='left', on_columns=on_columns)

    def right_join(self, child_node, parent_node, on_columns):
        self.join(child_node=child_node, parent_node=parent_node, join_type='right', on_columns=on_columns)

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

    def _parallel_execute(self, independent_param_vals):
        """
        Execution the QueryGraph in 'parallel'. For each node generation
        contained in the graph, this requires:

            ...

        Parameters
        ----------
        independent_param_vals : dict
            Dictionary mapping independent parameter names to values - for
            use in template rendering.

        """
        threads = list()
        root_thread = self.root_node.execution_thread(threads=threads,
                                                      **independent_param_vals)
        root_thread.start()
        for thread in threads:
            thread.join()

    def parallel_execute(self, **independent_param_vals):
        self._parallel_execute(independent_param_vals=independent_param_vals)
        self.root_node.fold_children()
        return self.root_node.df

    def node_generations(self):
        """
        Iterable that returns lists containing each 'generation' of
        nodes contained in the graph. In the example graph below,
        the first generation contains the node labeled '1', the second
        generation contains both nodes labeled '2', and the third
        generation contains only the node labeled '3'.

                      +---+
                  +---+ 1 +---+
                  |   +---+   |
                  |           |
                +-v-+       +-v-+
                | 2 |       | 2 |
                +-+-+       +---+
                  |
                +-v-+
                | 3 |
                +---+

        Each generation is yielded as a list containing the node
        instances.

        """
        parent_generation = [self.root_node]
        while len(parent_generation) > 0:
            yield parent_generation
            child_generation = list()
            for parent_node in parent_generation:
                for child_node in parent_node.children:
                    child_generation.append(child_node)
            parent_generation = child_generation

    def execute(self, **independent_param_vals):
        if not self.is_spanning_tree:
            raise DisconnectedNodes("Cannot execute: QueryGraph is not a minimum spanning "
                                    "tree - there are disconnected nodes.")
        root_node = self.root_node
        root_node.execute(**independent_param_vals)
        return root_node.df

    def render_viz(self, save_path):
        dot = Digraph(comment='Query Graph Visualization')
        for node in self:
            dot.node(node.name)
        for node in self:
            for child_node in node.children:
                dot.edge(node.name, child_node.name)
                dot.edge(child_node.name, node.name, label='%s' % str(child_node.join_context), fontsize='6',
                         style='dashed', minlen='4')
        dot.render(save_path)

    def render_parallel_viz(self, save_path):
        sub_graphs = list()
        for generation_num, generation in enumerate(self.node_generations()):
            generation_subgraph = Digraph('Generation %s' % generation_num)
            generation_subgraph.body.append('style=filled')
            generation_subgraph.body.append('color=lightgrey')
            generation_subgraph.body.append('label = "process #1"')
            for node in generation:
                if node.parent is not None:
                    generation_subgraph.edge(tail_name=node.name, head_name=node.parent.name)
            sub_graphs.append(generation_subgraph)
        g = Digraph('Query Graph')
        for sub_graph in sub_graphs:
            g.subgraph(sub_graph)
        for node in self:
            for child_node in node.children:
                g.edge(node.name, child_node.name)
                g.edge(child_node.name, node.name, label='%s' % str(child_node.join_context), fontsize='6',
                         style='dashed', minlen='4')
        g.render(save_path)