
from querygraph import exceptions
from querygraph.language.compiler import QGLCompiler
from querygraph.query_node import QueryNode
from querygraph.log import ExecutionLog


# =================================================
# Query Graph Class
# -------------------------------------------------

class QueryGraph(object):

    """
    QueryGraph class containing core logic for creating and executing
    query graphs.

    Responsibilities:

        (1) Mapping node names to their instances.
        (2) Joining nodes.
        (3) Execution..




    """

    def __init__(self, qgl_str=None):
        # Dictionary that maps node names to their instances.
        self.nodes = dict()
        self.num_edges = 0
        self.log = ExecutionLog(stdout_print=True)
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
            raise exceptions.GraphConfigException("When adding a graph node, the 'query_node' argument must be a "
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
    def root_node(self):
        """ Return the root QueryNode instance - the node with no parent."""
        # if not self.is_spanning_tree:
        #     raise exceptions.DisconnectedNodes("Can't return the root node because not all nodes are connected.")
        arbitrary_node = self.nodes.values()[0]
        return arbitrary_node.root_node()

    def __contains__(self, item):
        """ Check if graph contains given QueryNode instance. """
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
            raise exceptions.GraphConfigException("Can't join an instance that is not a QueryNode - %s." % child_node)
        if not isinstance(parent_node, QueryNode):
            raise exceptions.GraphConfigException("Can't join an instance that is not a QueryNode.")
        # Check for bad join conditions.
        self._join_checks(child_node, parent_node)

        parent_node.children.append(child_node)
        child_node.parent = parent_node
        child_node.join_context.join_type = join_type
        child_node.join_context.parent_node_name = parent_node.name
        for pair_dict in on_columns:
            child_node.join_context.add_on_column_pair(parent_col_name=pair_dict[parent_node.name],
                                                       child_col_name=pair_dict[child_node.name])
        self.num_edges += 1

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
            raise exceptions.GraphConfigException("The child node attempting to be joined, '%s',"
                                                  " is not a node in this graph." % child_node.name)
        if parent_node not in self:
            raise exceptions.GraphConfigException("The parent node attempting to be joined,"
                                                  " '%s', is not a node in this graph." % parent_node.name)
        if child_node.parent is not None:
            raise exceptions.GraphConfigException("The child node attempting to be joined,"
                                                  " '%s', already has a parent node." % child_node.name)
        if parent_node in child_node:
            raise exceptions.CycleException("Joining parent node '%s' with child node '%s' would"
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
        root_thread = self.root_node.root_execution_thread(threads=threads,
                                                           independent_param_vals=independent_param_vals)
        root_thread.start()
        threads.append(root_thread)
        while len(threads) < self.num_nodes:
            # Wait until all threads have been created.
            pass
        for thread in threads:
            thread.join()
        self.root_node.fold_children()

    def _pre_execution_checks(self):
        if not self.is_spanning_tree:
            self.log.graph_error(msg="Can't execute graph because there are disconnected nodes.")
            raise exceptions.DisconnectedNodes("Can't execute graph because there are disconnected nodes.")

    def _execute(self, independent_param_vals):
        for query_node in self:
            query_node.retrieve_dataframe(independent_param_vals=independent_param_vals)
        self.root_node.fold_children()

    def execute(self, **independent_param_vals):
        self.log.graph_info(msg="Starting execution on query graph with %s nodes." % self.num_nodes)
        self._parallel_execute(independent_param_vals)
        return self.root_node.df



