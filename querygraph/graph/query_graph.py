import inspect

import yaml
from graphviz import Digraph

from querygraph.query.node import QueryNode
from querygraph.graph.exceptions import GraphException, GraphConfigException, CycleException
from querygraph.db.connectors import SQLite, MySQL, Postgres
from querygraph.manipulation.set import Create


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

    def __iter__(self):
        for node in self.root_node:
            yield node

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
        child_node.join_context.parent_node_name = parent_node.name
        for pair_dict in on_columns:
            child_node.join_context.add_on_column_pair(parent_col_name=pair_dict[parent_node.name],
                                                       child_col_name=pair_dict[child_node.name])

    def inner_join(self, child_node, parent_node, *on_columns):
        self._join(child_node, parent_node, join_type='inner', *on_columns)

    def outer_join(self, child_node, parent_node, *on_columns):
        self._join(child_node=child_node, parent_node=parent_node, join_type='outer', *on_columns)

    def left_join(self, child_node, parent_node, on_columns):
        self._join(child_node, parent_node, 'left', on_columns)

    def right_join(self, child_node, parent_node, on_columns):
        self._join(child_node=child_node, parent_node=parent_node, join_type='right', on_columns=on_columns)

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


class MalformedYaml(GraphException):
    pass


class YamlQueryGraph(QueryGraph):

    DB_CONNECTORS = {'sqlite': SQLite,
                     'mysql': MySQL,
                     'postgres': Postgres}

    MANIPULATION_TYPES = {'CREATE': Create}

    def __init__(self, yaml_path=None, yaml_str=None):
        self.yaml_path = yaml_path
        self.yaml_str = yaml_str
        self.db_connectors = dict()
        QueryGraph.__init__(self)

    def _create_graph(self):
        if self.yaml_path:
            graph_data = yaml.load(file('%s' % self.yaml_path, 'r'))
        else:
            graph_data = yaml.load(self.yaml_str)
        self._key_check(data=graph_data, required_keys=('DATABASES', 'QUERY_NODES'), container_name='Query Graph')

    def _create_db_connectors(self, graph_data):
        connector_dict = graph_data['DATABASES']
        for conn_name, conn_dict in connector_dict.item():
            self._key_check(conn_dict, required_keys=('TYPE', ), container_name="'%s' connector" % conn_name)
            db_type = conn_dict.pop('TYPE')
            self.db_connectors[conn_name] = self._db_connector(conn_name, db_type, conn_dict)

    def _db_connector(self, conn_name, db_type, conn_dict):
        conn_class = self.DB_CONNECTORS[db_type]
        required_keys = [key.upper() for key in inspect.getargspec(conn_class.__init__)]
        self._key_check(data=conn_dict, required_keys=required_keys, container_name='%s connector' % conn_name)
        return conn_class(**conn_dict)

    def _create_nodes(self, graph_data):
        query_nodes_dict = graph_data['QUERY_NODES']
        for node_name, node_dict in query_nodes_dict.items():
            self._key_check(data=node_dict, required_keys=['DATABASE', 'QUERY'], container_name='%s node' % node_name)
            if node_dict['DATABASE'] not in self.db_connectors.keys():
                raise MalformedYaml("The database connector '%s' for query node"
                                    " '%s' is not defined." % (node_dict['DATABASE'], node_name))

    def _query_node(self, node_name, node_dict):
        db_connector_name = node_dict['DATABASE']
        query_node = QueryNode(name=node_name,
                               query=node_dict['QUERY'],
                               db_connector=self.db_connectors[db_connector_name])
        if 'MANIPULATION_SET' in node_dict:
            for manipulation_dict in node_dict['MANIPULATION_SET']:
                manipulation_type = manipulation_dict.keys()[0]
                if manipulation_type == 'CREATE':
                    col_list = manipulation_dict.values()[0]
                    for create_col in col_list:
                        query_node.manipulation_set += Create(new_col_name=create_col.keys()[0],
                                                              new_col_expression=create_col.values()[0])


    @staticmethod
    def _key_check(data, required_keys, container_name):
        for key in required_keys:
            if key not in data:
                raise MalformedYaml("'%s' missing in %s data." % (key, container_name))