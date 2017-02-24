import yaml

from querygraph.exceptions import QueryGraphException
from querygraph.db.connectors import SQLite, MySQL, MsSQL, Postgres
from querygraph.query_node import QueryNode


# =============================================
# Exceptions
# ---------------------------------------------

class YamlFormatException(QueryGraphException):
    pass


class DatabaseConfigException(YamlFormatException):
    pass


# =============================================
# YAML Reader
# ---------------------------------------------

class ReadYaml(object):

    db_connector_map = {'sqlite': SQLite,
                        'mysql': MySQL,
                        'mssql': MsSQL,
                        'postgres': Postgres}

    def __init__(self, yaml_str=None, yaml_path=None):
        self.yaml_str = yaml_str
        self.yaml_path = yaml_path
        self.query_nodes = dict()
        self.db_connectors = dict()
        self.root_node = None

    def query_graph(self):
        """ Create a Query Graph from YAML data. """
        if self.yaml_path:
            graph_data = yaml.load(file('%s' % self.yaml_path, 'r'))
        else:
            graph_data = yaml.load(self.yaml_str)
        self._create_db_connectors(graph_data)
        self._create_query_nodes(graph_data)
        self._join_query_nodes(graph_data)
        if self.root_node is None:
            raise YamlFormatException("No root node defined - there should be a single node with no parent.")
        else:
            return self.root_node

    def _create_db_connectors(self, graph_data):
        if 'DATABASES' not in graph_data:
            raise YamlFormatException("No databases defined.")
        databases = graph_data['DATABASES']
        for connector_name, connector_attribs in databases.items():
            if 'type' not in connector_attribs:
                raise DatabaseConfigException("No database type defined for '%s'" % connector_name)
            db_type = connector_attribs.pop('type')
            if db_type not in self.db_connector_map:
                raise DatabaseConfigException("The given database type, '%s', for '%s' is not "
                                              "supported." % (connector_name, connector_name))
            self.db_connectors[connector_name] = self.db_connector_map[db_type](**connector_attribs)

    def _create_query_nodes(self, graph_data):
        if 'QUERY_NODES' not in graph_data:
            raise YamlFormatException("No query nodes defined.")
        query_nodes = graph_data['QUERY_NODES']
        for node_name, node_attribs in query_nodes.items():
            self.query_nodes[node_name] = QueryNode(name=node_name,
                                                    query=node_attribs['query'],
                                                    db_connector=self.db_connectors[node_attribs['database']])
            if 'create' in node_attribs:
                query_node = self.query_nodes[node_name]
                for new_col_name, new_col_def in node_attribs['create'].items():
                    kwarg = {new_col_name: new_col_def}
                    query_node.add_column(**kwarg)

    def _join_query_nodes(self, graph_data):
        query_nodes = graph_data['QUERY_NODES']
        for node_name, node_attribs in query_nodes.items():
            if 'parent_node' not in node_attribs:
                if self.root_node is not None:
                    raise YamlFormatException("More than one query node do not have a parent node.")
                else:
                    self.root_node = self.query_nodes[node_name]
            else:
                parent_node = self.query_nodes[node_attribs['parent_node']]
                child_node = self.query_nodes[node_name]
                parent_node.join(child_node, how=node_attribs['join_context']['how'],
                                 on_columns=node_attribs['join_context']['on_columns'])


