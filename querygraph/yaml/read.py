import yaml

from querygraph.exceptions import QueryGraphException
from querygraph.db.connectors import SQLite
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

    db_connector_map = {'sqlite': SQLite}

    def __init__(self, yaml_str=None, yaml_path=None):
        self.yaml_str = yaml_str
        self.yaml_path = yaml_path
        self.query_nodes = dict()
        self.db_connectors = dict()

    def query_graph(self):
        if self.yaml_path:
            graph_data = yaml.load(file('%s' % self.yaml_path, 'r'))
        else:
            graph_data = yaml.load(self.yaml_str)
        self._create_db_connectors(graph_data)
        self._create_query_nodes(graph_data)
        self._join_query_nodes(graph_data)

    def _create_db_connectors(self, graph_data):
        if 'DATABASES' not in graph_data:
            raise YamlFormatException("No databases defined.")
        databases = graph_data['DATABASES']
        for connector_name, connector_attribs in databases.items():
            if 'type' not in connector_attribs:
                raise DatabaseConfigException("No database type defined for '%s'" % connector_name)
            db_type = connector_attribs.pop('type')
            self.db_connectors[connector_name] = self.db_connector_map[db_type](**connector_attribs)

    def _create_query_nodes(self, graph_data):
        if 'QUERY_NODES' not in graph_data:
            raise YamlFormatException("No query nodes defined.")
        query_nodes = graph_data['QUERY_NODES']
        for node_name, node_attribs in query_nodes.items():
            self.query_nodes[node_name] = QueryNode(name=node_name,
                                                    query=node_attribs['query'],
                                                    db_connector=self.db_connectors[node_attribs['database']])
            if 'add_columns' in node_attribs:
                query_node = self.query_nodes[node_name]
                for new_col_name, new_col_def in node_attribs['add_columns'].items():
                    query_node.add_column(new_col_name=new_col_def)

    def _join_query_nodes(self, graph_data):
        query_nodes = graph_data['QUERY_NODES']
        for node_name, node_attribs in query_nodes.items():
            parent_node = self.query_nodes[node_attribs['parent_node']]
            child_node = self.query_nodes[node_name]
            parent_node.join(child_node, how=node_attribs['join_context']['how'],
                             on_columns=node_attribs['join_context']['on_columns'])


