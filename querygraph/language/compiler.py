import re

import pyparsing as pp

from querygraph.db.connectors import SQLite, MySQL, Postgres, MongoDb
from querygraph.query.node import QueryNode


class ConnectBlock(object):
    """
    Compiler for the CONNECT block of a GQL query. The body of the
    CONNECT block takes the form:

        <connector_name> <- <connector_type>(<**kwargs>)
        ...

    Each connector will be created and stored in the db_connectors
    dict.

    """

    def __init__(self):
        self.db_connectors = dict()

    def compile(self, connect_block_str):
        parser = pp.OneOrMore(self._connector())
        parser.parseString(connect_block_str)

    @property
    def connector_names(self):
        return self.db_connectors.keys()

    def _create_connector(self, conn_name, conn_type, conn_kwargs):
        print "CREATED CONNECTOR!"

        connector_classes = {'MySql': MySQL,
                             'Sqlite': SQLite,
                             'Postgres': Postgres,
                             'MongoDb': MongoDb}

        self.db_connectors[conn_name] = connector_classes[conn_type](**conn_kwargs)

    def _connector(self):
        connector_name = pp.Word(pp.alphas, pp.alphanums + "_$")

        connector_type = pp.Word(pp.alphas)
        connector_kwarg = (pp.Word(pp.alphas, pp.alphanums + "_$") + pp.Suppress("=") + pp.QuotedString(quoteChar="'"))
        connector_kwarg.setParseAction(lambda x: {x[0]: x[1]})

        conn_kwarg_list = pp.delimitedList(connector_kwarg)
        conn_kwarg_list.setParseAction(lambda x: dict(pair for d in x for pair in d.items()))

        single_connector = (connector_name + pp.Suppress("<-") + connector_type +
                            pp.Suppress("(") + conn_kwarg_list + pp.Suppress(")"))
        single_connector.setParseAction(lambda x: self._create_connector(conn_name=x[0],
                                                                         conn_type=x[1],
                                                                         conn_kwargs=x[2]))
        return single_connector


class RetrieveBlock(object):
    """
    Compiler for the RETRIEVE block string. The body of the RETRIEVE block
    takes the form:

        QUERY |
            <query_template_string>;
        USING <connector_name>
        THEN |
            <manipulation_1_type>(<**kwargs>) >>
            ...
            <manipulation_n_type>(<**kwargs>);
        AS <node_name>
        ---
        ...

    where the 'THEN|...;' block is the optional manipulation set.


    """

    def __init__(self, connect_block, query_graph):
        if not isinstance(connect_block, ConnectBlock):
            raise Exception
        self.connect_block = connect_block
        self.query_graph = query_graph
        self.nodes = dict()

    def compile(self, retrieve_block_str):
        parser = pp.delimitedList(self._query_node(), delim="---")
        parser.parseString(retrieve_block_str)

    def _create_query_node(self, query_value, connector_name, node_name, manipulation_set=None):
        db_connector = self.connect_block.db_connectors[connector_name]
        self.nodes[node_name] = QueryNode(name=node_name, query=query_value, db_connector=db_connector)
        self.query_graph.add_node(self.nodes[node_name])

    def _query_node(self):
        query_key = pp.Suppress("QUERY")
        query_value = pp.Suppress("|") + pp.SkipTo(pp.Suppress(";"), include=True)

        connector_name = pp.oneOf(" ".join(self.connect_block.connector_names))

        using_block = pp.Suppress("USING") + connector_name

        then_key = pp.Suppress("THEN")

        manipulation_set = pp.Suppress("|") + pp.SkipTo(pp.Suppress(";"), include=True)
        then_block = then_key + manipulation_set

        as_key = pp.Suppress("AS")
        node_name = pp.Word(pp.alphas, pp.alphanums + "_$")
        as_block = as_key + node_name

        query_node_block = (query_key + query_value + using_block + pp.Optional(then_block, default=None) + as_block)
        query_node_block.setParseAction(lambda x: self._create_query_node(query_value=x[0],
                                                                          connector_name=x[1],
                                                                          node_name=x[3],
                                                                          manipulation_set=x[2]))
        return query_node_block


class JoinBlock(object):
    """
    Compiler for the JOIN block string. The body of the JOIN block
    takes the form:

        <join_type> (<child_node_name>[<col_1>,...,<col_n>] ==> <parent_node_name>[<col_1>,...,<col_n>]);
        ...

    """

    def __init__(self, retrieve_block, query_graph):
        if not isinstance(retrieve_block, RetrieveBlock):
            raise Exception
        self.retrieve_block = retrieve_block
        self.query_graph = query_graph

    def compile(self, join_block_str):
        join_block = pp.delimitedList(self._node_join(), delim=";")
        join_block.parseString(join_block_str)

    def _add_join(self, join_type, child_node_name, child_cols, parent_node_name, parent_cols):
        child_node = self.query_graph.nodes[child_node_name]
        parent_node = self.query_graph.nodes[parent_node_name]
        on_columns = list()
        for child_col, parent_col in zip(child_cols, parent_cols):
            on_columns.append({parent_node_name: parent_col, child_node_name: child_col})
        self.query_graph.join(child_node=child_node,
                              parent_node=parent_node,
                              on_columns=on_columns,
                              join_type=join_type.lower())

    def _node_join(self):
        join_type = (pp.Literal("LEFT") | pp.Literal("RIGHT") | pp.Literal("INNER") | pp.Literal("OUTER"))

        node_names = self.retrieve_block.nodes.keys()
        print node_names
        node_name = pp.oneOf(" ".join(node_names))

        col_name = pp.Word(pp.alphas, pp.alphanums + "_$")
        col_name_list = pp.Group(pp.delimitedList(col_name, delim=","))

        l_brac = pp.Suppress("[")
        r_brac = pp.Suppress("]")

        single_join = (join_type + pp.Suppress("(") + node_name + l_brac +
                       col_name_list + r_brac + pp.Suppress("==>") + node_name +
                       l_brac + col_name_list + r_brac + pp.Suppress(")"))

        single_join.addParseAction(lambda x: self._add_join(join_type=x[0],
                                                            child_node_name=x[1],
                                                            child_cols=x[2],
                                                            parent_node_name=x[3],
                                                            parent_cols=x[4]))
        return single_join


class QGLCompiler(object):

    def __init__(self, qgl_str, query_graph):
        self.qgl_str = qgl_str
        self.query_graph = query_graph
        self.connect_block = ConnectBlock()
        self.retrieve_block = RetrieveBlock(connect_block=self.connect_block, query_graph=self.query_graph)
        self.join_block = JoinBlock(retrieve_block=self.retrieve_block, query_graph=self.query_graph)

    def compile(self):
        blocks = re.split("\s*(CONNECT|RETRIEVE|JOIN)\s*[\n]", self.qgl_str)
        blocks = [block_str for block_str in blocks if block_str not in ('CONNECT', 'RETRIEVE', 'JOIN', '')]

        connect_block_str = blocks[0]
        self.connect_block.compile(connect_block_str=connect_block_str)

        retrieve_block_str = blocks[1]
        self.retrieve_block.compile(retrieve_block_str=retrieve_block_str)

        join_block_str = blocks[2]
        self.join_block.compile(join_block_str=join_block_str)
        return self.query_graph



test_query = """CONNECT
    sqlite_conn <- Sqlite(host='fun host!')
    sqlite_conn_2 <- Sqlite(host='fun host!')
RETRIEVE
	QUERY |
		SELECT *
        FROM month_seasons
        WHERE season IN {% seasons|value_list:str %};
	USING sqlite_conn
	AS NODE_X
	---
	QUERY |
		{"tags" : {"$in" : {{ seasons_tags|value_list:str }} }};
	USING sqlite_conn_2
	AS NODE_Y
JOIN
	LEFT (NODE_Y[col_1, col_2] ==> NODE_X[col_1, col_2]);
"""


# query_parser = QGLCompiler(qgl_str=test_query)
# query_graph = query_parser.compile()

# print query_graph.nodes