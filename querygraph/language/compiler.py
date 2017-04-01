import pyparsing as pp

from querygraph.query_node import QueryNode
from querygraph.manipulation.expression.evaluator import Evaluator
from querygraph.manipulation.set import ManipulationSet, Mutate, Rename
from querygraph.manipulation import common_parsers
from querygraph.db import interfaces
from querygraph.exceptions import QGLSyntaxError


class ConnectBlock(object):
    """
    Compiler for the CONNECT block of a GQL query. The body of the
    CONNECT block takes the form:

        <connector_name> <- <connector_type>(<**kwargs>)
        ...

    Data for each connector is stored in the 'connector' dict. Actual
    DbConnector instances are created by the QGLCompiler class.

    """

    def __init__(self):
        self.connectors = dict()

    def _add_connector(self, conn_name, conn_type, conn_kwargs):
        self.connectors[conn_name] = {'conn_type': conn_type, 'conn_kwargs': conn_kwargs}

    def parser(self):
        connector_name = pp.Word(pp.alphas, pp.alphanums + "_$")

        connector_type = pp.Word(pp.alphas)
        connector_kwarg = (pp.Word(pp.alphas, pp.alphanums + "_$") + pp.Suppress("=") + pp.QuotedString(quoteChar="'"))
        connector_kwarg.setParseAction(lambda x: {x[0]: x[1]})

        conn_kwarg_list = pp.delimitedList(connector_kwarg)
        conn_kwarg_list.setParseAction(lambda x: dict(pair for d in x for pair in d.items()))

        single_connector = (connector_name + pp.Suppress("<-") + connector_type +
                            pp.Suppress("(") + conn_kwarg_list + pp.Suppress(")"))
        single_connector.setParseAction(lambda x: self._add_connector(conn_name=x[0],
                                                                      conn_type=x[1],
                                                                      conn_kwargs=x[2]))

        connector_block = pp.OneOrMore(single_connector)
        return connector_block


class RetrieveBlock(object):

    """
    Compiler for the RETRIEVE block string. The body of the RETRIEVE block
    takes the form:

        QUERY |
            <query_template_string>;
        FIELDS <field_name_1>, <field_name_2>,...
        USING <connector_name>
        THEN |
            <manipulation_1_type>(<**kwargs>) >>
            ...
            <manipulation_n_type>(<**kwargs>);
        AS <node_name>
        ---
        ...
    where the 'FIELDS ..." block is the optional field select for NOSQL
    databases, and the 'THEN|...;' block is the optional manipulation set.
    Data for each query node are stored in dicts and created later by the
    QGLCompiler class.


    """

    def __init__(self):
        self.nodes = dict()

    def _add_query_node(self, query_value, connector_name, node_name, fields=None, manipulation_set=None):
        self.nodes[node_name] = {'query_value': query_value,
                                 'connector_name': connector_name,
                                 'fields': fields,
                                 'manipulation_set': manipulation_set}

    def parser(self):
        query_key = pp.Keyword("QUERY")
        query_value = pp.Suppress("|") + pp.SkipTo(pp.Suppress(";"), include=True)

        fields_key = pp.Keyword("FIELDS")
        field_name = common_parsers.column
        field_name_list = pp.Group(pp.delimitedList(field_name, delim=",")).setParseAction(lambda x: x.asList())

        fields_block = (pp.Suppress(fields_key) + field_name_list)

        connector_name = pp.Word(pp.alphas, pp.alphanums + "_$")
        using_block = pp.Suppress("USING") + connector_name

        then_key = pp.Suppress("THEN")
        manipulation_set = pp.Suppress("|") + pp.SkipTo(pp.Suppress(";"), include=True)
        then_block = then_key + manipulation_set

        as_key = pp.Suppress("AS")
        node_name = pp.Word(pp.alphas, pp.alphanums + "_$")
        as_block = as_key + node_name

        query_node_block = (pp.Suppress(query_key) + query_value + pp.Optional(fields_block, default=None) + using_block + pp.Optional(then_block, default=None) + as_block)
        query_node_block.setParseAction(lambda x: self._add_query_node(query_value=x[0],
                                                                       connector_name=x[2],
                                                                       node_name=x[4],
                                                                       fields=x[1],
                                                                       manipulation_set=x[3]))
        single_query_node = query_node_block + pp.Optional(pp.Suppress("---"))
        retrieve_block = pp.OneOrMore(single_query_node)
        return retrieve_block


class JoinBlock(object):

    """
    Compiler for the JOIN block string. The body of the JOIN block
    takes the form:

        <join_type> (<child_node_name>[<col_1>,...,<col_n>] ==> <parent_node_name>[<col_1>,...,<col_n>])
        ...

    Data for each join are stored in dicts and actual joins applied
    later by QGLCompiler class.

    """

    def __init__(self):
        self.joins = list()

    def __nonzero__(self):
        return len(self.joins) > 0

    def _add_join(self, join_type, child_node_name, child_cols, parent_node_name, parent_cols):
        self.joins.append({'join_type': join_type,
                           'child_node': child_node_name,
                           'child_cols': child_cols,
                           'parent_node': parent_node_name,
                           'parent_cols': parent_cols})

    def parser(self):
        join_type = (pp.Literal("LEFT") | pp.Literal("RIGHT") | pp.Literal("INNER") | pp.Literal("OUTER"))
        node_name = pp.Word(pp.alphas, pp.alphanums + "_$")

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
        join_block = pp.OneOrMore(single_join)
        return join_block


class QGLCompiler(object):

    db_interface_map = {'sqlite': interfaces.Sqlite,
                        'postgres': interfaces.Postgres,
                        'mysql': interfaces.MySql,
                        'mongodb': interfaces.MongoDb,
                        'elasticsearch': interfaces.ElasticSearch,
                        'mariadb': interfaces.MariaDb,
                        'cassandra': interfaces.Cassandra,
                        'influxdb': interfaces.InfluxDb,
                        'ms_sql': interfaces.MsSql}

    def __init__(self, qgl_str, query_graph):
        self.qgl_str = qgl_str
        self.query_graph = query_graph

        self.connect_block = ConnectBlock()
        self.retrieve_block = RetrieveBlock()
        self.join_block = JoinBlock()
        self.manipulation_set_str = None

        self.connectors = dict()

    def _add_manipulation_set(self, manipulation_set_str):
        self.manipulation_set_str = manipulation_set_str

    def compile(self):
        manipulation_set = pp.Optional(pp.Suppress(pp.Keyword("THEN")) +
                                       pp.Suppress("|") + pp.SkipTo(pp.Suppress(";"), include=True))
        manipulation_set.setParseAction(lambda x: self._add_manipulation_set(x[0]))

        parser = (pp.Keyword("CONNECT") + self.connect_block.parser() +
                  pp.Keyword("RETRIEVE") + self.retrieve_block.parser() +
                  pp.Optional(pp.Keyword("JOIN") + self.join_block.parser()))
        try:
            parser.parseString(self.qgl_str)
        except pp.ParseException, e:
            raise QGLSyntaxError("Couldn't parse query: \n %s" % e)
        self._create_connectors()
        self._create_query_nodes()
        if self.join_block:
            self._create_joins()
        if self.manipulation_set_str:
            self.query_graph.manipulation_set.append_from_str(self.manipulation_set_str)

    def _validate_conn_type(self, conn_type):
        if conn_type not in self.db_interface_map.keys():
            raise QGLSyntaxError("Error: '%s' is not a valid database connector type." % conn_type)

    def _create_connectors(self):
        for conn_name, conn_dict in self.connect_block.connectors.items():
            conn_type = conn_dict['conn_type'].lower()
            conn_kwargs = conn_dict['conn_kwargs']
            conn_kwargs['name'] = conn_name
            self._validate_conn_type(conn_type=conn_type)
            try:
                self.connectors[conn_name] = self.db_interface_map[conn_type](**conn_kwargs)
            except TypeError:
                raise QGLSyntaxError("Missing arguments for connector '%s' for database type '%s'." % (conn_name,
                                                                                                       conn_type))

    def _create_query_nodes(self):
        for node_name, node_dict in self.retrieve_block.nodes.items():
            self.query_graph.nodes[node_name] = QueryNode(name=node_name, query=node_dict['query_value'],
                                                          log=self.query_graph.log,
                                                          db_interface=self.connectors[node_dict['connector_name']],
                                                          fields=node_dict['fields'])
            if node_dict['manipulation_set'] is not None:
                self.query_graph.nodes[node_name].manipulation_set.append_from_str(node_dict['manipulation_set'])

    def _validate_join_nodes(self, parent_node, child_node):
        if parent_node not in self.query_graph.nodes:
            raise QGLSyntaxError("Error: can't join parent node '%s' because it does not exist." % parent_node)
        if child_node not in self.query_graph.nodes:
            raise QGLSyntaxError("Error: can't join child node '%s' because it does not exist." % child_node)

    def _create_joins(self):
        for join_dict in self.join_block.joins:
            parent_node_name = join_dict['parent_node']
            child_node_name = join_dict['child_node']
            self._validate_join_nodes(parent_node=parent_node_name, child_node=child_node_name)

            parent_cols = join_dict['parent_cols']
            child_cols = join_dict['child_cols']
            on_columns = list()
            for parent_col, child_col in zip(parent_cols, child_cols):
                on_columns.append({parent_node_name: parent_col, child_node_name: child_col})
            self.query_graph.join(child_node=self.query_graph.nodes[child_node_name],
                                  parent_node=self.query_graph.nodes[parent_node_name],
                                  join_type=join_dict['join_type'].lower(),
                                  on_columns=on_columns)