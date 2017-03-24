import pyparsing as pp

from querygraph.db import connectors
from querygraph.query_node import QueryNode
from querygraph.manipulation.expression.evaluator import Evaluator
from querygraph.manipulation.set import ManipulationSet, Mutate, Rename


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


class ManipulationSetParser(object):

    manipulation_type_map = {'mutate': Mutate,
                             'rename': Rename}

    def __init__(self):
        self.manipulations = list()
        self.manipulation_set = ManipulationSet()

    def __call__(self, manipulation_set_str):
        parser = self.parser()
        parser.parseString(manipulation_set_str)
        return self.manipulation_set

    def _add_manipulation(self, manipulation_type, kwargs):
        self.manipulation_set += self.manipulation_type_map[manipulation_type](**kwargs)

    def _rename_parser(self):
        lpar = pp.Suppress("(")
        rpar = pp.Suppress(")")
        rename = pp.Suppress('rename')
        old_col_name = pp.Word(pp.alphas, pp.alphanums + "_$")
        new_col_name = pp.Word(pp.alphas, pp.alphanums + "_$")
        parser = rename + lpar + old_col_name + pp.Suppress("=") + new_col_name + rpar
        parser.setParseAction(lambda x: self._add_manipulation(manipulation_type='rename',
                                                               kwargs={'old_column_name': x[0],
                                                                       'new_column_name': x[1]}))
        return parser

    def _mutate_parser(self):
        lpar = pp.Suppress("(")
        rpar = pp.Suppress(")")
        mutate = pp.Suppress('mutate')
        col_name = pp.Word(pp.alphas, pp.alphanums + "_$")

        expr_evaluator = Evaluator(deferred_eval=True)
        col_expr = expr_evaluator.parser()

        parser = mutate + lpar + col_name + pp.Suppress("=") + col_expr + rpar
        parser.setParseAction(lambda x: self._add_manipulation(manipulation_type='mutate',
                                                               kwargs={'col_name': x[0], 'col_expr': x[1]}))
        return parser

    def parser(self):
        single_manipulation = (self._mutate_parser() | self._rename_parser())
        manipulation_set = pp.delimitedList(single_manipulation, delim='>>')
        return manipulation_set


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
        field_name = pp.Word(pp.alphas, pp.alphanums + "_$")
        field_name_list = pp.Group(pp.delimitedList(field_name, delim=","))

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

    connector_map = {'sqlite': connectors.Sqlite,
                     'mysql': connectors.MySql,
                     'postgres': connectors.Postgres,
                     'mongodb': connectors.MongoDb,
                     'elasticsearch': connectors.ElasticSearch}

    def __init__(self, qgl_str, query_graph):
        self.qgl_str = qgl_str
        self.query_graph = query_graph

        self.connect_block = ConnectBlock()
        self.retrieve_block = RetrieveBlock()
        self.join_block = JoinBlock()

        self.connectors = dict()

    def compile(self):
        parser = (pp.Keyword("CONNECT") + self.connect_block.parser() +
                  pp.Keyword("RETRIEVE") + self.retrieve_block.parser() +
                  pp.Keyword("JOIN") + self.join_block.parser())

        parser.parseString(self.qgl_str)
        self._create_connectors()
        self._create_query_nodes()
        self._create_joins()

    def _create_connectors(self):
        for conn_name, conn_dict in self.connect_block.connectors.items():
            conn_type = conn_dict['conn_type'].lower()
            conn_kwargs = conn_dict['conn_kwargs']
            self.connectors[conn_name] = self.connector_map[conn_type](**conn_kwargs)

    def _create_query_nodes(self):
        for node_name, node_dict in self.retrieve_block.nodes.items():
            self.query_graph.nodes[node_name] = QueryNode(name=node_name, query=node_dict['query_value'],
                                                          db_connector=self.connectors[node_dict['connector_name']],
                                                          fields=node_dict['fields'])
            if node_dict['manipulation_set'] is not None:
                print node_dict['manipulation_set']
                self.query_graph.nodes[node_name].manipulation_set.append_from_str(node_dict['manipulation_set'])

    def _create_joins(self):
        for join_dict in self.join_block.joins:
            parent_node_name = join_dict['parent_node']
            parent_cols = join_dict['parent_cols']
            child_node_name = join_dict['child_node']
            child_cols = join_dict['child_cols']
            on_columns = list()
            for parent_col, child_col in zip(parent_cols, child_cols):
                on_columns.append({parent_node_name: parent_col, child_node_name: child_col})
            self.query_graph.join(child_node=self.query_graph.nodes[child_node_name],
                                  parent_node=self.query_graph.nodes[parent_node_name],
                                  join_type=join_dict['join_type'].lower(),
                                  on_columns=on_columns)