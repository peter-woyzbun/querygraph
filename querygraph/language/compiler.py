import pyparsing as pp


class ConnectBlock(object):

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

    def __init__(self):
        self.nodes = dict()

    def parser(self):
        pass