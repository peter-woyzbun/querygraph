from pyparsing import Word, alphanums, alphas, Literal, Suppress


class QueryParameter(object):

    def __init__(self, parameter_str):
        self.parameter_str = parameter_str
        self.name = None
        self.container_type = None
        self.data_type = None

        self._parse()

    def _set_attribute(self, target, value):
        setattr(self, target, value)

    def _parse(self):
        parameter_name = Word(alphas, alphanums + "_$").\
            addParseAction(lambda x: self._set_attribute(target='name', value=x[0]))
        # Container types.
        value_list = Literal('value_list')
        value = Literal('value')
        container_type = (value_list | value).addParseAction(lambda x: self._set_attribute(target='container_type',
                                                                                           value=x[0]))
        # Data types
        num = Literal('num')
        _int = Literal('int')
        _float = Literal('float')
        _str = Literal('str')
        data_type = (num | _int | _float | _str).addParseAction(lambda x: self._set_attribute(target='data_type',
                                                                                              value=x[0]))
        parser = (parameter_name + Suppress("|") + container_type + Suppress(":") + data_type)
        parser.parseString(self.parameter_str)

    def _make_single_value(self, value):
        data_type_formatter = {'num': lambda x: x,
                               'int': lambda x: int(x),
                               'float': lambda x: float(x),
                               'str': lambda x: "'%s'" % x}
        return data_type_formatter[self.data_type](value)

    def _make_value_list(self, parameter_value):
        val_str = ", ".join(str(self._make_single_value(x)) for x in parameter_value)
        val_str = "(%s)" % val_str
        return val_str

    def query_value(self, parameter_value):
        if self.container_type == 'value_list':
            return self._make_value_list(parameter_value)
        elif self.container_type == 'value':
            return self._make_single_value(parameter_value)