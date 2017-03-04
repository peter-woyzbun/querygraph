from pyparsing import Suppress, Word, alphanums, alphas, SkipTo, Literal, ParseException

from querygraph.exceptions import QueryGraphException


# =============================================
# Template Parameter Exceptions
# ---------------------------------------------

class TemplateParameterException(QueryGraphException):
    pass


class ParameterParseException(TemplateParameterException):
    pass


class TemplateParameter(object):

    GENERIC_DATA_TYPES = {'int': lambda x: int(x),
                          'float': lambda x: float(x),
                          'str': lambda x: "'%s'" % str(x)}
    DATA_TYPES = dict()
    CONTAINER_TYPES = dict()

    def __init__(self, parameter_str):
        self.parameter_str = parameter_str
        self.param_expr_str = None


    def _initial_parse(self):
        # Parameter expression in brackets...
        param_expr = (Suppress('[') + SkipTo(Suppress(']'), include=True))
        param_expr.addParseAction(lambda x: self._set_attribute('param_expr_str', value=x[0]))

        param_name = Word(alphas, alphanums + "_$")
        param_name.addParseAction(lambda x: self._set_attribute(target='name', value=x[0]))

        param_declaration = (param_expr | param_name)

        # Container types.
        value_list = Literal('value_list')
        value = Literal('value')
        container_type = (value_list | value)
        container_type.addParseAction(lambda x: self._set_attribute(target='container_type', value=x[0]))

        # Data types
        num = Literal('num')
        _int = Literal('int')
        _float = Literal('float')
        _str = Literal('str')
        _date = Literal('date')

        custom = (Suppress('custom[') + SkipTo(Suppress(']'), include=True))
        custom.addParseAction(lambda x: self._set_custom_data_type(x[0]))
        data_type_literals = [Literal(x) for x in self.DATA_TYPES.keys()]
        data_type = reduce(lambda x, y: x | y, data_type_literals)
        # data_type = (num | _int | _float | _str | _date | custom)
        data_type.addParseAction(lambda x: self._set_attribute(target='data_type', value=x[0]))

        parameter_block = (param_declaration + Suppress("|") + container_type + Suppress(":") + data_type)
        try:
            parameter_block.parseString(self.param_str)
        except ParseException:
            raise ParameterParseException