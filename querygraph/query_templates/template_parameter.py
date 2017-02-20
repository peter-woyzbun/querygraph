from pyparsing import *

from querygraph.exceptions import QueryGraphException
from querygraph.evaluation.evaluator import Evaluator


# =============================================
# Template Parameter Exceptions
# ---------------------------------------------

class TemplateParameterException(QueryGraphException):
    pass


# =============================================
# Template Parameter Class
# ---------------------------------------------


class TemplateParameter(object):

    def __init__(self, param_str):
        self.param_str = param_str
        self.param_expr_str = None
        self.name = None
        self.container_type = None
        self.data_type = None

        self._initial_parse()

    def _set_attribute(self, target, value):
        setattr(self, target, value)

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

        data_type = (num | _int | _float | _str)
        data_type.addParseAction(lambda x: self._set_attribute(target='data_type', value=x[0]))

        parameter_block = (param_declaration + Suppress("|") + container_type + Suppress(":") + data_type)
        parameter_block.parseString(self.param_str)

    def _make_single_value(self, value):
        data_type_formatter = {'num': lambda x: x,
                               'int': lambda x: int(x),
                               'float': lambda x: float(x),
                               'str': lambda x: "'%s'" % x}
        return data_type_formatter[self.data_type](value)

    def _make_value_list(self, parameter_value):
        # Todo: unique values only?
        parameter_value = list(set(parameter_value))
        val_str = ", ".join(str(self._make_single_value(x)) for x in parameter_value)
        val_str = "(%s)" % val_str
        return val_str

    def _make_query_value(self, pre_value):
        if self.container_type == 'value_list':
            return self._make_value_list(pre_value)
        elif self.container_type == 'value':
            return self._make_single_value(pre_value)

    def query_value(self, pre_value=None, df=None, independent_params=None):
        if not self.param_expr_str and (pre_value is None):
            raise TemplateParameterException("Something!")
        if pre_value is not None:
            return self._make_query_value(pre_value=pre_value)
        else:
            evaluator = Evaluator()
            pre_value = evaluator.eval(eval_str=self.param_expr_str, df=df, independent_params=independent_params)
            return self._make_query_value(pre_value=pre_value)