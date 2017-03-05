from pyparsing import Suppress, Word, alphanums, alphas, SkipTo, Literal, ParseException

from querygraph.exceptions import QueryGraphException
from querygraph.manipulation.expression.evaluator import Evaluator


# =============================================
# Template Parameter Exceptions
# ---------------------------------------------

class TemplateParameterException(QueryGraphException):
    pass


class ParameterParseException(TemplateParameterException):
    pass


class TemplateParameter(object):

    GENERIC_DATA_TYPES = {'int': {int: lambda x: x,
                                  float: lambda x: int(x),
                                  str: lambda x: int(x)},
                          'float': {int: lambda x: float(x),
                                    float: lambda x: x,
                                    str: lambda x: float(x)},
                          'str': {str: lambda x: "'%s'" % x,
                                  float: lambda x: "'%s'" % x,
                                  int: lambda x: "'%s'" % x}}

    CHILD_DATA_TYPES = dict()
    DATA_TYPES = dict()
    CONTAINER_TYPES = dict()

    def __init__(self, parameter_str, parameter_type):
        self.parameter_str = parameter_str
        self.name = None
        self.parameter_type = parameter_type
        self.param_expr_str = None
        self.data_type = None
        self.custom_data_type_str = None
        self.container_type = None
        self._make_data_types()
        self._initial_parse()

    def _make_data_types(self):
        if not self.CHILD_DATA_TYPES:
            self.DATA_TYPES = self.GENERIC_DATA_TYPES
        else:
            self.DATA_TYPES = self.CHILD_DATA_TYPES.copy()
            for data_type, input_type_dict in self.GENERIC_DATA_TYPES.items():
                if data_type not in self.DATA_TYPES:
                    self.DATA_TYPES[data_type] = input_type_dict
                elif data_type in self.DATA_TYPES:
                    for input_type in input_type_dict.keys():
                        if input_type not in self.DATA_TYPES[data_type]:
                            self.DATA_TYPES[data_type][input_type] = input_type_dict[input_type]

    @property
    def _data_type_parser(self):
        data_type_literals = [Literal(d_type) for d_type in self.DATA_TYPES.keys()]
        data_type = reduce(lambda x, y: x | y, data_type_literals)
        return data_type

    def _set_attribute(self, target, value):
        setattr(self, target, value)

    def _set_custom_data_type(self, value):
        self.custom_data_type_str = value
        return 'custom'

    def _make_single_value(self, pre_value):
        if self.data_type == 'custom':
            return self.custom_data_type_str % pre_value
        else:
            return self.DATA_TYPES[self.data_type][type(pre_value)](pre_value)

    def _make_value_list(self, parameter_value):
        parameter_value = list(set(parameter_value))
        val_str = ", ".join(str(self._make_single_value(x)) for x in parameter_value)
        val_str = "(%s)" % val_str
        return val_str

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

        custom = (Suppress('custom[') + SkipTo(Suppress(']'), include=True))
        custom.addParseAction(lambda x: self._set_custom_data_type(x[0]))
        data_type = (self._data_type_parser | custom)
        # data_type = (num | _int | _float | _str | _date | custom)
        data_type.addParseAction(lambda x: self._set_attribute(target='data_type', value=x[0]))

        parameter_block = (param_declaration + Suppress("|") + container_type + Suppress(":") + data_type)
        try:
            parameter_block.parseString(self.parameter_str)
        except ParseException:
            raise ParameterParseException

    def _make_query_value(self, pre_value):
        if self.container_type == 'value_list':
            return self._make_value_list(pre_value)
        elif self.container_type == 'value':
            return self._make_single_value(pre_value)

    def query_value(self, df=None, independent_params=None):
        """
        Return TemplateParameter query value. This involves:

            (1) Creating the query 'pre_value', which is either
                derived from evaluating the parameter's expression
                string, or taken directly from the independent_params
                dict.
            (2) Converting the pre_value into its actualy query value,
                which is based on the parameter 'data_type' and
                'container_type' (single value, or list of values).

        Parameters
        ----------
        df : Pandas DataFrame or None
            If not None, the DataFrame of the parent node of the QueryNode this
            TemplateParameter is associated with.
        independent_params : dict
            A dictionary mapping independent parameter names to given parameter
            values.

        """
        # If the parameter is independent, and no independent parameter values are given,
        # raise exception.
        if self.parameter_type == 'independent' and independent_params is None:
            raise TemplateParameterException("Independent template parameter '%s' cannot be defined because no "
                                             "independent parameter values were given." % self.name)
        # If the parameter is dependent, and no dataframe is given, raise exception.
        if self.parameter_type == 'dependent' and df is None:
            raise TemplateParameterException("Dependent template parameter '%s' cannot be defined because no "
                                             "parent dataframe was given." % self.name)

        if self.parameter_type == 'independent' and self.param_expr_str is None:
            pre_value = independent_params[self.name]
            return self._make_query_value(pre_value=pre_value)
        elif self.parameter_type == 'dependent' and self.param_expr_str is None:
            pre_value = df[self.name]
            return self._make_query_value(pre_value=pre_value)
        else:
            evaluator = Evaluator()
            pre_value = evaluator.eval(eval_str=self.param_expr_str, df=df, independent_param_vals=independent_params)
            return self._make_query_value(pre_value=pre_value)