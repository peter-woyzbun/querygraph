import re

import numpy as np
from pyparsing import (Suppress,
                       Word,
                       alphanums,
                       alphas,
                       SkipTo,
                       Literal,
                       ParseException,
                       Keyword,
                       Optional)

from querygraph.exceptions import QueryGraphException
from querygraph.type_converter import TypeConverter
from querygraph.manipulation.expression.evaluator import Evaluator
from querygraph.utils.deserializer import Deserializer


# =============================================
# Template Parameter Exceptions
# ---------------------------------------------

class TemplateParameterException(QueryGraphException):
    pass


class ParameterParseException(TemplateParameterException):
    pass


# =============================================
# Template Parameter
# ---------------------------------------------

class TemplateParameter(object):

    """
    Base query template parameter class. This class handles the conversion
    of given parameter values into the form required for query execution.
    Parameter strings have one of two possible forms:

        <parameter_name>|<container_type>:<data_type>

        or

        [<manipulation_expression>]|<container_type>:<data_type>


    Parameters
    ----------
    param_str : str
        The raw parameter string.
    parameter_type : str
        The type of parameter: independent or dependent

    Attributes
    ----------
    name : str or None
        The name assigned to the parameter - this will be None if a
        manipulation expression is provided in place of a parameter
        name (see above).
    data_type : str (after initial parse)
        The data type of the parameter query value - this determines
        how the value is rendered. For example, a 'str' data type
        will likely require quotes (e.g. in SQL queries).
    custom_data_type_str : str (after initial parse)
        Desc
    container_type : str (after initial parse)
        Desc


    """

    DATA_TYPES = ('int',
                  'str',
                  'float',
                  'datetime',
                  'date',
                  'time',
                  'custom')

    def __init__(self,
                 param_str,
                 independent=True):
        self.param_str = param_str
        self.name = None
        self.independent = independent
        self.param_expr_str = None
        self.data_type = None
        self.custom_data_type_str = None
        self.container_type = None
        self.pre_value = None

        self.type_converter = TypeConverter()
        self.deserialize = Deserializer()

        self._setup_generic_converters()
        self._setup_db_specific_converters()
        # self._initial_parse()

    def _setup_generic_converters(self):
        # Setup generic converters for 'int' type converters.
        self.type_converter.add_int_converters({int: lambda x: x,
                                                np.int64: lambda x: x,
                                                np.int32: lambda x: x,
                                                np.int16: lambda x: x,
                                                np.int8: lambda x: x,
                                                float: lambda x: int(x),
                                                str: lambda x: int(x)})
        # Setup generic converters for 'float' type converters.
        self.type_converter.add_float_converters({int: lambda x: float(x),
                                                  np.int64: lambda x: float(x),
                                                  np.float64: lambda x: x,
                                                  float: lambda x: x,
                                                  str: lambda x: float(x)})
        # Setup generic converters for 'str' type converters.
        self.type_converter.add_str_converters({str: lambda x: "'%s'" % x,
                                                float: lambda x: "'%s'" % x,
                                                int: lambda x: "'%s'" % x})

    def _setup_db_specific_converters(self):
        pass

    @property
    def _data_type_parser(self):
        """ Return Pyparsing parser object for parsing data type. """
        # Create a keyword for each data type.
        data_type_keywords = [Keyword(d_type) for d_type in self.DATA_TYPES]
        # This is equivalent to: (<keyword_1> | <keyword_2> | ... | <keyword_n>)
        data_type = reduce(lambda x, y: x | y, data_type_keywords)
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
            # return self.DATA_TYPES[self.data_type][type(pre_value)](pre_value)
            return self.type_converter.convert(data_type=self.data_type, value=pre_value)

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
        container_type = Optional(value_list | value)
        container_type.addParseAction(lambda x: self._set_attribute(target='container_type', value=x[0]))

        custom = (Suppress('custom[') + SkipTo(Suppress(']'), include=True))
        custom.addParseAction(lambda x: self._set_custom_data_type(x[0]))
        data_type = (self._data_type_parser | custom)
        # data_type = (num | _int | _float | _str | _date | custom)
        data_type.addParseAction(lambda x: self._set_attribute(target='data_type', value=x[0]))

        parameter_block = (param_declaration + Suppress("|") + container_type + Suppress(":") + data_type)

        try:
            parameter_block.parseString(self.param_str)
        except ParseException:
            raise ParameterParseException

    def _parse(self, parent_node_name=None, df=None, independent_param_vals=None):
        if self.independent:
            expr_evaluator = Evaluator(name_dict=independent_param_vals)
        else:
            expr_evaluator = Evaluator(df=df, df_name=parent_node_name)
        param_expr = expr_evaluator.parser()

        container_type = (Optional('value', default='value') | Literal("list"))
        container_type.addParseAction(lambda x: self._set_attribute(target='container_type', value=x[0]))

        custom = (Suppress('custom[') + SkipTo(Suppress(']'), include=True))
        custom.addParseAction(lambda x: self._set_custom_data_type(x[0]))
        data_type = (self._data_type_parser | custom)
        # data_type = (num | _int | _float | _str | _date | custom)
        data_type.addParseAction(lambda x: self._set_attribute(target='data_type', value=x[0]))

        parameter_block = (param_expr + Suppress("|") + container_type + Suppress(":") + data_type)
        parameter_block.parseString(self.param_str)
        self.pre_value = expr_evaluator.output_value()

    def _make_query_value(self):
        if self.container_type == 'list':
            return self._make_value_list(self.pre_value)
        elif self.container_type == 'value':
            return self._make_single_value(self.pre_value)

    def query_value(self, df=None, parent_node_name=None, independent_param_vals=None):
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
        parent_node_name : str
            The name of the parent node: used for...
        independent_param_vals : dict
            A dictionary mapping independent parameter names to given parameter
            values.

        """
        # If the parameter is independent, and no independent parameter values are given,
        # raise exception.
        if self.independent and independent_param_vals is None:
            raise TemplateParameterException("Independent template parameter '%s' cannot be defined because no "
                                             "independent parameter values were given." % self.name)
        # If the parameter is dependent, and no dataframe is given, raise exception.
        if not self.independent and df is None:
            raise TemplateParameterException("Dependent template parameter '%s' cannot be defined because no "
                                             "parent dataframe was given." % self.name)

        self._parse(df=df,
                    parent_node_name=parent_node_name,
                    independent_param_vals=independent_param_vals)
        return self._make_query_value()
