import re
import datetime
from collections import defaultdict

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

from querygraph import exceptions
from querygraph.manipulation.expression.evaluator import Evaluator
from querygraph.manipulation.expression import ManipulationExpression


# =============================================
# Template Parameter Exceptions
# ---------------------------------------------

class TemplateParameterException(exceptions.QueryGraphException):
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



    """

    DATA_TYPES = ('int',
                  'str',
                  'float',
                  'datetime',
                  'date',
                  'time',
                  'custom')

    def __init__(self, param_str, independent=True):
        self.param_str = param_str
        self.independent = independent

        self.container_type = None
        self.data_type = None
        self.custom_data_type_str = None

        self.type_converters = defaultdict(dict)

        self._setup_generic_converters()
        self._setup_db_specific_converters()

        self.python_value = None

    # =============================================
    # Type Conversion Handling
    # ---------------------------------------------

    def _setup_generic_converters(self):

        self._add_int_converters(
            {int: lambda x: x,
             np.int64: lambda x: x,
             np.int32: lambda x: x,
             np.int16: lambda x: x,
             np.int8: lambda x: x,
             np.float64: lambda x: x,
             np.float32: lambda x: x,
             np.float16: lambda x: x,
             float: lambda x: int(x),
             str: lambda x: int(x)}
        )

        self._add_float_converters(
            {int: lambda x: float(x),
             np.int64: lambda x: float(x),
             np.int32: lambda x: float(x),
             np.int16: lambda x: float(x),
             np.int8: lambda x: float(x),
             np.float64: lambda x: x,
             np.float32: lambda x: x,
             np.float16: lambda x: x,
             float: lambda x: x,
             str: lambda x: float(x)}
        )

        self._add_str_converters(
            {str: lambda x: "'%s'" % x,
             unicode: lambda x: "'%s'" % x,
             float: lambda x: "'%s'" % x,
             int: lambda x: "'%s'" % x}
        )

        self._add_date_converters(
            {datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
             datetime.date: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
             str: lambda x: "'%s'" % x}
        )

    def _setup_db_specific_converters(self):
        pass

    def convert(self, data_type, value):
        if data_type not in self.type_converters:
            raise exceptions.TypeConversionError("There are no converters defined for parameter "
                                                 "data type '%s'." % data_type)
        if type(value) not in self.type_converters[data_type]:
            raise exceptions.TypeConversionError("There is no converter defined for parameter data type '%s', "
                                                 "input type '%s'." % (data_type, type(value)))
        return self.type_converters[data_type][type(value)](value)

    def _convert_python_value(self, python_value):
        if type(python_value) not in self.type_converters[self.data_type]:
            raise exceptions.TypeConversionError("There is no converter defined for parameter data type '%s', "
                                                 "input type '%s'." % (self.data_type, type(python_value)))
        return self.type_converters[self.data_type][type(python_value)](python_value)

    def _add_type_converter(self, data_type, input_type, converter):
        """
        Add a single type converter to the 'type_converters' dict.

        """
        if data_type not in self.DATA_TYPES:
            raise exceptions.ParameterConfigException("Trying to add converter for data type ('%s')"
                                                      " that does not exist." % data_type)
        if not callable(converter):
            raise exceptions.ParameterConfigException("Tried to add a 'converter' that is not callable.")
        self.type_converters[data_type][input_type] = converter

    def _add_type_converters(self, data_type, converter_dict):
        for input_type, converter in converter_dict.items():
            self._add_type_converter(data_type=data_type,
                                     input_type=input_type,
                                     converter=converter)

    def _add_int_converters(self, converter_dict):
        self._add_type_converters(data_type='int', converter_dict=converter_dict)

    def _add_float_converters(self, converter_dict):
        self._add_type_converters(data_type='float', converter_dict=converter_dict)

    def _add_str_converters(self, converter_dict):
        self._add_type_converters(data_type='str', converter_dict=converter_dict)

    def _add_datetime_converters(self, converter_dict):
        self._add_type_converters(data_type='datetime', converter_dict=converter_dict)

    def _add_date_converters(self, converter_dict):
        self._add_type_converters(data_type='date', converter_dict=converter_dict)

    def _add_time_converters(self, converter_dict):
        self._add_type_converters(data_type='time', converter_dict=converter_dict)

    # =============================================
    # Parsing/Rendering
    # ---------------------------------------------

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

    def _expr_evaluator(self, parent_node_name=None, df=None, independent_param_vals=None):
        if self.independent:
            expr_evaluator = Evaluator(name_dict=independent_param_vals)
        else:
            expr_evaluator = Evaluator(df=df, df_name=parent_node_name)
        return expr_evaluator

    def _make_python_value(self, parent_node_name=None, df=None, independent_param_vals=None):
        expr_evaluator = self._expr_evaluator(parent_node_name, df, independent_param_vals)
        param_expr = expr_evaluator.parser()

        container_type = Optional(Literal("list") + Suppress(":"), default='value')
        container_type.addParseAction(lambda x: self._set_attribute(target='container_type', value=x[0]))

        custom = (Suppress('custom[') + SkipTo(Suppress(']'), include=True))
        custom.addParseAction(lambda x: self._set_custom_data_type(x[0]))
        data_type = (self._data_type_parser() | custom)
        # data_type = (num | _int | _float | _str | _date | custom)
        data_type.addParseAction(lambda x: self._set_attribute(target='data_type', value=x[0]))

        parameter_block = (param_expr + Suppress("->") + container_type + data_type)
        parameter_block.parseString(self.param_str)
        self.python_value = expr_evaluator.output_value()

    def _make_atomic_query_value(self, python_value):
        if self.data_type == 'custom':
            return self.custom_data_type_str % python_value
        else:
            return self._convert_python_value(python_value=python_value)

    def _make_list_query_value(self):
        parameter_value = self.python_value
        val_str = ", ".join(str(self._make_atomic_query_value(x)) for x in parameter_value)
        val_str = "(%s)" % val_str
        return val_str

    def _data_requirement_check(self, df=None, independent_param_vals=None):
        # If the parameter is independent, and no independent parameter values are given,
        # raise exception.
        if self.independent and independent_param_vals is None:
            raise TemplateParameterException("Independent template parameter cannot be defined because no "
                                             "independent parameter values were given.")
        # If the parameter is dependent, and no dataframe is given, raise exception.
        if not self.independent and df is None:
            raise TemplateParameterException("Dependent template parameter cannot be defined because no "
                                             "parent dataframe was given.")

    def query_value(self, df=None, parent_node_name=None, independent_param_vals=None):
        """
        ...

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
        self._data_requirement_check(df=df, independent_param_vals=independent_param_vals)

        self._make_python_value(df=df,
                                parent_node_name=parent_node_name,
                                independent_param_vals=independent_param_vals)
        if self.container_type == 'list':
            value = self._make_list_query_value()
            return value
        elif self.container_type == 'value':
            value = self._make_atomic_query_value(python_value=self.python_value)
            return value