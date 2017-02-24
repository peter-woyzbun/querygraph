from pyparsing import Suppress, SkipTo, Word, alphas, alphanums, Literal, ParseException

from querygraph.exceptions import QueryGraphException
from querygraph.evaluation.evaluator import Evaluator
from querygraph.db.connectors import DatabaseConnector, SQLite, MySQL


# =============================================
# Template Parameter Exceptions
# ---------------------------------------------

class TemplateParameterException(QueryGraphException):
    pass


class ParameterParseException(TemplateParameterException):
    pass


# =============================================
# Template Parameter Class
# ---------------------------------------------


class TemplateParameter(object):

    def __init__(self, param_str, param_type, db_connector):
        self.param_str = param_str
        self.param_type = param_type
        self.param_expr_str = None
        self.name = None
        self.container_type = None
        self.data_type = None
        if not isinstance(db_connector, DatabaseConnector):
            raise TemplateParameterException
        self.db_connector = db_connector
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
        _date = Literal('date')

        data_type = (num | _int | _float | _str| _date)
        data_type.addParseAction(lambda x: self._set_attribute(target='data_type', value=x[0]))

        parameter_block = (param_declaration + Suppress("|") + container_type + Suppress(":") + data_type)
        try:
            parameter_block.parseString(self.param_str)
        except ParseException:
            raise ParameterParseException

    def _make_single_date(self, value):
        if isinstance(self.db_connector, SQLite):
            return "date(%s)" % value.strftime('%Y-%m-%d')
        elif isinstance(self.db_connector, MySQL):
            return "date(%s)" % value.strftime('%Y-%m-%d')

    def _make_single_datetime(self, value):
        if isinstance(self.db_connector, SQLite):
            return "datetime(%s)" % value.strftime('%Y-%m-%d %H:%M:%S')

    def _make_single_value(self, value):
        data_type_formatter = {'num': lambda x: x,
                               'int': lambda x: int(x),
                               'float': lambda x: float(x),
                               'str': lambda x: "'%s'" % x,
                               'date': lambda x: self._make_single_date(x),
                               'datetime': lambda x: self._make_single_datetime(x)}
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
        if self.param_type == 'independent' and independent_params is None:
            raise TemplateParameterException("Independent template parameter '%s' cannot be defined because no "
                                             "independent parameter values were given." % self.name)
        # If the parameter is dependent, and no dataframe is given, raise exception.
        if self.param_type == 'dependent' and df is None:
            raise TemplateParameterException("Dependent template parameter '%s' cannot be defined because no "
                                             "parent dataframe was given." % self.name)

        if self.param_type == 'independent' and self.param_expr_str is None:
            pre_value = independent_params[self.name]
            return self._make_query_value(pre_value=pre_value)
        elif self.param_type == 'dependent' and self.param_expr_str is None:
            pre_value = df[self.name]
            return self._make_query_value(pre_value=pre_value)
        else:
            evaluator = Evaluator()
            pre_value = evaluator.eval(eval_str=self.param_expr_str, df=df, independent_params=independent_params)
            return self._make_query_value(pre_value=pre_value)