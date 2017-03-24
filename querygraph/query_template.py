import re

from querygraph.exceptions import QueryGraphException
from querygraph.utils.deserializer import Deserializer
from querygraph.template_parameter import TemplateParameter


# =============================================
# Exceptions
# ---------------------------------------------

class QueryTemplateException(QueryGraphException):
    pass


class DependentParameterException(QueryTemplateException):
    pass


class IndependentParameterException(QueryTemplateException):
    pass


# =============================================
# QueryTemplate Base Class
# ---------------------------------------------

class QueryTemplate(object):
    """
    Base QueryTemplate class. Handles rendering of queries with parameter values so
    they may be executed and data retrieved.

    Parameters
    ----------
    template_str : str
        The raw template string. This should be a valid query, once parameters
        are replaced with actual values, for whatever database type it is
        intended for.
    db_connector : DbConnector instance
        The database connector for this query.
    parameter_class : Child TemplateParameter class instance.
        The TemplateParameter class associated with the given query template database type (e.g. 'MySql').
        This class instance is used to render the actual parameter values.


    """

    def __init__(self, template_str, db_connector, parameter_class, fields=None):
        self.template_str = template_str
        self.db_connector = db_connector
        self.rendered_query = None
        if not issubclass(parameter_class, TemplateParameter):
            raise Exception
        self.parameter_class = parameter_class
        self.fields = fields

        self.deserialize = Deserializer()

    def _render_independent_param(self, param_str, independent_param_vals):
        independent_parameter = self.parameter_class(param_str=param_str, independent=True)
        return str(independent_parameter.query_value(independent_param_vals=independent_param_vals))

    def _render_dependent_param(self, param_str, df):
        dependent_parameter = self.parameter_class(param_str=param_str, independent=False)
        return dependent_parameter.query_value(df=df)

    def render(self, df=None, independent_param_vals=None):
        """
        Returns parsed query template string.

        """
        parsed_query = ""
        tokens = re.split(r"(?s)({{.*?}}|{%.*?%}|{#.*?#})", self.template_str)
        for token in tokens:
            # Dependent parameter.
            if token.startswith('{{'):
                tok_expr = token[2:-2].strip()
                parsed_query += self._render_dependent_param(param_str=tok_expr, df=df)
            # Comment.
            elif token.startswith('{#'):
                pass
            # Independent parameter.
            elif token.startswith('{%'):
                tok_expr = token[2:-2].strip()
                parsed_query += self._render_independent_param(param_str=tok_expr,
                                                               independent_param_vals=independent_param_vals)
            else:
                parsed_query += token
        return self._post_render_value(parsed_query)

    def pre_render(self, df=None, independent_param_vals=None):
        self.rendered_query = self.render(df, independent_param_vals)

    def _post_render_value(self, render_value):
        return render_value

    def execute(self, df=None, independent_param_vals=None):
        """ Should be implemented by child class. """
        raise NotImplementedError

