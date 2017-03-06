import re
from abc import abstractmethod

from querygraph.exceptions import QueryGraphException
from querygraph.query.template.parameter import TemplateParameter


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

class BaseQueryTemplate(object):

    def __init__(self, template_str, db_connector, parameter_class):
        self.template_str = template_str
        self.db_connector = db_connector
        self.rendered_query = None
        self.parameter_class = parameter_class

    def render(self, df=None, **independent_param_vals):
        """
        Returns parsed query template string.

        """
        parsed_query = ""
        tokens = re.split(r"(?s)({{.*?}}|{%.*?%}|{#.*?#})", self.template_str)
        for token in tokens:
            # Dependent parameter.
            if token.startswith('{{'):
                tok_expr = token[2:-2].strip()
                # dependent_parameter = QueryParameter(parameter_str=tok_expr)
                dependent_parameter = self.parameter_class(parameter_str=tok_expr, parameter_type='dependent')
                if df is None:
                    raise DependentParameterException("No dataframe was given from which to generate dependent"
                                                      "parameter value(s).")
                parsed_query += dependent_parameter.query_value(df=df)
            # Comment.
            elif token.startswith('{#'):
                pass
            # Independent parameter.
            elif token.startswith('{%'):
                tok_expr = token[2:-2].strip()
                independent_parameter = self.parameter_class(parameter_str=tok_expr, parameter_type='independent')
                if not independent_param_vals:
                    raise IndependentParameterException("Independent parameters present in query and no independent"
                                                        "parameter values given.")
                parsed_query += str(independent_parameter.query_value(independent_params=independent_param_vals))
            else:
                parsed_query += token
        return self._post_render_value(parsed_query)

    def pre_render(self, df=None, **independent_param_vals):
        self.rendered_query = self.render(df, **independent_param_vals)

    def _post_render_value(self, render_value):
        return render_value

    def execute(self, df=None, independent_param_vals=None):
        pass
