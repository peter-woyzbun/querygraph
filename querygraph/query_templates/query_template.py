import re

from querygraph.exceptions import QueryGraphException
from querygraph.query_templates.template_parameter import TemplateParameter


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
# Query Parser Class
# ---------------------------------------------

class QueryTemplate(object):

    def __init__(self, query):
        self.query = query
        self.query_isolated = True

    def parse(self, df=None, independent_params=None):
        """
        Parses the query template string.

        """
        parsed_query = ""
        tokens = re.split(r"(?s)({{.*?}}|{%.*?%}|{#.*?#})", self.query)
        for token in tokens:
            # Dependent parameter.
            if token.startswith('{{'):
                tok_expr = token[2:-2].strip()
                # dependent_parameter = QueryParameter(parameter_str=tok_expr)
                dependent_parameter = TemplateParameter(param_str=tok_expr)
                if df is None:
                    raise DependentParameterException("No dataframe was given from which to generate dependent"
                                                      "parameter value(s).")
                if dependent_parameter.param_expr_str is None:
                    pre_val = df[dependent_parameter.name]
                else:
                    pre_val = None
                parsed_query += dependent_parameter.query_value(pre_value=pre_val, df=df)
            # Comment.
            elif token.startswith('{#'):
                pass
            # Independent parameter.parameter_value=df[dependent_parameter.name].unique()
            elif token.startswith('{%'):
                tok_expr = token[2:-2].strip()
                independent_parameter = TemplateParameter(param_str=tok_expr)
                if independent_params is None:
                    raise IndependentParameterException("Independent parameters present in query and no independent"
                                                        "parameter values given.")
                if independent_parameter.param_expr_str is None:
                    pre_val = df[independent_parameter.name]
                    parsed_query += str(independent_parameter.query_value(pre_value=pre_val))
                else:
                    parsed_query += str(independent_parameter.query_value(independent_params=independent_params))
            else:
                parsed_query += token
        print parsed_query
        return parsed_query

    def has_dependent_parameters(self):
        contains_dependent_parameter = False
        tokens = re.split(r"(?s)({{.*?}})", self.query)
        for token in tokens:
            if token.startswith('{{'):
                contains_dependent_parameter = True
                break
        return contains_dependent_parameter