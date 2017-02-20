import re

from querygraph.exceptions import QueryGraphException
from querygraph.parsing.query_parameter import QueryParameter


# =============================================
# Exceptions
# ---------------------------------------------

class QueryParserException(QueryGraphException):
    pass


class DependentParameterException(QueryGraphException):
    pass


class IndependentParameterException(QueryGraphException):
    pass


# =============================================
# Query Parser Class
# ---------------------------------------------

class QueryParser(object):

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
                dependent_parameter = QueryParameter(parameter_str=tok_expr)
                if df is None:
                    raise DependentParameterException("No dataframe was given from which to generate dependent"
                                                      "parameter value(s).")
                if dependent_parameter.name not in df.columns:
                    raise DependentParameterException("The dependent parameter '%s' is not defined in the parent "
                                                      "node's dataframe." % dependent_parameter.name)
                parsed_query += dependent_parameter.query_value(parameter_value=df[dependent_parameter.name].unique())
            # Comment.
            elif token.startswith('{#'):
                pass
            # Independent parameter.
            elif token.startswith('{%'):
                tok_expr = token[2:-2].strip()
                independent_parameter = QueryParameter(parameter_str=tok_expr)
                print "INDEPENDENT PARAMETER NAME: %s" % independent_parameter.name
                if independent_params is None:
                    raise IndependentParameterException("Independent parameters present in query and no independent"
                                                        "parameter values given.")
                if independent_parameter.name not in independent_params:
                    raise IndependentParameterException("Independent parameter '%s' has no"
                                                        " value defined." % independent_parameter.name)
                parsed_query += str(independent_parameter.\
                    query_value(parameter_value=independent_params[independent_parameter.name]))
            else:
                parsed_query += token
        return parsed_query

    def has_dependent_parameters(self):
        contains_dependent_parameter = False
        tokens = re.split(r"(?s)({{.*?}})", self.query)
        for token in tokens:
            if token.startswith('{{'):
                contains_dependent_parameter = True
                break
        return contains_dependent_parameter