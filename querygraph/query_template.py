import re

from querygraph.template_parameter import TemplateParameter


# =============================================
# QueryTemplate Class
# ---------------------------------------------

class QueryTemplate(object):

    def __init__(self, template_str, type_converter):
        self.template_str = template_str
        self.type_converter = type_converter

    def _render_independent_param(self, param_str, independent_param_vals):
        independent_parameter = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        return str(independent_parameter.render(independent_param_vals=independent_param_vals))

    def _render_dependent_param(self, param_str, df):
        dependent_parameter = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        return dependent_parameter.render(df=df)

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
        return parsed_query

