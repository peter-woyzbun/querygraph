import datetime
import ast

from querygraph.query.templates.base_parameter import BaseTemplateParameter
from querygraph.query.templates.base_template import BaseQueryTemplate


# =============================================
# MongoDb Template Parameter
# ---------------------------------------------

class MongoDbParameter(BaseTemplateParameter):

    def __init__(self, parameter_str, parameter_type):
        BaseTemplateParameter.__init__(self, parameter_str, parameter_type)

    def _make_value_list(self, parameter_value):
        parameter_value = list(set(parameter_value))
        val_str = ", ".join(str(self._make_single_value(x)) for x in parameter_value)
        val_str = "[%s]" % val_str
        return val_str


# =============================================
# MongoDb Query Template
# ---------------------------------------------


class QueryTemplate(BaseQueryTemplate):

    def __init__(self, template_str, db_connector, fields):
        self.fields = fields
        BaseQueryTemplate.__init__(self,
                                   template_str=template_str,
                                   db_connector=db_connector,
                                   parameter_class=MongoDbParameter)

    def _post_render_value(self, render_value):
        post_value = ast.literal_eval(render_value)
        return post_value

    def execute(self, df=None, **independent_param_vals):
        if self.rendered_query is not None:
            rendered_query = self.rendered_query
        else:
            rendered_query = self.render(df=df, **independent_param_vals)
        df = self.db_connector.execute_query(query=rendered_query, fields=self.fields)
        return df
