import datetime
import ast

from querygraph.query.templates import base


# =============================================
# MongoDb Template Parameter
# ---------------------------------------------

class MongoDbParameter(base.TemplateParameter):

    def __init__(self, parameter_str, parameter_type):
        base.TemplateParameter.__init__(self, parameter_str, parameter_type)

    def _make_value_list(self, parameter_value):
        parameter_value = list(set(parameter_value))
        val_str = ", ".join(str(self._make_single_value(x)) for x in parameter_value)
        val_str = "[%s]" % val_str
        return val_str


# =============================================
# MongoDb Query Template
# ---------------------------------------------


class QueryTemplate(base.QueryTemplate):

    def __init__(self, template_str, db_connector, fields):
        self.fields = fields
        base.QueryTemplate.__init__(self,
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
