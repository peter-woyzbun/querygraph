import datetime
import ast

from querygraph.template_parameter import TemplateParameter
from querygraph.query_template import QueryTemplate


# =============================================
# MongoDb Template Parameter
# ---------------------------------------------

class MongoDbParameter(TemplateParameter):

    def __init__(self, param_str, independent=True):
        TemplateParameter.__init__(self,
                                   param_str=param_str,
                                   independent=independent)

    def _make_list_query_value(self):
        parameter_value = self.prep_value
        val_str = ", ".join(str(self._make_atomic_query_value(x)) for x in parameter_value)
        val_str = "[%s]" % val_str
        return val_str

    def _setup_db_specific_converters(self):
        self._add_datetime_converters({datetime.datetime: lambda x: repr(x)})


# =============================================
# MongoDb Query Template
# ---------------------------------------------


class MongoDbTemplate(QueryTemplate):

    def __init__(self, template_str, db_connector, fields):
        QueryTemplate.__init__(self,
                               template_str=template_str,
                               db_connector=db_connector,
                               parameter_class=MongoDbParameter,
                               fields=fields)

    def _post_render_value(self, render_value):
        post_value = self.deserialize(render_value)
        print "POST RENDER MONGODB VALUE!"
        print type(post_value)
        print post_value
        return post_value

    def execute(self, df=None, **independent_param_vals):
        if self.rendered_query is not None:
            rendered_query = self.rendered_query
        else:
            rendered_query = self.render(df=df, **independent_param_vals)
        df = self.db_connector.execute_query(query=rendered_query, fields=self.fields)
        return df
