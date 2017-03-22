import datetime

from querygraph.template_parameter import TemplateParameter
from querygraph.query_template import QueryTemplate


# =============================================
# Cassandra Template Parameter
# ---------------------------------------------

class CassandraParameter(TemplateParameter):

    def __init__(self, param_str, independent=True):
        TemplateParameter.__init__(self,
                                   param_str=param_str,
                                   independent=independent)

    def _setup_db_specific_converters(self):

        self._add_datetime_converters(
            {datetime.datetime: lambda x: "'%s'" % x.isoformat(' '),
             str: lambda x: "'%s'" % x}
        )


# =============================================
# Cassandra Query Template
# ---------------------------------------------

class CassandraTemplate(QueryTemplate):

    def __init__(self, template_str, db_connector):
        QueryTemplate.__init__(self,
                               template_str=template_str,
                               db_connector=db_connector,
                               parameter_class=CassandraParameter)

    def execute(self, df=None, **independent_param_vals):
        if self.rendered_query is not None:
            rendered_query = self.rendered_query
        else:
            rendered_query = self.render(df=df, **independent_param_vals)
        df = self.db_connector.execute_query(query=rendered_query)
        return df
