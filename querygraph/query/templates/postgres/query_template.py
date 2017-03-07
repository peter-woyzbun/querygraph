import datetime

from querygraph.query.templates.base_parameter import BaseTemplateParameter
from querygraph.query.templates.base_template import BaseQueryTemplate


# =============================================
# Postgres Template Parameter
# ---------------------------------------------

class PostgresParameter(BaseTemplateParameter):

    CHILD_DATA_TYPES = {
        'datetime': {datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d %H:%M:%S'),
                     str: lambda x: "'%s'" % x},
        'date': {datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
                 str: lambda x: "'%s'" % x},
        'time': {datetime.datetime: lambda x: "'%s'" % x.strftime('%H:%M:%S'),
                 str: lambda x: "'%s'" % x}
    }

    def __init__(self, parameter_str, parameter_type):
        BaseTemplateParameter.__init__(self,
                                       parameter_str=parameter_str,
                                       parameter_type=parameter_type)


# =============================================
# Postgres Template Parameter
# ---------------------------------------------

class QueryTemplate(BaseQueryTemplate):

    def __init__(self, template_str, db_connector):
        BaseQueryTemplate.__init__(self,
                                   template_str=template_str,
                                   db_connector=db_connector,
                                   parameter_class=PostgresParameter)

    def execute(self, df=None, **independent_param_vals):
        if self.rendered_query is not None:
            rendered_query = self.rendered_query
        else:
            rendered_query = self.render(df=df, **independent_param_vals)
        df = self.db_connector.execute_query(query=rendered_query)
        return df