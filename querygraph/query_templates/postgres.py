import datetime

from querygraph.template_parameter import TemplateParameter
from querygraph.query_template import QueryTemplate


# =============================================
# Postgres Template Parameter
# ---------------------------------------------

class PostgresParameter(TemplateParameter):

    def __init__(self, parameter_str, parameter_type):
        TemplateParameter.__init__(self,
                                   parameter_str=parameter_str,
                                   parameter_type=parameter_type)

        self.type_converter.add_datetime_converter(input_type=datetime.datetime,
                                                   converter=lambda x: "'%s'" % x.strftime('%Y-%m-%d %H:%M:%S'))

    def _setup_db_specific_converters(self):

        self.type_converter.add_datetime_converters(
            {datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d %H:%M:%S'),
             str: lambda x: "'%s'" % x}
        )

        self.type_converter.add_date_converters(
            {datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
             str: lambda x: "'%s'" % x}
        )

        self.type_converter.add_time_converters(
            {datetime.datetime: lambda x: "'%s'" % x.strftime('%H:%M:%S'),
             str: lambda x: "'%s'" % x}
        )


# =============================================
# Postgres Template Parameter
# ---------------------------------------------

class PostgresTemplate(QueryTemplate):

    def __init__(self, template_str, db_connector):
        QueryTemplate.__init__(self,
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
