import datetime


from querygraph.query.template import QueryTemplate
from querygraph.query.db_types.postgres.template_parameter import PostgresParameter


class PostgresTemplate(QueryTemplate):

    def __init__(self, template_str, db_connector):
        QueryTemplate.__init__(self,
                               template_str=template_str,
                               db_connector=db_connector,
                               parameter_class=PostgresParameter)