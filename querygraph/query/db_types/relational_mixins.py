


class RelationalTemplateMixin(object):

    @staticmethod
    def execute(rendered_query, db_connector):

        df = db_connector.execute_query(query=rendered_query)
        return df