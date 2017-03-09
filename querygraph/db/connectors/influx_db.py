

from querygraph.db.connectors import base


class InfluxDb(base.DatabaseConnector):

    def __init__(self, host, port, user, password, db_name):
        self.port = port
        base.DatabaseConnector.__init__(self, database_type='InfluxDb',
                                   host=host, db_name=db_name, user=user, password=password)