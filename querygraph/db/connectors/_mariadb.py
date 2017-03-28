

from _mysql import MySqlConnector


class MariaDbConnector(MySqlConnector):

    def __init__(self, name, db_name, host, port, user, password):
        MySqlConnector.__init__(self,
                                name=name,
                                db_name=db_name,
                                user=user,
                                host=host,
                                port=port,
                                password=password)

