from _mysql import MySql


class MariaDb(MySql):

    db_type = 'Maria DB'

    def __init__(self, name, db_name, user, password, host, port):
        MySql.__init__(self,
                       name=name,
                       db_name=db_name,
                       user=user,
                       password=password,
                       host=host,
                       port=port)

