import psycopg2
import pandas as pd

from querygraph.db.connectors import base


class Postgres(base.DatabaseConnector):

    def __init__(self, host, port, db_name, user, password):
        self.port = port
        base.DatabaseConnector.__init__(self, database_type='Postgres',
                                   host=host,
                                   user=user,
                                   password=password,
                                   db_name=db_name)

    def execute_query(self, query):
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (self.db_name,
                                                                                   self.user,
                                                                                   self.host,
                                                                                   self.password))
        df = pd.read_sql_query(query, con=conn)
        return df

    def execute_write_query(self, query, latin_encoding=False):
        conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s port=%s" % (self.db_name,
                                                                                   self.user,
                                                                                   self.host,
                                                                                   self.password,
                                                                                   self.port))
        if latin_encoding:
            conn.set_client_encoding('Latin1')
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()