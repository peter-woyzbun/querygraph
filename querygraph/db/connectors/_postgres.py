import psycopg2
import pandas as pd

from querygraph.db.connector import RelationalDbConnector


class PostgresConnector(RelationalDbConnector):
    def __init__(self, db_name, user, password, host, port):
        RelationalDbConnector.__init__(self,
                                       host=host,
                                       db_name=db_name,
                                       user=user,
                                       password=password,
                                       port=port,
                                       db_type='postgres',
                                       conn_exception=psycopg2.OperationalError,
                                       execution_exception=psycopg2.DatabaseError)

    def _conn(self):
        return psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s' port='%s'" % (self.db_name,
                                                                                             self.user,
                                                                                             self.host,
                                                                                             self.password,
                                                                                             self.port))

    def _execute_query(self, query):
        connector = self.conn()
        df = pd.read_sql_query(query, connector)
        connector.close()
        return df

    def execute_insert_query(self, query):
        conn = self.conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
