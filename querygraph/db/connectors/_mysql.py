import mysql.connector
import pandas as pd

from querygraph.db.connector import RelationalDbConnector


class MySqlConnector(RelationalDbConnector):

    def __init__(self, db_name, user, password, host, port):
        RelationalDbConnector.__init__(self,
                                       host=host,
                                       db_name=db_name,
                                       user=user,
                                       password=password,
                                       port=port,
                                       db_type='mysql',
                                       conn_exception=mysql.connector.DatabaseError,
                                       execution_exception=mysql.connector.ProgrammingError)

    def _conn(self):
        return mysql.connector.connect(user=self.user, password=self.password,
                                       host=self.host,
                                       database=self.db_name)

    def _execute_query(self, query):
        connector = self.conn()
        df = pd.read_sql_query(query, connector)
        connector.close()
        return df

    def execute_insert_query(self, query):
        conn = self.conn()
        cur = conn.cursor()
        cur.execute(query)
        cur.close()
        conn.close()