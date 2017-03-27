import sqlite3
import pandas as pd

from querygraph.db.connector import RelationalDbConnector


class SqliteConnector(RelationalDbConnector):

    def __init__(self, name, host):
        RelationalDbConnector.__init__(self,
                                       name=name,
                                       host=host,
                                       db_type='sqlite',
                                       conn_exception=Exception,
                                       execution_exception=sqlite3.OperationalError)

    def _conn(self):
        return sqlite3.connect(self.host)

    def _execute_query(self, query):
        connector = self.conn()
        df = pd.read_sql_query(query, connector)
        connector.close()
        return df

    def execute_insert_query(self, query):
        connector = self.conn()
        c = connector.cursor()
        c.execute(query)
        connector.commit()
        c.close()
        connector.close()
        