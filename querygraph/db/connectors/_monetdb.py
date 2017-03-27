import pymonetdb
import pandas as pd

from querygraph.db.connector import RelationalDbConnector


class MonetDbConnector(RelationalDbConnector):

    def __init__(self, name, host, db_name, user, password):
        RelationalDbConnector.__init__(self,
                                       name=name,
                                       host=host,
                                       db_type='monetdb',
                                       db_name=db_name,
                                       user=user,
                                       password=password,
                                       execution_exception=pymonetdb.exceptions.ProgrammingError,
                                       conn_exception=pymonetdb.exceptions.DatabaseError)

    def _conn(self):
        return pymonetdb.connect(username=self.user, password=self.password, hostname=self.host, database=self.db_name)

    def _execute_query(self, query):
        conn = self.conn()
        cursor = conn.cursor()
        n_results = cursor.execute(query)
        col_names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=col_names)
        return df

    def execute_insert_query(self, query):
        conn = self.conn()
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
