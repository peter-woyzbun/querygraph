import mysql.connector
import pandas as pd

from querygraph.db.connectors import base


class MySQL(base.DatabaseConnector):
    def __init__(self, host, db_name, user, password):
        base.DatabaseConnector.__init__(self, database_type='MySql', host=host,
                                        db_name=db_name, user=user, password=password)

    def execute_query(self, query):
        conn = mysql.connector.connect(user=self.user, password=self.password,
                                       host=self.host,
                                       database=self.db_name)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def execute_write_query(self, query, latin_encoding=False):
        if latin_encoding:
            conn = mysql.connector.connect(user=self.user, password=self.password,
                                           host=self.host,
                                           database=self.db_name,
                                           charset='latin1')
        else:
            conn = mysql.connector.connect(user=self.user, password=self.password,
                                           host=self.host,
                                           database=self.db_name)

        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.close()
