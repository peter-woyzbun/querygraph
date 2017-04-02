import psycopg2
import pandas as pd


from querygraph.db.interface import DatabaseInterface
from querygraph.db.type_converter import TypeConverter


class Postgres(DatabaseInterface):

    TYPE_CONVERTER = TypeConverter(
        type_converters={
            'bool':
                {
                    bool: lambda x: 'TRUE' if x else 'FALSE',
                    int: lambda x: x,
                    str: lambda x: "'%s'" % x
                }
        }
    )

    def __init__(self, name, db_name, user, password, host, port):
        self.host = host
        self.db_name = db_name
        self.user = user
        self.password = password
        self.port = port
        DatabaseInterface.__init__(self,
                                   name=name,
                                   db_type='Postgres',
                                   conn_exception=psycopg2.OperationalError,
                                   execution_exception=psycopg2.DatabaseError,
                                   type_converter=self.TYPE_CONVERTER)

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