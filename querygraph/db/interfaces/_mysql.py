import mysql.connector
import pandas as pd


from querygraph.db.interface import DatabaseInterface
from querygraph.db.type_converter import TypeConverter


class MySql(DatabaseInterface):

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
                                   db_type='MySql',
                                   conn_exception=mysql.connector.DatabaseError,
                                   execution_exception=mysql.connector.ProgrammingError,
                                   type_converter=TypeConverter())

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