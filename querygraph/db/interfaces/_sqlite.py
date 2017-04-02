import sqlite3
import pandas as pd


from querygraph.db.interface import DatabaseInterface
from querygraph.db.type_converter import TypeConverter


class Sqlite(DatabaseInterface):

    TYPE_CONVERTER = TypeConverter(
        type_converters={
            'bool':
                {
                    bool: lambda x: 1 if x else 0,
                    int: lambda x: x,
                    str: lambda x: x
                }
        }
    )

    def __init__(self, name, host):
        self.host = host
        DatabaseInterface.__init__(self,
                                   name=name,
                                   db_type='Sqlite',
                                   conn_exception=Exception,
                                   execution_exception=sqlite3.OperationalError,
                                   type_converter=self.TYPE_CONVERTER)

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
