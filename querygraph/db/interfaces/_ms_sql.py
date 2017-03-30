import pandas as pd
import pymssql

from querygraph.db.interface import DatabaseInterface
from querygraph.db.type_converter import TypeConverter


class MsSql(DatabaseInterface):

    def __init__(self, name, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        DatabaseInterface.__init__(self,
                                   name=name,
                                   db_type='MS Sql',
                                   conn_exception=Exception,
                                   execution_exception=Exception,
                                   type_converter=TypeConverter())

    def _conn(self):
        conn = pymssql.connect(server=self.host, user=self.user, password=self.password, port=self.port)
        return conn

    def _execute_query(self, query):
        conn = self.conn()
        df = pd.read_sql(query, conn)
        return df

    def execute_insert_query(self, *args, **kwargs):
        pass