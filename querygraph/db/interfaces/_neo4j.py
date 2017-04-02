import pandas as pd
import py2neo

from querygraph.db.interface import DatabaseInterface
from querygraph.db.type_converter import TypeConverter


class Neo4j(DatabaseInterface):

    TYPE_CONVERTER = TypeConverter()

    def __init__(self, name, host, user, password):
        self.host = host
        self.user = user
        self.password = password

        DatabaseInterface.__init__(self,
                                   name=name,
                                   db_type='Neo4j',
                                   conn_exception=py2neo.database.status.Unauthorized,
                                   execution_exception=py2neo.database.status.DatabaseError,
                                   type_converter=self.TYPE_CONVERTER)

    def _conn(self):
        return py2neo.Graph(bolt=True, host=self.host, user=self.user, password=self.password)

    def _execute_query(self, query):
        graph = self.conn()
        df = pd.DataFrame(graph.data(query))
        return df

    def execute_insert_query(self, query):
        graph = self.conn()
        graph.run(query)
