from cassandra.cluster import Cluster
from cassandra.connection import ConnectionException
from cassandra import ReadFailure
import pandas as pd

from querygraph.db.connector import NoSqlDbConnector


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


class CassandraConnector(NoSqlDbConnector):

    def __init__(self, name, contact_point, port, keyspace):
        self.contact_point = contact_point
        self.port = port
        self.keyspace = keyspace

        NoSqlDbConnector.__init__(self,
                                  name=name,
                                  db_type='cassandra',
                                  conn_exception=ConnectionException,
                                  execution_exception=ReadFailure)

    def _conn(self):
        cluster = Cluster([self.contact_point])
        session = cluster.connect(self.keyspace)
        session.row_factory = pandas_factory
        session.default_fetch_size = None
        return session

    def _execute_query(self, query):
        session = self.conn()
        rows = session.execute(query)
        df = rows._current_rows
        return df

    def execute_insert_query(self, query):
        session = self.conn()
        session.execute(query)

