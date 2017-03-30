import datetime

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.connection import ConnectionException
from cassandra import ReadFailure

from querygraph.db.interface import DatabaseInterface
from querygraph.db.type_converter import TypeConverter


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


class Cassandra(DatabaseInterface):

    TYPE_CONVERTER = TypeConverter(
        type_converters={
            'datetime': {
                datetime.datetime: lambda x: "'%s'" % x.isoformat(' '),
                str: lambda x: "'%s'" % x
            }
        }
    )

    def __init__(self, name, contact_point, port, keyspace):
        self.contact_point = contact_point
        self.port = port
        self.keyspace = keyspace
        DatabaseInterface.__init__(self,
                                   name=name,
                                   db_type='Apache Cassandra',
                                   conn_exception=ConnectionException,
                                   execution_exception=ReadFailure,
                                   type_converter=self.TYPE_CONVERTER)

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