import redis
import pandas as pd

from querygraph.db.connector import NoSqlDbConnector


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


class RedisConnector(NoSqlDbConnector):

    def __init__(self, name, host, port, db_name, password):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.password = password

        NoSqlDbConnector.__init__(self,
                                  name=name,
                                  db_type='redis',
                                  conn_exception=redis.ConnectionError,
                                  execution_exception=redis.RedisError)

    def _conn(self):
        r = redis.StrictRedis(host=self.host, port=self.port, db=self.db_name)
        return r

    def _execute_query(self, query):
        r = self.conn()
        return pd.read_msgpack(r.get(query))

    def execute_insert_query(self, key, value):
        r = self.conn()
        r.set(key, value)

