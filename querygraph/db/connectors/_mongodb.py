import pymongo
from pymongo import errors
import pandas as pd

from querygraph.db.connector import NoSqlDbConnector


class MongoDbConnector(NoSqlDbConnector):

    def __init__(self, name, host, port, db_name, collection):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection = collection

        NoSqlDbConnector.__init__(self,
                                  name=name,
                                  db_type='mongodb',
                                  conn_exception=pymongo.errors.ConnectionFailure,
                                  execution_exception=pymongo.errors.OperationFailure)

    def _conn(self):
        return pymongo.MongoClient(host=self.host, port=int(self.port))

    def _execute_query(self, query, fields):
        client = self.conn()
        db = client[self.db_name]
        collection = db[self.collection]
        projection_fields = {k: 1 for k in fields}
        results = collection.find(query, projection_fields)
        df = pd.DataFrame(list(results))
        return df

    def execute_insert_query(self, data):
        client = self.conn()
        db = client[self.db_name]
        collection = db[self.collection]
        result = collection.insert_many(data)