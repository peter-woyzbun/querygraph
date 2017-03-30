import datetime

import pymongo
from pymongo import errors
import pandas as pd

from querygraph.db.interface import DatabaseInterface
from querygraph.db.type_converter import TypeConverter


class MongoDb(DatabaseInterface):

    # Mongo DB specific type converters...
    TYPE_CONVERTER = TypeConverter(
        {
            'datetime':
                {
                    datetime.datetime: lambda x: repr(x),
                    datetime.date: lambda x: repr(x),
                    datetime.time: lambda x: repr(x)
                }
        }
    )

    def __init__(self, name, host, port, db_name, collection):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection = collection
        DatabaseInterface.__init__(self,
                                   name=name,
                                   db_type='Mongo DB',
                                   conn_exception=pymongo.errors.ConnectionFailure,
                                   execution_exception=pymongo.errors.OperationFailure,
                                   type_converter=self.TYPE_CONVERTER,
                                   deserialize_query=True)

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