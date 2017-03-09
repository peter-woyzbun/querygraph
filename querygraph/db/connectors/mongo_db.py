from pymongo import MongoClient
import pandas as pd

from querygraph.db.connectors import base


class MongoDb(base.DatabaseConnector):

    def __init__(self, host, port, db_name, collection):
        self.port = port
        self.collection = collection
        base.DatabaseConnector.__init__(self, database_type='Mongodb', host=host, db_name=db_name)

    def execute_query(self, query, **kwargs):

        fields = kwargs.get('fields')
        projection_fields = {k: 1 for k in fields}
        client = MongoClient(self.host, self.port)
        db = client[self.db_name]
        collection = db[self.collection]
        results = collection.find(query, projection_fields)
        df = pd.DataFrame(list(results))
        return df

    def insert_many(self, data):

        client = MongoClient(self.host, self.port)
        db = client[self.db_name]
        collection = db[self.collection]

        result = collection.insert_many(data)