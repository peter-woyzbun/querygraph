from elasticsearch import Elasticsearch
import pandas as pd

from querygraph.db.connectors import base


class ElasticSearch(base.DatabaseConnector):

    def __init__(self, host, port, doc_type):
        self.port = port
        self.doc_type = doc_type
        base.DatabaseConnector.__init__(self, database_type='ElasticSearch', host=host)

    def execute_query(self, query):

        es = Elasticsearch([{'host': self.host, 'port': self.port}])

    def insert_entry(self, index, id, data):

        es = Elasticsearch([{'host': self.host, 'port': self.port}])
        es.index(index=index, doc_type=self.doc_type, id=id, body=data)
