from elasticsearch import Elasticsearch
import pandas as pd

from querygraph.db.connectors import base


class ElasticSearch(base.DatabaseConnector):

    def __init__(self, host, port, doc_type, index):
        self.port = port
        self.doc_type = doc_type
        self.index = index
        base.DatabaseConnector.__init__(self, database_type='ElasticSearch', host=host)

    def execute_query(self, query, fields):

        es = Elasticsearch([{'host': self.host, 'port': self.port}])
        return es.search(index=self.index, body={"_source": fields, "query": query})['hits']['hits']

    def insert_entry(self, id, data):

        es = Elasticsearch([{'host': self.host, 'port': self.port}])
        es.index(index=self.index, doc_type=self.doc_type, id=id, body=data)
