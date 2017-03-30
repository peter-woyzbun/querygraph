import datetime

import elasticsearch
import pandas as pd
from pandas.io.json import json_normalize

from querygraph.db.interface import DatabaseInterface
from querygraph.db.type_converter import TypeConverter


class ElasticSearch(DatabaseInterface):

    # ElasticSearch specific type converters...
    TYPE_CONVERTER = TypeConverter()

    def __init__(self, name, host, port, doc_type, index):
        self.host = host
        self.port = port
        self.doc_type = doc_type
        self.index = index
        DatabaseInterface.__init__(self,
                                   name=name,
                                   db_type='ElasticSearch',
                                   conn_exception=elasticsearch.ConnectionError,
                                   execution_exception=elasticsearch.ElasticsearchException,
                                   type_converter=self.TYPE_CONVERTER,
                                   deserialize_query=True)

    def _conn(self):
        return elasticsearch.Elasticsearch([{'host': self.host, 'port': int(self.port)}])

    def _execute_query(self, query, fields):
        es = self.conn()
        df = json_normalize(es.search(index=self.index, body={"query": query, "_source": fields})['hits']['hits'])
        df.rename(columns={'_source.%s' % field_name: field_name for field_name in fields}, inplace=True)
        return df[fields]

    def execute_insert_query(self, id, data):
        es = self.conn()
        es.index(index=self.index, doc_type=self.doc_type, id=id, body=data)