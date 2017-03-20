import elasticsearch
import pandas as pd
from pandas.io.json import json_normalize

from querygraph.db.connector import NoSqlDbConnector


class ElasticSearchConnector(NoSqlDbConnector):

    def __init__(self, host, port, doc_type, index):
        self.host = host
        self.port = port
        self.doc_type = doc_type
        self.index = index

        NoSqlDbConnector.__init__(self,
                                  db_type='elasticsearch',
                                  conn_exception=elasticsearch.ConnectionError,
                                  execution_exception=elasticsearch.ElasticsearchException)

    def _conn(self):
        return elasticsearch.Elasticsearch([{'host': self.host, 'port': self.port}])

    def _execute_query(self, query, fields):
        es = self.conn()
        df = json_normalize(es.search(index=self.index, body={"_source": fields, "query": query})['hits']['hits'])
        df.rename(columns={'_source.%s' % field_name: field_name for field_name in fields}, inplace=True)
        df = df[fields]
        return df

    def execute_insert_query(self, id, data):
        es = self.conn()
        es.index(index=self.index, doc_type=self.doc_type, id=id, body=data)