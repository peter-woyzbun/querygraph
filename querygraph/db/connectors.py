from abc import ABCMeta, abstractmethod
import re

import sqlite3
import pandas as pd
import psycopg2
import pymssql
import mysql.connector

from querygraph.exceptions import QueryGraphException


# =================================================
# Exceptions
# -------------------------------------------------

class ConnectorError(QueryGraphException):
    pass


class ConnectorImportError(ConnectorError):
    pass


# =================================================
# DATABASE CONNECTOR BASE CLASS
# -------------------------------------------------

class DatabaseConnector(object):

    def __init__(self, database_type, db_name=None, host=None, user=None, password=None):
        """
        ...

        :param database_type:
        :param connector:
        :param db_name:
        :param host:
        :param user:
        :param password:
        """
        self.database_type = database_type
        self.db_name = db_name
        self.host = host
        self.user = user
        self.password = password

    @abstractmethod
    def execute_query(self, query):
        """ Execute query and return dataframe or ...  """


# =================================================
# DATABASE CONNECTOR CHILD CLASSES
# -------------------------------------------------

class SQLite(DatabaseConnector):

    def __init__(self, host):
        """
        Connector for SQLite databases.

        :param host: name of SQLite database file.
        """

        DatabaseConnector.__init__(self, database_type='SQLite', host=host)

    def execute_query(self, query):
        """ See abstract base class method docs. """
        connector = sqlite3.connect('%s' % self.host)
        # cursor = connector.cursor()
        df = pd.read_sql_query(query, connector)
        connector.close()
        return df

    def execute_write_query(self, query):
        connector = sqlite3.connect('%s' % self.host)
        c = connector.cursor()
        c.execute(query)
        connector.commit()
        c.close()
        connector.close()


class MySQL(DatabaseConnector):

    def __init__(self, host, db_name, user, password):
        DatabaseConnector.__init__(self, database_type='MySql', host=host,
                                   db_name=db_name, user=user, password=password)

    def execute_query(self, query):
        print "executed mysql query!"
        conn = mysql.connector.connect(user=self.user, password=self.password,
                                       host=self.host,
                                       database=self.db_name)
        df = pd.read_sql_query(query, conn)
        print df
        conn.close()
        return df

    def execute_write_query(self, query, latin_encoding=False):
        if latin_encoding:
            conn = mysql.connector.connect(user=self.user, password=self.password,
                                           host=self.host,
                                           database=self.db_name,
                                           charset='latin1')
        else:
            conn = mysql.connector.connect(user=self.user, password=self.password,
                                           host=self.host,
                                           database=self.db_name)

        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
        conn.close()


class Postgres(DatabaseConnector):

    def __init__(self, host, port, db_name, user, password):
        self.port = port
        DatabaseConnector.__init__(self, database_type='Postgres',
                                   host=host,
                                   user=user,
                                   password=password,
                                   db_name=db_name)

    def execute_query(self, query):
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (self.db_name,
                                                                                   self.user,
                                                                                   self.host,
                                                                                   self.password))
        df = pd.read_sql_query(query, con=conn)
        return df

    def execute_write_query(self, query, latin_encoding=False):
        conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s port=%s" % (self.db_name,
                                                                                   self.user,
                                                                                   self.host,
                                                                                   self.password,
                                                                                   self.port))
        if latin_encoding:
            conn.set_client_encoding('Latin1')
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()


class MongoDb(DatabaseConnector):

    def __init__(self, host, port, db_name, collection):
        self.port = port
        self.collection = collection
        DatabaseConnector.__init__(self, database_type='Mongodb', host=host, db_name=db_name)

    def execute_query(self, query, **kwargs):
        from pymongo import MongoClient

        fields = kwargs.get('fields')
        projection_fields = {k: 1 for k in fields}
        client = MongoClient(self.host, self.port)
        db = client[self.db_name]
        collection = db[self.collection]
        results = collection.find(query, projection_fields)
        df = pd.DataFrame(list(results))
        return df

    def insert_many(self, data):
        from pymongo import MongoClient

        client = MongoClient(self.host, self.port)
        db = client[self.db_name]
        collection = db[self.collection]

        result = collection.insert_many(data)


class ElasticSearch(DatabaseConnector):

    def __init__(self, host, port, doc_type):
        self.port = port
        self.doc_type = doc_type
        DatabaseConnector.__init__(self, database_type='ElasticSearch', host=host)

    def execute_query(self, query):

        from elasticsearch import Elasticsearch

        es = Elasticsearch([{'host': self.host, 'port': self.port}])

    def insert_entry(self, index, id, data):

        from elasticsearch import Elasticsearch

        es = Elasticsearch([{'host': self.host, 'port': self.port}])
        es.index(index=index, doc_type=self.doc_type, id=id, body=data)


class InfluxDb(DatabaseConnector):

    def __init__(self, host, port, user, password, db_name):
        self.port = port
        DatabaseConnector.__init__(self, database_type='InfluxDb',
                                   host=host, db_name=db_name, user=user, password=password)


# =================================================
# TEST CONNECTOR
# -------------------------------------------------


class NullConnector(DatabaseConnector):

    def __init__(self):
        DatabaseConnector.__init__(self, database_type='null')

    def execute_query(self, query):
        return None


class TestConnector(DatabaseConnector):

    def __call__(self):
        return SQLite(host='')

    @classmethod
    def sqlite(cls):
        return SQLite(host='')

    @classmethod
    def mysql(cls):
        return MySQL(host='', db_name='', user='', password='')

    @classmethod
    def postgres(cls):
        return Postgres(host='', db_name='', user='', password='', port='')

    def execute_query(self, query):
        return None