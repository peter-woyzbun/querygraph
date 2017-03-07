from abc import ABCMeta, abstractmethod

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


class MySQL(DatabaseConnector):

    def __init__(self, host, db_name, user, password):
        self.db_name = db_name
        self.user = user
        self.password = password
        DatabaseConnector.__init__(self, database_type='MySQL', host=host)

    def execute_query(self, query):
        conn = mysql.connector.connect(user=self.user, password=self.password,
                                       host=self.host,
                                       database=self.db_name)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df


class Postgres(DatabaseConnector):

    def __init__(self, host, port, db_name, user, password):
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        DatabaseConnector.__init__(self, database_type='Postgres', host=host)

    def execute_query(self, query):
        conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (self.db_name,
                                                                                   self.user,
                                                                                   self.host,
                                                                                   self.password))
        df = pd.read_sql_query(query, con=conn)
        return df


class MsSQL(DatabaseConnector):

    def __init__(self, host, db_name, user, password):
        self.db_name = db_name
        self.user = user
        self.password = password
        DatabaseConnector.__init__(self, database_type='MsSQL', host=host)

    def execute_query(self, query):
        conn = pymssql.connect(self.host, self.user, self.password, self.db_name)
        df = pd.read_sql_query(query, con=conn)
        return df


class MongoDb(DatabaseConnector):

    def __init__(self, host, db_name, collection):
        self.db_name = db_name
        self.collection = collection
        DatabaseConnector.__init__(self, database_type='Mongodb', host=host, db_name=db_name)

    def execute_query(self, query, **kwargs):

        from pymongo import MongoClient

        fields = kwargs.get('fields')
        projection_fields = {k: 1 for k in fields}
        client = MongoClient()
        db = client[self.db_name]
        collection = db[self.collection]
        results = collection.find(query, projection_fields)
        df = pd.DataFrame(list(results))
        return df


# =================================================
# TEST CONNECTOR
# -------------------------------------------------

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