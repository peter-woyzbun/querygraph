from abc import ABCMeta, abstractmethod
import re

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