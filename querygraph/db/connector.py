from abc import abstractmethod

from querygraph import exceptions


# =================================================
# Base Database Connector Class
# -------------------------------------------------

class DbConnector(object):
    """
    Base class for all database connectors. It provides a common
    interface for:

        (1) Catching 'connection' exceptions - errors connecting
            to databases.
        (2) Catching 'execution' exceptions - errors executing
            queries on databases.

    The goal...


    Parameters
    ----------

    db_type : str
        A string indicating the database type (e.g. 'mysql').
    conn_exception : Exception
        For the given connector, the exception that will be raised by the
        associated connector package (e.g. 'psycopg2' is the connector package
        for Postgres) in the event of a database connector error.
    execution_exception : Exception
        For the given connector, the exception that will be raised by the
        associated connector package in the event of an error executing
        a query.


    """

    def __init__(self, db_type, conn_exception, execution_exception):
        self.db_type = db_type
        self.conn_exception = conn_exception
        self.execution_exception = execution_exception

    def conn(self):
        try:
            return self._conn()
        except self.conn_exception:
            raise exceptions.ConnectionError

    @abstractmethod
    def _conn(self):
        pass

    def execute_query(self, *args, **kwargs):
        try:
            return self._execute_query(*args, **kwargs)
        except self.execution_exception:
            raise exceptions.ExecutionError

    @abstractmethod
    def _execute_query(self, *args, **kwargs):
        pass

    @abstractmethod
    def execute_insert_query(self, *args, **kwargs):
        pass


# =================================================
# Relational Database Connector Class
# -------------------------------------------------

class RelationalDbConnector(DbConnector):

    def __init__(self, db_type,
                 conn_exception,
                 execution_exception,
                 host=None,
                 db_name=None,
                 user=None,
                 password=None,
                 port=None):
        self.host = host
        self.db_name = db_name
        self.user = user
        self.password = password
        self.port = port
        DbConnector.__init__(self,
                             db_type=db_type,
                             conn_exception=conn_exception,
                             execution_exception=execution_exception)

    @abstractmethod
    def _conn(self):
        pass

    @abstractmethod
    def _execute_query(self, *args, **kwargs):
        pass

    @abstractmethod
    def execute_insert_query(self, *args, **kwargs):
        pass


# =================================================
# NoSQL Database Connector Class
# -------------------------------------------------

class NoSqlDbConnector(DbConnector):

    def __init__(self, db_type,
                 conn_exception,
                 execution_exception):
        DbConnector.__init__(self,
                             db_type=db_type,
                             conn_exception=conn_exception,
                             execution_exception=execution_exception)

    @abstractmethod
    def _conn(self):
        pass

    @abstractmethod
    def _execute_query(self, *args, **kwargs):
        pass

    @abstractmethod
    def execute_insert_query(self, *args, **kwargs):
        pass
