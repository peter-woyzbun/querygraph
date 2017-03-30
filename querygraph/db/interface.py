from abc import abstractmethod

from querygraph import exceptions


class DatabaseInterface(object):

    def __init__(self, name, db_type, conn_exception, execution_exception, type_converter, deserialize_query=False):
        self.name = name
        self.db_type = db_type
        self.conn_exception = conn_exception
        self.execution_exception = execution_exception
        self.type_converter = type_converter
        self.deserialize_query = deserialize_query

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
        except self.execution_exception, e:
            raise exceptions.ExecutionError("%s" % e)

    @abstractmethod
    def _execute_query(self, *args, **kwargs):
        pass

    @abstractmethod
    def execute_insert_query(self, *args, **kwargs):
        pass