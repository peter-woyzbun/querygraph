from abc import abstractmethod

from querygraph import exceptions
from querygraph.utils.deserializer import Deserializer


class DatabaseInterface(object):

    def __init__(self,
                 name,
                 db_type,
                 conn_exception,
                 execution_exception,
                 type_converter,
                 fields_accepted=False,
                 deserialize_query=False):
        self.name = name
        self.db_type = db_type
        self.conn_exception = conn_exception
        self.execution_exception = execution_exception
        self.type_converter = type_converter
        self.fields_accepted = fields_accepted
        self.deserialize_query = deserialize_query
        self.deserialize = Deserializer()

    def conn(self):
        try:
            return self._conn()
        except self.conn_exception:
            raise exceptions.ConnectionError

    @abstractmethod
    def _conn(self):
        pass

    def execute_query(self, query, *args, **kwargs):
        try:
            if self.deserialize_query:
                query = self.deserialize(query)
            return self._execute_query(query, *args, **kwargs)
        except self.execution_exception, e:
            raise exceptions.ExecutionError("%s" % e)

    @abstractmethod
    def _execute_query(self, *args, **kwargs):
        pass

    @abstractmethod
    def execute_insert_query(self, *args, **kwargs):
        pass