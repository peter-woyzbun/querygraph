

# =================================================
# Base Exception
# -------------------------------------------------

class QueryGraphException(Exception):
    pass


# =================================================
# Database Connector Exceptions
# -------------------------------------------------

class DatabaseError(QueryGraphException):
    pass


class ConnectionError(DatabaseError):
    pass


class ExecutionError(DatabaseError):
    pass

# =================================================
# Type Converter Exceptions
# -------------------------------------------------

class TypeConverterException(QueryGraphException):
    pass


class TypeConversionError(TypeConverterException):
    pass


# =================================================
# Graph Exceptions
# -------------------------------------------------

class GraphException(QueryGraphException):
    pass


class GraphConfigException(GraphException):
    pass


class CycleException(GraphConfigException):
    pass


class DisconnectedNodes(GraphException):
    pass


# =================================================
# Join Context Exceptions
# -------------------------------------------------

class JoinContextException(QueryGraphException):
    pass


# =================================================
# Query Template Exceptions
# -------------------------------------------------

class QueryTemplateError(QueryGraphException):
    pass


class MissingDataError(QueryTemplateError):
    pass


class ParameterException(QueryGraphException):
    pass


class ParameterRenderError(ParameterException):
    pass


class ParameterError(QueryTemplateError):

    def __init__(self, param_str, msg):
        self.param_str = param_str
        self.pre_msg = msg
        QueryTemplateError.__init__(self, msg=self._make_msg())

    def _make_msg(self):
        msg = "Parameter error for parameter string '%s': \n %s" % (self.param_str, self.pre_msg)
        return msg


# =================================================
# Parameter Exceptions
# -------------------------------------------------

class ParameterException(QueryGraphException):
    pass


class ParameterConfigException(ParameterException):
    pass
