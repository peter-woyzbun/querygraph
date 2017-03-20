

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
# Parameter Exceptions
# -------------------------------------------------

class ParameterException(QueryGraphException):
    pass


class ParameterConfigException(ParameterException):
    pass

class TypeConversionError(QueryGraphException):
    pass