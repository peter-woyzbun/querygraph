from querygraph.exceptions import QueryGraphException


class GraphException(QueryGraphException):
    pass


class GraphConfigException(GraphException):
    pass


class CycleException(GraphConfigException):
    pass