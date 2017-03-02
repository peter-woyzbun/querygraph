from querygraph.exceptions import QueryGraphException


class JoinContextException(QueryGraphException):
    pass


class JoinContext(object):

    def __init__(self):
        self.parent_cols = list()
        self.child_cols = list()
        self._join_type = None

    @property
    def join_type(self):
        return self._join_type

    @join_type.setter
    def join_type(self, value):
        if value not in ('inner', 'left', 'right', 'outer'):
            raise JoinContextException("Tried to set join context join type to invalid join type: '%s'." % value)
        else:
            self._join_type = value

    def add_on_column_pair(self, parent_col_name, child_col_name):
        self.parent_cols.append(parent_col_name)
        self.child_cols.append(child_col_name)