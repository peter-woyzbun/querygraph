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

    @staticmethod
    def df_contains_column(df, column_name):
        return column_name in list(df.columns.values)

    def _column_check(self, parent_df, child_df):
        for col in self.parent_cols:
            if not self.df_contains_column(parent_df, col):
                raise JoinContextException("Column does not exist.")
        for col in self.child_cols:
            if not self.df_contains_column(child_df, col):
                raise JoinContextException("Column does not exist.")

    def apply_join(self, parent_df, child_df):
        self._column_check(parent_df, child_df)
        joined_df = parent_df.merge(child_df, how=self.join_type, left_on=self.parent_cols, right_on=self.child_cols)
        return joined_df