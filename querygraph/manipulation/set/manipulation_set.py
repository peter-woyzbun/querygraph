from abc import ABCMeta, abstractmethod


from querygraph.exceptions import QueryGraphException
from querygraph.manipulation.expression.evaluator import Evaluator


# =============================================
# Manipulation Exceptions
# ---------------------------------------------

class ManipulationException(QueryGraphException):
    pass


class ManipulationSetException(QueryGraphException):
    pass


class ConfigurationException(ManipulationException):
    pass


# =============================================
# Manipulation Abstract Base Class
# ---------------------------------------------

class Manipulation(object):

    __metaclass__ = ABCMeta

    def __call__(self, df, evaluator=None):
        return self.execute(df, evaluator)

    def execute(self, df, evaluator=None):
        return self._execute(df, evaluator)

    @abstractmethod
    def _execute(self, df, evaluator=None):
        pass


# =============================================
# Manipulation Types
# ---------------------------------------------

class Mutate(Manipulation):

    def __init__(self, col_name, col_expr):
        self.col_name = col_name
        self.col_expr = col_expr

    def _execute(self, df, evaluator=None):
        if not isinstance(evaluator, Evaluator):
            raise ManipulationException
        expr_evaluator = Evaluator(df=df)
        df[self.col_name] = expr_evaluator.eval(expr_str=self.col_expr)
        return df


class Rename(Manipulation):

    def __init__(self, old_column_name, new_column_name):
        self.old_column_name = old_column_name
        self.new_column_name = new_column_name

    def _execute(self, df, evaluator=None):
        df = df.rename(columns={self.old_column_name: self.new_column_name})
        return df


# =============================================
# Manipulation Set
# ---------------------------------------------


class ManipulationSet(object):

    def __init__(self):
        self.manipulations = list()

    def __add__(self, other):
        if not isinstance(other, Manipulation):
            raise ConfigurationException("Only Manipulation instances can be added to ManipulationSet.")
        else:
            self.manipulations.append(other)
        return self

    __radd__ = __add__

    def __iter__(self):
        for x in self.manipulations:
            yield x

    def __nonzero__(self):
        if len(self.manipulations) > 0:
            return True
        else:
            return False

    def execute(self, df):
        print "Executing manipulation set!"
        evaluator = Evaluator()
        for manipulation in self:
            df = manipulation.execute(df, evaluator)
        return df
