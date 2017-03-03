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

class Create(Manipulation):

    def __init__(self, new_col_name, new_col_expression):
        self.new_col_name = new_col_name
        self.new_col_expression = new_col_expression

    def _execute(self, df, evaluator=None):
        if not isinstance(evaluator, Evaluator):
            raise ManipulationException
        df[self.new_col_name] = evaluator.eval(df=df, eval_str=self.new_col_expression)
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
