from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np


# function_groups = vars()['EvalFuncGroup'].__subclasses__()


# =============================================
# Eval Function Base Class
# ---------------------------------------------

class EvalFunc(object):

    __metaclass__ = ABCMeta

    def __call__(self, *args, **kwargs):
        return self._execute(*args, **kwargs)

    @abstractmethod
    def _execute(self, *args, **kwargs):
        raise NotImplementedError


# =============================================
# Eval Function Group Base Class
# ---------------------------------------------

class EvalFuncGroup(object):

    def __init__(self, group_label):
        self.group_label = group_label


# =============================================
# String Functions
# ---------------------------------------------

class Combine(EvalFunc):

    def _execute(self, *args):
        return reduce(self._combine, args)

    @staticmethod
    def _combine(a, b):
        if isinstance(a, pd.Series):
            a = a.astype(str)
        if isinstance(b, pd.Series):
            b = b.astype(str)
        return a + b


class StringFuncs(EvalFuncGroup):

    LABEL = "string"

    def __init__(self):
        self.combine = Combine()
        super(StringFuncs, self).__init__(group_label='string')


# =============================================
# Math Functions
# ---------------------------------------------

class Round(EvalFunc):

    def _execute(self, col):
        return np.round(col)


class Max(EvalFunc):

    def _execute(self, col):
        return max(col)


class Min(EvalFunc):

    def _execute(self, col):
        return min(col)


class Floor(EvalFunc):

    def _execute(self, col):
        return np.floor(col)


class Ceiling(EvalFunc):

    def _execute(self, col):
        return np.ceil(col)


class CumSum(EvalFunc):

    def _execute(self, col):
        return np.cumsum(col)


class Sum(EvalFunc):

    def _execute(self, col):
        return sum(col)


class CumProd(EvalFunc):

    def _execute(self, col):
        return np.cumprod(col)


class MathFuncs(EvalFuncGroup):

    LABEL = "m"

    def __init__(self):
        self.round = Round()
        self.max = Max()
        self.min = Min()
        self.floor = Floor()
        self.ceil = Ceiling()
        self.cumsum = CumSum()
        self.sum = Sum()
        self.cumprod = CumProd()
        super(MathFuncs, self).__init__(group_label='m')


# =============================================
# Data Type Functions
# ---------------------------------------------

class AsString(EvalFunc):

    def _execute(self, col):
        return col.astype(str)


class AsInt(EvalFunc):

    def _execute(self, col):
        return col.astype(int)


class AsFloat(EvalFunc):

    def _execute(self, col):
        return col.astype(float)


class AsDate(EvalFunc):

    def _execute(self, col):
        return pd.to_datetime(col)


class TypeFuncs(EvalFuncGroup):

    LABEL = "as_type"

    def __init__(self):
        self.str = AsString()
        self.int = AsInt()
        self.float = AsFloat()
        self.date = AsDate()
        super(TypeFuncs, self).__init__(group_label='as_type')


# =============================================
# Collect Function Groups
# ---------------------------------------------

function_groups = EvalFuncGroup.__subclasses__()

print function_groups