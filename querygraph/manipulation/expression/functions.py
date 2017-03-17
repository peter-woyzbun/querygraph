import abc
import re
import datetime

import pandas as pd
import numpy as np


from querygraph.utils.multi_method import multimethod
from querygraph.manipulation.exceptions import ManipulationError


# =============================================
# Exceptions
# ---------------------------------------------

class ExpressionFunctionError(ManipulationError):
    pass


class InputTypeError(ExpressionFunctionError):
    pass


# =============================================
# Base Expression Function
# ---------------------------------------------

class ExprFunc(object):

    __metaclass__ = abc.ABCMeta

    name = None

    def __call__(self, *args, **kwargs):
        try:
            return self._execute(*args, **kwargs)
        except TypeError:
            raise InputTypeError("Types used as input for function '%s' not supported." % self.name)

    @abc.abstractmethod
    def _execute(self, *args, **kwargs):
        pass


# =============================================
# String Functions
# ---------------------------------------------

class Uppercase(ExprFunc):

    """ Convert strings to uppercase. """

    name = 'uppercase'

    @staticmethod
    @multimethod(pd.Series)
    def _execute(value):
        return value.str.upper()

    @staticmethod
    @multimethod(list)
    def _execute(value):
        return map(lambda x: x.upper(), value)

    @staticmethod
    @multimethod(str)
    def _execute(value):
        return value.upper()

    @staticmethod
    @multimethod(float)
    def _execute(value):
        return value

    @staticmethod
    @multimethod(int)
    def _execute(value):
        return value




# =============================================
# Expression Func Child Class Collector
# ---------------------------------------------

function_classes = ExprFunc.__subclasses__()