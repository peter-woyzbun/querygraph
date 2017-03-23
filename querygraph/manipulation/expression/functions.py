import abc
import datetime
import re

import pandas as pd
import numpy as np

from querygraph.utils.multi_method import multimethod


# =============================================
# Base Eval Function
# ---------------------------------------------

class ExprFunc(object):

    def __init__(self, name):
        self.name = name

    def __call__(self, *args, **kwargs):
        return self._execute(*args, **kwargs)

    @abc.abstractmethod
    def _execute(self, *args, **kwargs):
        pass


# =============================================
# Misc. Functions
# ---------------------------------------------

class Lag(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='lag')

    @multimethod(pd.Series, int)
    def _execute(self, value, periods):
        return value.shift(periods)


class Length(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='len')

    @multimethod(pd.Series)
    def _execute(self, value):
        return len(value)

    @multimethod(str)
    def _execute(self, value):
        return len(value)


# =============================================
# Math Functions
# ---------------------------------------------

class Log(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='log')

    @multimethod(pd.Series)
    def _execute(self, value):
        return np.log(value)

    @multimethod(int)
    def _execute(self, value):
        return np.log(value)

    @multimethod(float)
    def _execute(self, value):
        return np.log(value)


class Log10(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='log10')

    @multimethod(pd.Series)
    def _execute(self, value):
        return np.log10(value)

    @multimethod(int)
    def _execute(self, value):
        return np.log10(value)

    @multimethod(float)
    def _execute(self, value):
        return np.log10(value)


class Floor(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='floor')

    @multimethod(pd.Series)
    def _execute(self, value):
        return np.floor(value)

    @multimethod(int)
    def _execute(self, value):
        return np.floor(value)

    @multimethod(float)
    def _execute(self, value):
        return np.floor(value)


class Ceiling(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='ceil')

    @multimethod(pd.Series)
    def _execute(self, value):
        return np.ceil(value)

    @multimethod(int)
    def _execute(self, value):
        return np.ceil(value)

    @multimethod(float)
    def _execute(self, value):
        return np.ceil(value)


class Sin(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='sin')

    @multimethod(pd.Series)
    def _execute(self, value):
        return np.sin(value)

    @multimethod(int)
    def _execute(self, value):
        return np.sin(value)

    @multimethod(float)
    def _execute(self, value):
        return np.sin(value)


class Cos(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='cos')

    @multimethod(pd.Series)
    def _execute(self, value):
        return np.cos(value)

    @multimethod(int)
    def _execute(self, value):
        return np.cos(value)

    @multimethod(float)
    def _execute(self, value):
        return np.cos(value)


class Tan(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='tan')

    @multimethod(pd.Series)
    def _execute(self, value):
        return np.tan(value)

    @multimethod(int)
    def _execute(self, value):
        return np.tan(value)

    @multimethod(float)
    def _execute(self, value):
        return np.tan(value)


class Sqrt(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='sqrt')

    @multimethod(pd.Series)
    def _execute(self, value):
        return np.sqrt(value)

    @multimethod(int)
    def _execute(self, value):
        return np.sqrt(value)

    @multimethod(float)
    def _execute(self, value):
        return np.sqrt(value)


class Square(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='square')

    @multimethod(pd.Series)
    def _execute(self, value):
        return np.square(value)

    @multimethod(int)
    def _execute(self, value):
        return np.square(value)

    @multimethod(float)
    def _execute(self, value):
        return np.square(value)


# =============================================
# String Functions
# ---------------------------------------------

class Uppercase(ExprFunc):

    """ Convert strings to uppercase. """

    def __init__(self):
        ExprFunc.__init__(self, name='uppercase')

    @multimethod(pd.Series)
    def _execute(self, value):
        return value.str.upper()

    @multimethod(list)
    def _execute(self, value):
        return map(lambda x: x.upper(), value)

    @multimethod(str)
    def _execute(self, value):
        print "Uppercase for str called!"
        return value.upper()

    @multimethod(float)
    def _execute(self, value):
        return value

    @multimethod(int)
    def _execute(self, value):
        return value


class Lowercase(ExprFunc):

    """ Convert strings to lowercase. """

    def __init__(self):
        ExprFunc.__init__(self, name='lowercase')

    @multimethod(pd.Series)
    def _execute(self, value):
        return value.str.lower()

    @multimethod(list)
    def _execute(self, value):
        return map(lambda x: x.lower(), value)

    @multimethod(str)
    def _execute(self, value):
        print "Lowercase for str called!"
        return value.lower()

    @multimethod(float)
    def _execute(self, value):
        return value

    @multimethod(int)
    def _execute(self, value):
        return value


class Capitalize(ExprFunc):

    """ Capitalize strings. """

    def __init__(self):
        ExprFunc.__init__(self, name='capitalize')

    @multimethod(pd.Series)
    def _execute(self, value):
        return value.str.capitalize()

    @multimethod(list)
    def _execute(self, value):
        return map(lambda x: x.capitalize(), value)

    @multimethod(str)
    def _execute(self, value):
        return value.capitalize()

    @multimethod(float)
    def _execute(self, value):
        return value

    @multimethod(int)
    def _execute(self, value):
        return value


class ToDate(ExprFunc):

    """ Convert string to datetime date type. """

    def __init__(self):
        ExprFunc.__init__(self, name='to_date')

    @multimethod(pd.Series, str)
    def _execute(self, value, format):
        new_col = pd.to_datetime(value, format=format)
        return new_col.dt.date

    @multimethod(str, str)
    def _execute(self, value, format):
        return datetime.datetime.strptime(value, format).date()


class ToDateTime(ExprFunc):

    """ Convert string to datetime datetime type. """

    def __init__(self):
        ExprFunc.__init__(self, name='to_datetime')

    @multimethod(pd.Series, str)
    def _execute(self, value, format):
        new_col = pd.to_datetime(value, format=format)
        return new_col.dt

    @multimethod(str, str)
    def _execute(self, value, format):
        return datetime.datetime.strptime(value, format)


class RegexSub(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='regex_sub')

    @multimethod(pd.Series, str, object)
    def _execute(self, value, regex, new_val):
        return value.str.replace(r'%s' % regex, new_val)

    @multimethod(str, str, object)
    def _execute(self, value, regex, new_val):
        return re.sub(regex, new_val, value)

    @multimethod(int, str, object)
    def _execute(self, value, regex, new_val):
        return int(re.sub(regex, new_val, str(value)))

    @multimethod(float, str, object)
    def _execute(self, value, regex, new_val):
        return float(re.sub(regex, new_val, str(value)))


class Replace(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='replace')

    @multimethod(pd.Series)
    def _execute(self, value, old_val, new_val):
        return value.str.replace(old_val, new_val)

    @multimethod(str)
    def _execute(self, value, old_val, new_val):
        return value.replace(old_val, new_val)


class Slice(ExprFunc):

    def __init__(self):
        ExprFunc.__init__(self, name='slice')

    @multimethod(pd.Series, int, int)
    def _execute(self, value, start, stop):
        return value.str.slice(start=start, stop=stop)

    @multimethod(str)
    def _execute(self, value, start, stop):
        return value[start: stop]

    @multimethod(float)
    def _execute(self, value, start, stop):
        return float(str(value)[start: stop])

    @multimethod(int)
    def _execute(self, value, start, stop):
        return int(str(value)[start: stop])


class ReformatDatetimeStr(ExprFunc):

    # Todo: implement for pandas series, lists,...

    def __init__(self):
        ExprFunc.__init__(self, name='reformat_dt_str')

    @multimethod(str, str, str)
    def _execute(self, value, in_fmt, out_fmt):
        dt_val = datetime.datetime.strptime(value, in_fmt)
        str_val = dt_val.strftime(out_fmt)
        return str_val


# =============================================
# Expression Func Child Class Collector
# ---------------------------------------------

all_functions = [func() for func in ExprFunc.__subclasses__()]