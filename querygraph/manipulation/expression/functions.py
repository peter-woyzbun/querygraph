from abc import ABCMeta
import re
import pandas as pd
import numpy as np
import datetime

from querygraph.exceptions import QueryGraphException


# =============================================
# Exceptions
# ---------------------------------------------

class ExprFuncException(QueryGraphException):
    pass


# =============================================
# Expression Function Base Class
# ---------------------------------------------

class ExprFunc(object):

    # Should be set in each child class.
    name = None

    __metaclass__ = ABCMeta

    def __call__(self, target, *args, **kwargs):
        return self._execute(target, *args, **kwargs)

    def _execute(self, target, *args, **kwargs):
        # Pandas Series execution.
        if isinstance(target, pd.Series):
            return self._series_execute(target, *args, **kwargs)
        # Numpy array execution.
        elif isinstance(target, np.ndarray):
            return self._np_array_execute(target, *args, **kwargs)
        # List execution.
        elif isinstance(target, list):
            return self._list_execute(target, *args, **kwargs)
        # Singleton execution.
        elif isinstance(target, (str, float, int, datetime.date, datetime.datetime, datetime.time)):
            return self._singleton_execute(target, *args, **kwargs)
        else:
            raise ExprFuncException("Type given not accepted by function '%s'." % self.name)

    def _series_execute(self, target, *args, **kwargs):
        raise ExprFuncException("Pandas Series type not supported for this function.")

    def _list_execute(self, target, *args, **kwargs):
        raise ExprFuncException("List type not supported for this function.")

    def _np_array_execute(self, target, *args, **kwargs):
        raise ExprFuncException("Numpy array type not supported for this function.")

    def _singleton_execute(self, target, *args, **kwargs):
        if isinstance(target, str):
            return self._str_execute(target, *args, **kwargs)
        elif isinstance(target, int):
            return self._int_execute(target, *args, **kwargs)
        elif isinstance(target, float):
            return self._float_execute(target, *args, **kwargs)
        elif isinstance(target, datetime.datetime):
            return self._datetime_execute(target, *args, **kwargs)
        elif isinstance(target, datetime.date):
            return self._date_execute(target, *args, **kwargs)
        elif isinstance(target, datetime.time):
            return self._time_execute(target, *args, **kwargs)

    def _str_execute(self, target, *args, **kwargs):
        raise ExprFuncException("String type not supported for this function.")

    def _int_execute(self, target, *args, **kwargs):
        raise ExprFuncException("Int type not supported for this function.")

    def _float_execute(self, target, *args, **kwargs):
        raise ExprFuncException("Float type not supported for this function.")

    def _datetime_execute(self, target, *args, **kwargs):
        raise ExprFuncException("Datetime type not supported for this function.")

    def _date_execute(self, target, *args, **kwargs):
        raise ExprFuncException("Date type not supported for this function.")

    def _time_execute(self, target, *args, **kwargs):
        raise ExprFuncException("Time type not supported for this function.")

    def _get_args(self, args, n_expected):
        pass


# =============================================
# Expression Function Group Base Class
# ---------------------------------------------

class ExprFuncGroup(object):

    GROUP_LABEL = None

    def __init__(self):
        for attr, value in self.__dict__.iteritems():
            if isinstance(value, ExprFunc):
                value.name = "%s.%s" % (self.GROUP_LABEL, attr)


# =============================================
# String Functions
# ---------------------------------------------

class Uppercase(ExprFunc):

    def _series_execute(self, target, *args, **kwargs):
        return target.str.upper()

    def _list_execute(self, target, *args, **kwargs):
        return map(lambda x: x.upper(), target)

    def _np_array_execute(self, target, *args, **kwargs):
        return np.str.upper(target)

    def _str_execute(self, target, *args, **kwargs):
        return target.upper()

    def _int_execute(self, target, *args, **kwargs):
        return target

    _float_execute = _int_execute
    _datetime_execute = _int_execute


class Lowercase(ExprFunc):

    def _series_execute(self, target, *args, **kwargs):
        return target.str.lower()

    def _list_execute(self, target, *args, **kwargs):
        return map(lambda x: x.lower(), target)

    def _np_array_execute(self, target, *args, **kwargs):
        return np.str.lower(target)

    def _str_execute(self, target, *args, **kwargs):
        return target.lower()

    def _int_execute(self, target, *args, **kwargs):
        return target

    _float_execute = _int_execute
    _datetime_execute = _int_execute


class Capitalize(ExprFunc):

    def _series_execute(self, target, *args, **kwargs):
        return target.str.capitalize()

    def _list_execute(self, target, *args, **kwargs):
        return map(lambda x: x.capitalize(), target)

    def _np_array_execute(self, target, *args, **kwargs):
        return np.str.capitalize(target)

    def _str_execute(self, target, *args, **kwargs):
        return target.capitalize()

    def _int_execute(self, target, *args, **kwargs):
        return target

    _float_execute = _int_execute
    _datetime_execute = _int_execute


class ToDate(ExprFunc):

    def _series_execute(self, target, *args, **kwargs):
        _format = kwargs.pop('format', None)
        new_col = pd.to_datetime(target, format=_format)
        return new_col.dt.date

    def _str_execute(self, target, *args, **kwargs):
        _format = kwargs.pop('format')
        return datetime.datetime.strptime(target, _format).date()

    def _list_execute(self, target, *args, **kwargs):
        _format = kwargs.pop('format')
        return map(lambda x: self._str_execute(x, format=_format), target)

    _np_array_execute = _list_execute

    def _int_execute(self, target, *args, **kwargs):
        return self._str_execute(str(target), *args, **kwargs)

    _float_execute = _int_execute


class ToDateTime(ToDate):

    def _str_execute(self, target, *args, **kwargs):
        _format = kwargs.pop('format')
        return datetime.datetime.strptime(target, _format)

    def _series_execute(self, target, *args, **kwargs):
        _format = kwargs.pop('format', None)
        new_col = pd.to_datetime(target, format=_format)
        return new_col

    def _list_execute(self, target, *args, **kwargs):
        _format = kwargs.pop('format')
        return map(lambda x: self._str_execute(x, format=_format), target)

    def _int_execute(self, target, *args, **kwargs):
        return self._str_execute(str(target), *args, **kwargs)

    _float_execute = _int_execute


class RegexSub(ExprFunc):

    def _str_execute(self, target, *args, **kwargs):
        regex = kwargs.pop('regex')
        with_val = kwargs.pop('with_val')
        return re.sub(regex, with_val, target)

    def _list_execute(self, target, *args, **kwargs):
        regex = kwargs.pop('regex')
        with_val = kwargs.pop('with_val')
        return map(lambda x: self._str_execute(x, regex=regex, with_val=with_val), target)

    def _series_execute(self, target, *args, **kwargs):
        regex = kwargs.pop('regex')
        with_val = kwargs.pop('with_val')
        return target.str.replace(r'%s' % regex, with_val).astype('int')


class Replace(ExprFunc):

    def _series_execute(self, target, *args, **kwargs):
        target_val = kwargs.pop('target_val')
        with_val = kwargs.pop('with_val')
        return target.str.replace(target_val, with_val)

    def _list_execute(self, target, *args, **kwargs):
        target = pd.Series(target)
        series = self._series_execute(target, *args, **kwargs)
        return series.tolist()

    _np_array_execute = _list_execute

    def _str_execute(self, target, *args, **kwargs):
        target_val = kwargs.pop('target_val')
        with_val = kwargs.pop('with_val')
        return target.replace(target_val, with_val)

    def _int_execute(self, target, *args, **kwargs):
        return self._str_execute(str(target), *args, **kwargs)

    _float_execute = _int_execute


class Combine(ExprFunc):

    def _series_execute(self, target, *args, **kwargs):
        to_combine = (target,) + args
        return reduce(self._combine, to_combine)

    def _str_execute(self, target, *args, **kwargs):
        to_combine = (target,) + args
        return reduce(self._combine, to_combine)

    @staticmethod
    def _combine(a, b):
        if isinstance(a, pd.Series):
            a = a.astype(str)
        if isinstance(b, pd.Series):
            b = b.astype(str)
        return a + b


class Slice(ExprFunc):

    def _str_execute(self, target, *args, **kwargs):
        start = kwargs.pop('s')
        end = kwargs.pop('e')
        return target[start: end]

    def _list_execute(self, target, *args, **kwargs):
        return [self._singleton_execute(x, *args, **kwargs) for x in target]

    def _series_execute(self, target, *args, **kwargs):
        start = kwargs.pop('s')
        end = kwargs.pop('e')
        return target.str.slice(start=start, stop=end)

    def _int_execute(self, target, *args, **kwargs):
        return self._str_execute(str(target), *args, **kwargs)

    _float_execute = _int_execute


class StrFuncs(ExprFuncGroup):

    GROUP_LABEL = 'str'

    uppercase = Uppercase()
    lowercase = Lowercase()
    capitalize = Capitalize()
    to_date = ToDate()
    to_datetime = ToDateTime()
    replace = Replace()
    combine = Combine()
    slice = Slice()


# =============================================
# As Type Functions
# ---------------------------------------------

class AsInt(ExprFunc):

    def _series_execute(self, target, *args, **kwargs):
        return target.fillna(0.0).astype('int')

    _np_array_execute = _series_execute

    def _list_execute(self, target, *args, **kwargs):
        return [int(x) for x in target]

    def _int_execute(self, target, *args, **kwargs):
        return target

    def _float_execute(self, target, *args, **kwargs):
        return int(target)

    def _str_execute(self, target, *args, **kwargs):
        try:
            return int(target)
        except:
            return 0


class AsFloat(ExprFunc):

    def _series_execute(self, target, *args, **kwargs):
        return target.fillna(0.0).astype('float')

    _np_array_execute = _series_execute

    def _list_execute(self, target, *args, **kwargs):
        return [float(x) for x in target]

    def _int_execute(self, target, *args, **kwargs):
        return float(target)

    def _float_execute(self, target, *args, **kwargs):
        return target

    def _str_execute(self, target, *args, **kwargs):
        try:
            return float(target)
        except:
            return 0.0


class AsTypeFuncs(ExprFuncGroup):

    GROUP_LABEL = 'as_type'

    int = AsInt()
    float = AsFloat()


# =============================================
# Date Time Functions
# ---------------------------------------------

class DateTimeToString(ExprFunc):

    def _datetime_execute(self, target, *args, **kwargs):
        _format = kwargs.pop('format')
        return target.strftime(_format)

    def _list_execute(self, target, *args, **kwargs):
        _format = kwargs.pop('format')
        return [dt.strftime(_format) for dt in target]

    def _series_execute(self, target, *args, **kwargs):
        _format = kwargs.pop('format')
        return target.dt.strftime(_format)


class AddDelta(ExprFunc):

    def _datetime_execute(self, target, *args, **kwargs):
        return target + datetime.timedelta(**kwargs)

    def _list_execute(self, target, *args, **kwargs):
        return [self._datetime_execute(x, **kwargs) for x in target]


class DateTimeDelta(object):

    def __call__(self, **kwargs):
        return datetime.timedelta(**kwargs)


class DateTimeFuncs(ExprFuncGroup):

    GROUP_LABEL = 'datetime'

    to_str = DateTimeToString()
    delta = DateTimeDelta()
    add_delta = AddDelta()


# =============================================
# Date Functions
# ---------------------------------------------

class DateToString(ExprFunc):

    pass


# =============================================
# Time Functions
# ---------------------------------------------

class TimeToString(ExprFunc):
    pass

# =============================================
# Expression Func Group Collector
# ---------------------------------------------

function_groups = ExprFuncGroup.__subclasses__()