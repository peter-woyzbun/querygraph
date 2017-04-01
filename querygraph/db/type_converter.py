import datetime
import time
import copy
from collections import defaultdict

import numpy as np

from querygraph import exceptions


class TypeConverter(object):
    """
    The TypeConverter class is in charge of rendering Python values
    into their appropriate form for the given database type.

    Parameters
    ----------
    type_converters : dict or None
        A dictionary mapping parameter render types (e.g. 'str') to
        Python input types (e.g. int) to a converter (callable) that
        converts a Python input value into a value that can be used in a
        database query.
    container_converters : dict or None
        A dictionary mapping container render types (e.g. 'list')
        to a converter (callable) that returns a value that can be
        used in a database query.

    """

    GENERIC_TYPE_CONVERTERS = {
        # Render type 'int'
        # Parameter string: '<parameter_expression> -> [<container_type>:]int'
        'int': {
            int: lambda x: x,
            np.int64: lambda x: x,
            np.int32: lambda x: x,
            np.int16: lambda x: x,
            np.int8: lambda x: x,
            np.float64: lambda x: x,
            np.float32: lambda x: x,
            np.float16: lambda x: x,
            float: lambda x: int(x),
            str: lambda x: int(x)
        },
        # Render type 'float'
        # Parameter string: '<parameter_expression> -> [<container_type>:]float'
        'float': {
            int: lambda x: float(x),
            np.int64: lambda x: float(x),
            np.int32: lambda x: float(x),
            np.int16: lambda x: float(x),
            np.int8: lambda x: float(x),
            np.float64: lambda x: x,
            np.float32: lambda x: x,
            np.float16: lambda x: x,
            float: lambda x: x,
            str: lambda x: float(x)
        },
        # Render type 'str'
        # Parameter string: '<parameter_expression> -> [<container_type>:]str'
        'str': {
            str: lambda x: "'%s'" % x,
            unicode: lambda x: "'%s'" % x,
            float: lambda x: "'%s'" % x,
            int: lambda x: "'%s'" % x,
            datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d %H:%M:%S'),
            datetime.date: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
            datetime.time: lambda x: "'%s'" % x.strftime('%H:%M:%S'),
        },
        # Render type 'datetime'
        # Parameter string: '<parameter_expression> -> [<container_type>:]datetime'
        'datetime': {
            datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d %H:%M:%S'),
            datetime.date: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
            str: lambda x: "'%s'" % x,
            unicode: lambda x: "'%s'" % x,
        },
        # Render type 'date'
        # Parameter string: '<parameter_expression> -> [<container_type>:]date'
        'date': {
            datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
            datetime.date: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
            str: lambda x: "'%s'" % x,
            unicode: lambda x: "'%s'" % x,
        },
        # Render type 'time'
        # Parameter string: '<parameter_expression> -> [<container_type>:]time'
        'time': {
            datetime.datetime: lambda x: "'%s'" % x.strftime('%H:%M:%S'),
            datetime.time: lambda x: "'%s'" % x.strftime('%H:%M:%S'),
            str: lambda x: "'%s'" % x,
            unicode: lambda x: "'%s'" % x,
        }
    }

    GENERIC_CONTAINER_CONVERTERS = {
        # Container type 'list'
        # Parameter string: '<parameter_expression> -> list:<render_type>'
        'list': lambda x: '(%s)' % ", ".join(str(y) for y in x),
        # Container type 'tuple'
        # Parameter string: '<parameter_expression> -> tuple:<render_type>'
        'tuple': lambda x: '(%s)' % ", ".join(str(y) for y in x)
    }

    def __init__(self, type_converters=None, container_converters=None):
        self.db_specific_converters = type_converters
        self.db_specific_container_converters = container_converters
        self.type_converters = defaultdict(dict)
        self.container_converters = defaultdict(dict)
        self._setup_type_converters()
        self._setup_container_converters()

    def _convert_atomic_value(self, rendered_type, python_value):
        self._check_conversion_inputs(rendered_type, python_value)
        return self.type_converters[rendered_type][type(python_value)](python_value)

    def convert(self, rendered_type, python_value, container_type=None):
        if container_type is not None:
            self._container_type_check(container_type)
            container_values = [self._convert_atomic_value(rendered_type, x) for x in python_value]
            return self.container_converters[container_type](container_values)
        else:
            return self._convert_atomic_value(rendered_type=rendered_type, python_value=python_value)

    def _container_type_check(self, container_type):
        if container_type not in self.supported_container_types:
            raise exceptions.TypeConversionError("Unsupported container type: '%s'." % container_type)

    def _check_conversion_inputs(self, rendered_type, python_value, container_type=None):
        if rendered_type not in self.supported_rendered_types:
            raise exceptions.TypeConversionError("The given render type '%s' is not supported." % rendered_type)
        if type(python_value) not in self.type_converters[rendered_type]:
            raise exceptions.TypeConversionError("Dont know how to convert Python"
                                                 "value of type '%s' to render type '%s'" % (type(python_value),
                                                                                             rendered_type))

    @property
    def supported_rendered_types(self):
        return self.type_converters.keys()

    @property
    def supported_container_types(self):
        return self.container_converters.keys()

    def _setup_type_converters(self):
        self._setup_generic_type_converters()
        self._set_db_specific_type_converters()

    def _setup_generic_type_converters(self):
        # Is this copy necessary?
        generic_type_converters = copy.deepcopy(self.GENERIC_TYPE_CONVERTERS)
        for render_type, converter_dict in generic_type_converters.items():
            self.type_converters[render_type] = converter_dict

    def _set_db_specific_type_converters(self):
        if self.db_specific_converters is not None:
            for render_type, converter_dict in self.db_specific_converters.items():
                for input_type, converter in converter_dict.items():
                    if not isinstance(input_type, type):
                        raise exceptions.TypeConverterException
                    if not callable(converter):
                        raise exceptions.TypeConverterException
                    self.type_converters[render_type][input_type] = converter
        else:
            pass

    def _setup_container_converters(self):
        self._setup_generic_container_converters()
        self._setup_db_specific_container_converters()

    def _setup_generic_container_converters(self):
        generic_container_converters = copy.deepcopy(self.GENERIC_CONTAINER_CONVERTERS)
        for container_type, converter_dict in generic_container_converters.items():
            self.container_converters[container_type] = converter_dict

    def _setup_db_specific_container_converters(self):
        if self.db_specific_container_converters is not None:
            for container_type, converter in self.db_specific_container_converters.items():
                if not callable(converter):
                    raise exceptions.TypeConverterException
                self.container_converters[container_type] = converter








