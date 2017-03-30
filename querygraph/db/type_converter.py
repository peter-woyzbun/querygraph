import datetime
import copy

import numpy as np

from querygraph import exceptions


class TypeConverter(object):

    GENERIC_TYPE_CONVERTERS = {
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
        'str': {
            str: lambda x: "'%s'" % x,
            unicode: lambda x: "'%s'" % x,
            float: lambda x: "'%s'" % x,
            int: lambda x: "'%s'" % x
        },
        'datetime': {
            datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d %H:%M:%S'),
            datetime.date: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
            str: lambda x: "'%s'" % x
        },
        'date': {
            datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
            datetime.date: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
            str: lambda x: "'%s'" % x
        },
        'time': {
            datetime.datetime: lambda x: "'%s'" % x.strftime('%H:%M:%S'),
            datetime.time: lambda x: "'%s'" % x.strftime('%H:%M:%S'),
            str: lambda x: "'%s'" % x
        }
    }

    GENERIC_CONTAINER_CONVERTERS = {
        'list': lambda x: '(%s)' % ", ".join(str(y) for y in x)
    }

    def __init__(self, type_converters=None, container_converters=None):
        self.db_specific_converters = type_converters
        self.db_specific_container_converters = container_converters
        self.type_converters = copy.deepcopy(self.GENERIC_TYPE_CONVERTERS)
        self.container_converters = copy.deepcopy(self.GENERIC_CONTAINER_CONVERTERS)
        self._setup_type_converters()
        self._setup_container_converters()

    def _convert_atomic_value(self, rendered_type, python_value):
        return self.type_converters[rendered_type][type(python_value)](python_value)

    def convert(self, rendered_type, python_value, container_type=None):
        self._check_conversion_inputs(rendered_type, python_value, container_type)
        if container_type is not None:
            container_values = [self._convert_atomic_value(rendered_type, x) for x in python_value]
            return self.container_converters[container_type](container_values)
        else:
            return self._convert_atomic_value(rendered_type=rendered_type, python_value=python_value)

    def _check_conversion_inputs(self, rendered_type, python_value, container_type=None):
        if rendered_type not in self.supported_rendered_types:
            raise exceptions.TypeConversionError
        if type(python_value) not in self.type_converters[rendered_type]:
            raise exceptions.TypeConversionError
        if container_type is not None and container_type not in self.supported_container_types:
            raise exceptions.TypeConversionError

    @property
    def supported_rendered_types(self):
        return self.type_converters.keys()

    @property
    def supported_container_types(self):
        return self.container_converters.keys()

    def _setup_type_converters(self):
        if self.db_specific_converters is not None:
            for input_type_converter in self.db_specific_converters.values():
                for input_type, converter in input_type_converter.items():
                    if not isinstance(input_type, type):
                        raise exceptions.TypeConverterException
                    if not callable(converter):
                        raise exceptions.TypeConverterException
                    self.type_converters[input_type_converter][input_type] = converter
        else:
            pass

    def _setup_container_converters(self):
        if self.db_specific_container_converters is not None:
            for container_type, converter in self.db_specific_container_converters.items():
                if not callable(converter):
                    raise exceptions.TypeConverterException
                self.container_converters[container_type] = converter






