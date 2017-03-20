from collections import defaultdict

from querygraph import exceptions


class TypeConverter(object):

    def __init__(self):
        self.converters = defaultdict(dict)

    def convert(self, data_type, value):
        if data_type not in self.converters:
            raise exceptions.TypeConversionError("There are no converters defined for parameter "
                                                 "data type '%s'." % data_type)
        if type(value) not in self.converters[data_type]:
            raise exceptions.TypeConversionError("There is no converter defined for parameter data type '%s', "
                                                 "input type '%s'." % (data_type, type(value)))
        return self.converters[data_type][type(value)](value)

    def add_converter(self, data_type, input_type, converter):
        if not callable(converter):
            raise exceptions.ParameterConfigException("Tried to add a 'converter' that is not callable.")
        self.converters[data_type][input_type] = converter

    def add_converters(self, data_type, converter_dict):
        for input_type, converter in converter_dict.items():
            self.add_converter(data_type=data_type,
                               input_type=input_type,
                               converter=converter)

    def add_int_converters(self, converter_dict):
        self.add_converters(data_type='int', converter_dict=converter_dict)

    def add_float_converters(self, converter_dict):
        self.add_converters(data_type='float', converter_dict=converter_dict)

    def add_str_converters(self, converter_dict):
        self.add_converters(data_type='str', converter_dict=converter_dict)

    def add_datetime_converters(self, converter_dict):
        self.add_converters(data_type='datetime', converter_dict=converter_dict)
