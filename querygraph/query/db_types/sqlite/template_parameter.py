import datetime

from querygraph.query.template.parameter import TemplateParameter


class SqliteParameter(TemplateParameter):

    CHILD_DATA_TYPES = {
        'datetime': {datetime.datetime: lambda x: "datetime(%s)" % x.strftime('%Y-%m-%d %H:%M:%S'),
                     str: lambda x: "datetime(%s)" % x},
        'date': {datetime.datetime: lambda x: "date(%s)" % x.strftime('%Y-%m-%d'),
                 str: lambda x: "date(%s)" % x},
        'time': {datetime.datetime: lambda x: "time(%s)" % x.strftime('%H:%M:%S'),
                 str: lambda x: "time(%s)" % x}
    }

    def __init__(self, parameter_str, parameter_type):
        TemplateParameter.__init__(self,
                                   parameter_str=parameter_str,
                                   parameter_type=parameter_type)


param_str = "test_datetime|value:datetime"
test_param = SqliteParameter(parameter_type='independent', parameter_str=param_str)

print test_param.DATA_TYPES

