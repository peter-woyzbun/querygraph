import datetime

from querygraph.query.template.parameter import TemplateParameter


class MySqlParameter(TemplateParameter):

    CHILD_DATA_TYPES = {
        'datetime': {datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d %H:%M:%S'),
                     str: lambda x: "'%s'" % x},
        'date': {datetime.datetime: lambda x: "'%s'" % x.strftime('%Y-%m-%d'),
                 str: lambda x: "'%s'" % x},
        'time': {datetime.datetime: lambda x: "'%s'" % x.strftime('%H:%M:%S'),
                 str: lambda x: "'%s'" % x}
    }

    def __init__(self, parameter_str, parameter_type):
        TemplateParameter.__init__(self,
                                   parameter_str=parameter_str,
                                   parameter_type=parameter_type)
