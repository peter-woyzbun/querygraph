import datetime

from querygraph.query.template.parameter import TemplateParameter


class MongoParameter(TemplateParameter):

    def __init__(self, parameter_str, parameter_type):
        TemplateParameter.__init__(self,
                                   parameter_str=parameter_str,
                                   parameter_type=parameter_type)

    def _make_value_list(self, parameter_value):
        parameter_value = list(set(parameter_value))
        val_str = ", ".join(str(self._make_single_value(x)) for x in parameter_value)
        val_str = "[%s]" % val_str
        return val_str
