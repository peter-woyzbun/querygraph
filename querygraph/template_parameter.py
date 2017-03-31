import pyparsing as pp

from querygraph.manipulation.expression.evaluator import Evaluator
from querygraph.manipulation.expression import ManipulationExpression
from querygraph.db.type_converter import TypeConverter


# =============================================
# Template Parameter
# ---------------------------------------------


class TemplateParameter(object):

    def __init__(self, parameter_str, type_converter):
        self.parameter_str = parameter_str
        assert isinstance(type_converter, TypeConverter)
        self.type_converter = type_converter

        self.param_expr = None
        self.render_as_type = None
        self.render_as_container = None

    def _set_param_expr(self, value):
        self.param_expr = value

    def _set_render_type(self, value):
        self.render_as_type = value

    def _set_container_type(self, value):
        self.render_as_container = value

    def _parse(self, df=None, independent_param_vals=None):
        expr_evaluator = Evaluator(df=df, name_dict=independent_param_vals)
        param_expr = expr_evaluator.parser()

        render_as_type = pp.Word(pp.alphas, pp.alphanums + "_$")
        render_as_type.setParseAction(lambda x: self._set_render_type(value=x[0]))

        container_type = pp.Optional(pp.Word(pp.alphas, pp.alphanums + "_$") + pp.Suppress(":"), default=None)
        container_type.setParseAction(lambda x: self._set_container_type(value=x[0]))

        parser = param_expr + pp.Suppress("->") + container_type + render_as_type
        parser.parseString(self.parameter_str)

        python_value = expr_evaluator.output_value()
        return python_value

    def render(self, df=None, independent_param_vals=None):
        python_value = self._parse(df=df, independent_param_vals=independent_param_vals)
        rendered_value = self.type_converter.convert(rendered_type=self.render_as_type,
                                                     container_type=self.render_as_container,
                                                     python_value=python_value)
        return rendered_value

