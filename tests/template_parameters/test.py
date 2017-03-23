import unittest
import datetime

from querygraph.template_parameter import TemplateParameter


class GenericRenderingTests(unittest.TestCase):

    def test_atomic_str(self):
        param_str = "test_param -> str"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': 'test str'})
        self.assertEquals(result, "'test str'")

    def test_list_str(self):
        param_str = "test_param -> list:str"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': ['test str', 'test str']})
        self.assertEquals(result, "('test str', 'test str')")

    def test_atomic_int(self):
        param_str = "test_param -> int"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': 5})
        self.assertEquals(result, 5)

    def test_list_int(self):
        param_str = "test_param -> list:int"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': [1, 2]})
        self.assertEquals(result, "(1, 2)")


class GenericExprTests(unittest.TestCase):

    def test_uppercase(self):
        param_str = "uppercase(test_param) -> str"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': 'test str'})
        self.assertEquals(result, "'TEST STR'")

    def test_lowercase(self):
        param_str = "lowercase(test_param) -> str"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': 'TEST STR'})
        self.assertEquals(result, "'test str'")

    def test_capitalize(self):
        param_str = "capitalize(test_param) -> str"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': 'test str'})
        self.assertEquals(result, "'Test str'")

    def test_to_date(self):
        param_str = 'to_date(test_param, "%Y/%m/%d") -> date'
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': '2009/01/06'})
        self.assertEquals(result, "'2009-01-06'")

    def test_int_addition(self):
        param_str = "test_param_1 + test_param_2 -> int"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param_1': 1, 'test_param_2': 1})
        self.assertEquals(result, 2)


def main():
    unittest.main()

if __name__ == '__main__':
    main()