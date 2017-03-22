import unittest

from querygraph.template_parameter import TemplateParameter


class GenericRenderingTests(unittest.TestCase):

    def test_atomic_str(self):
        param_str = "test_param|value:str"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': 'test str'})
        self.assertEquals(result, "'test str'")

    def test_list_str(self):
        param_str = "test_param|list:str"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': ['test str', 'test str']})
        self.assertEquals(result, "('test str', 'test str')")

    def test_atomic_int(self):
        param_str = "test_param|value:int"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': 5})
        self.assertEquals(result, 5)


class GenericExprTests(unittest.TestCase):

    def test_uppercase(self):
        param_str = "uppercase(test_param)|value:str"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param': 'test str'})
        self.assertEquals(result, "'TEST STR'")

    def test_int_addition(self):
        param_str = "test_param_1 + test_param_2|value:int"
        test_param = TemplateParameter(param_str=param_str, independent=True)
        result = test_param.query_value(independent_param_vals={'test_param_1': 1, 'test_param_2': 1})
        self.assertEquals(result, 2)


def main():
    unittest.main()

if __name__ == '__main__':
    main()