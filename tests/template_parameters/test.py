import unittest

from querygraph.template_parameter import TemplateParameter


class RenderingTests(unittest.TestCase):
    def test_type_converter(self):
        param_str = "test_param|value:str"
        test_param = TemplateParameter(parameter_str=param_str, parameter_type='independent')
        result = test_param.query_value(independent_params={'test_param': 'test str'})
        self.assertEquals(result, "'test str'")


def main():
    unittest.main()

if __name__ == '__main__':
    main()