import unittest

from querygraph.query_templates.template_parameter import TemplateParameter, ParameterParseException


class ParserTests(unittest.TestCase):

    def test_parameter_name(self):
        """ Test parameter name parsing. """
        param_string = "test_param|value_list:str"
        param_name = "test_param"
        template_parameter = TemplateParameter(param_str=param_string, param_type='dependent')
        self.assertEquals(param_name, template_parameter.name)

    def test_bad_parameter_name(self):
        """ Test bad parameter name parsing - should raise exception. """
        param_string = "test param|tea_cup:str"
        self.assertRaises(ParameterParseException, TemplateParameter, param_str=param_string, param_type='dependent')

    def test_container_type(self):
        """ Test container type parsing. """
        param_string = "test_param|value_list:str"
        template_parameter = TemplateParameter(param_str=param_string, param_type='dependent')
        self.assertEquals("value_list", template_parameter.container_type)

    def test_bad_container_type(self):
        """ Test bad container type (not 'value' or 'value_list') parsing - should raise exception. """
        param_string = "test_param|tea_cup:str"
        self.assertRaises(ParameterParseException, TemplateParameter, param_str=param_string, param_type='dependent')

    def test_data_type(self):
        """ Test data type parsing. """
        param_string = "test_param|value_list:str"
        template_parameter = TemplateParameter(param_str=param_string, param_type='dependent')
        self.assertEquals("str", template_parameter.data_type)

    def test_bad_data_type(self):
        """ Test bad data type (not 'str', 'int', or 'float') parsing - should raise exception. """
        param_string = "test_param|value:"
        self.assertRaises(ParameterParseException, TemplateParameter, param_str=param_string, param_type='dependent')


class QueryValueTests(unittest.TestCase):

    def test_string_value(self):
        test_param_str = "test_param|value:str"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent')
        param_value_dict = {'test_param': 'param_value'}
        self.assertEquals("'param_value'", independent_parameter.query_value(independent_params=param_value_dict))

    def test_int_value(self):
        test_param_str = "test_param|value:int"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent')
        param_value_dict = {'test_param': 1}
        self.assertEquals(1, independent_parameter.query_value(independent_params=param_value_dict))

    def test_float_value(self):
        test_param_str = "test_param|value:float"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent')
        param_value_dict = {'test_param': 1.0}
        self.assertEquals(1.0, independent_parameter.query_value(independent_params=param_value_dict))

    def test_string_value_list(self):
        test_param_str = "test_param|value_list:str"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent')
        param_value_dict = {'test_param': ['value_1', 'value_2']}
        expected_str = "('value_1', 'value_2')"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))

    def test_int_value_list(self):
        test_param_str = "test_param|value_list:int"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent')
        param_value_dict = {'test_param': [1, 2]}
        expected_str = "(1, 2)"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))

    def test_float_value_list(self):
        test_param_str = "test_param|value_list:int"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent')
        param_value_dict = {'test_param': [1.1, 2.1]}
        expected_str = "(1.1, 2.1)"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))



def main():
    unittest.main()

if __name__ == '__main__':
    main()