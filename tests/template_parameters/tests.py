import unittest
import datetime

from querygraph.query_templates.template_parameter import TemplateParameter, ParameterParseException
from querygraph.db.connectors import TestConnector


class ParserTests(unittest.TestCase):

    def test_parameter_name(self):
        """ Test parameter name parsing. """
        param_string = "test_param|value_list:str"
        param_name = "test_param"
        template_parameter = TemplateParameter(param_str=param_string,
                                               param_type='dependent',
                                               db_connector=TestConnector.sqlite())
        self.assertEquals(param_name, template_parameter.name)

    def test_bad_parameter_name(self):
        """ Test bad parameter name parsing - should raise exception. """
        param_string = "test param|tea_cup:str"
        self.assertRaises(ParameterParseException, TemplateParameter, param_str=param_string,
                          param_type='dependent',
                          db_connector=TestConnector.sqlite())

    def test_container_type(self):
        """ Test container type parsing. """
        param_string = "test_param|value_list:str"
        template_parameter = TemplateParameter(param_str=param_string, param_type='dependent',
                                               db_connector=TestConnector.sqlite())
        self.assertEquals("value_list", template_parameter.container_type)

    def test_bad_container_type(self):
        """ Test bad container type (not 'value' or 'value_list') parsing - should raise exception. """
        param_string = "test_param|tea_cup:str"
        self.assertRaises(ParameterParseException, TemplateParameter, param_str=param_string, param_type='dependent',
                          db_connector=TestConnector.sqlite())

    def test_data_type(self):
        """ Test data type parsing. """
        param_string = "test_param|value_list:str"
        template_parameter = TemplateParameter(param_str=param_string, param_type='dependent',
                                               db_connector=TestConnector.sqlite())
        self.assertEquals("str", template_parameter.data_type)

    def test_bad_data_type(self):
        """ Test bad data type (not 'str', 'int', or 'float') parsing - should raise exception. """
        param_string = "test_param|value:"
        self.assertRaises(ParameterParseException, TemplateParameter, param_str=param_string, param_type='dependent',
                          db_connector=TestConnector.sqlite())


class QueryValueTests(unittest.TestCase):

    def test_string_value(self):
        """ Test the rendering of a single string value. """
        test_param_str = "test_param|value:str"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=TestConnector.sqlite())
        param_value_dict = {'test_param': 'param_value'}
        self.assertEquals("'param_value'", independent_parameter.query_value(independent_params=param_value_dict))

    def test_int_value(self):
        """ Test the rendering of a single int value. """
        test_param_str = "test_param|value:int"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=TestConnector.sqlite())
        param_value_dict = {'test_param': 1}
        self.assertEquals(1, independent_parameter.query_value(independent_params=param_value_dict))

    def test_float_value(self):
        """ Test the rendering of a single float value. """
        test_param_str = "test_param|value:float"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=TestConnector.sqlite())
        param_value_dict = {'test_param': 1.0}
        self.assertEquals(1.0, independent_parameter.query_value(independent_params=param_value_dict))

    def test_custom_data_type_value(self):
        """ Test the rendering of a single custom data type. """
        param_string = "test_param|value:custom[date '%s']"
        template_parameter = TemplateParameter(param_str=param_string, param_type='independent',
                                               db_connector=TestConnector.sqlite())
        param_value_dict = {'test_param': '2001-09-28'}
        self.assertEquals("date '2001-09-28'", template_parameter.query_value(independent_params=param_value_dict))

    def test_string_value_list(self):
        """ Test the rendering of a value list of strings. """
        test_param_str = "test_param|value_list:str"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=TestConnector.sqlite())
        param_value_dict = {'test_param': ['value_1', 'value_2']}
        expected_str = "('value_1', 'value_2')"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))

    def test_int_value_list(self):
        """ Test the rendering of a value list of integers. """
        test_param_str = "test_param|value_list:int"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=TestConnector.sqlite())
        param_value_dict = {'test_param': [1, 2]}
        expected_str = "(1, 2)"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))

    def _test_float_value_list(self):
        # Todo: fix this.
        test_param_str = "test_param|value_list:int"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=TestConnector.sqlite())
        param_value_dict = {'test_param': [1.1, 2.1]}
        expected_str = "(1.1, 2.1)"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))

    def test_sqlite_date_value(self):
        test_param_str = "date_param|value:date"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=TestConnector.sqlite())
        param_value_dict = {'date_param': datetime.datetime(2009, 1, 6)}
        expected_str = "date(2009-01-06)"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))

    def test_mysql_date_value(self):
        test_param_str = "date_param|value:date"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=TestConnector.mysql())
        param_value_dict = {'date_param': datetime.datetime(2009, 1, 6)}
        expected_str = "date(2009-01-06)"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))

    def test_postgres_date_value(self):
        test_param_str = "date_param|value:date"
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=TestConnector.postgres())
        param_value_dict = {'date_param': datetime.datetime(2009, 1, 6)}
        expected_str = "date '2009-01-06'"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))

    def test_sqlite_datetime_value(self):
        test_param_str = "date_param|value:datetime"
        mysql_connector = TestConnector.mysql()
        independent_parameter = TemplateParameter(param_str=test_param_str, param_type='independent',
                                                  db_connector=mysql_connector)
        param_value_dict = {'date_param': datetime.datetime(2009, 1, 6)}
        expected_str = "date(2009-01-06)"
        self.assertEquals(expected_str, independent_parameter.query_value(independent_params=param_value_dict))


def main():
    unittest.main()

if __name__ == '__main__':
    main()