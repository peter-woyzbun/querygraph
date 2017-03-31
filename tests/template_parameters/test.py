import unittest
import datetime

import pandas as pd
import numpy as np

from querygraph.template_parameter import TemplateParameter
from querygraph.db.type_converter import TypeConverter


test_df = pd.DataFrame({'A': [1, 2, 3, 4],
                        'B': [0, 0, 0, 0],
                        'C': ['a', 'b', 'c', 'd'],
                        'D': ['A', 'B', 'C', 'D'],
                        'E': ['Abc', 'Abc', 'Abc', 'Abc'],
                        'F': ['abc', 'abc', 'abc', 'abc'],
                        'G': ['ABC', 'ABC', 'ABC', 'ABC'],
                        'H': [datetime.datetime(2009, 1, 6, 1, 1, 1) for i in range(0, 4)],
                        'I': ['2009-01-06 01:01:01' for j in range(0, 4)],
                        'J': ['2009-01-06' for k in range(0, 4)]})


class GenericRenderingTests(unittest.TestCase):

    type_converter = TypeConverter()

    def test_atomic_str(self):
        param_str = "test_param -> str"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(independent_param_vals={'test_param': 'test str'})
        self.assertEquals(result, "'test str'")

    def test_list_str(self):
        param_str = "test_param -> list:str"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(independent_param_vals={'test_param': ['test str', 'test str']})
        self.assertEquals(result, "('test str', 'test str')")

    def test_atomic_int(self):
        param_str = "test_param -> int"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(independent_param_vals={'test_param': 5})
        self.assertEquals(result, 5)

    def test_list_int(self):
        param_str = "test_param -> list:int"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(independent_param_vals={'test_param': [1, 2]})
        self.assertEquals(result, "(1, 2)")

    def test_dependent_param(self):
        param_str = "A -> list:int"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, "(1, 2, 3, 4)")


class GenericExprTests(unittest.TestCase):

    type_converter = TypeConverter()

    def test_uppercase(self):
        param_str = "uppercase(test_param) -> str"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(independent_param_vals={'test_param': 'test str'})
        self.assertEquals(result, "'TEST STR'")

    def test_lowercase(self):
        param_str = "lowercase(test_param) -> str"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(independent_param_vals={'test_param': 'TEST STR'})
        self.assertEquals(result, "'test str'")

    def test_capitalize(self):
        param_str = "capitalize(test_param) -> str"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(independent_param_vals={'test_param': 'test str'})
        self.assertEquals(result, "'Test str'")

    def test_len(self):
        param_str = "len(A) -> int"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, 4)

    def test_log(self):
        param_str = "log(2) -> float"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, np.log(2))

    def test_log10(self):
        param_str = "log10(2) -> float"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, np.log10(2))

    def test_floor(self):
        param_str = "floor(2.4) -> int"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, 2)

    def test_ceil(self):
        param_str = "ceil(2.4) -> int"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, 3)

    def test_sin(self):
        param_str = "sin(2.4) -> float"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, np.sin(2.4))

    def test_cos(self):
        param_str = "cos(2.4) -> float"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, np.cos(2.4))

    def test_tan(self):
        param_str = "tan(2.4) -> float"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, np.tan(2.4))

    def test_sqrt(self):
        param_str = "sqrt(2.4) -> float"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, np.sqrt(2.4))

    def test_square(self):
        param_str = "square(2.4) -> float"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, np.square(2.4))

    def test_round(self):
        param_str = "round(2.45, 1) -> float"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(df=test_df)
        self.assertEquals(result, np.round_(2.45, 1))

    def test_to_date(self):
        param_str = 'to_date(test_param, "%Y/%m/%d") -> date'
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(independent_param_vals={'test_param': '2009/01/06'})
        self.assertEquals(result, "'2009-01-06'")

    def test_int_addition(self):
        param_str = "test_param_1 + test_param_2 -> int"
        test_param = TemplateParameter(parameter_str=param_str, type_converter=self.type_converter)
        result = test_param.render(independent_param_vals={'test_param_1': 1, 'test_param_2': 1})
        self.assertEquals(result, 2)


def main():
    unittest.main()

if __name__ == '__main__':
    main()