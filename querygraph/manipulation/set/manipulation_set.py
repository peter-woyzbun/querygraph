from abc import ABCMeta, abstractmethod

import pandas as pd
import pyparsing as pp

from querygraph.exceptions import QueryGraphException
from querygraph.manipulation.expression.evaluator import Evaluator


# =============================================
# Manipulation Exceptions
# ---------------------------------------------

class ManipulationException(QueryGraphException):
    pass


class ManipulationSetException(QueryGraphException):
    pass


class ConfigurationException(ManipulationException):
    pass


# =============================================
# Manipulation Abstract Base Class
# ---------------------------------------------

class Manipulation(object):

    __metaclass__ = ABCMeta

    def __call__(self, df, evaluator=None):
        return self.execute(df, evaluator)

    def execute(self, df, evaluator=None):
        return self._execute(df, evaluator)

    @abstractmethod
    def _execute(self, df, evaluator=None):
        pass


# =============================================
# Manipulation Types
# ---------------------------------------------

class Mutate(Manipulation):

    def __init__(self, col_name, col_expr):
        self.col_name = col_name
        self.col_expr = col_expr

    def _execute(self, df, evaluator=None):
        if not isinstance(evaluator, Evaluator):
            raise ManipulationException
        expr_evaluator = Evaluator(df=df)
        df[self.col_name] = expr_evaluator.eval(expr_str=self.col_expr)
        return df


class Rename(Manipulation):

    def __init__(self, old_column_name, new_column_name):
        self.old_column_name = old_column_name
        self.new_column_name = new_column_name

    def _execute(self, df, evaluator=None):
        df = df.rename(columns={self.old_column_name: self.new_column_name})
        return df


class Select(Manipulation):

    def __init__(self, columns):
        self.columns = columns

    def _execute(self, df, evaluator=None):
        return df[self.columns]


class Remove(Manipulation):

    def __init__(self, columns):
        self.columns = columns

    def _execute(self, df, evaluator=None):
        return df.drop(self.columns, inplace=False, axis=1)


class Flatten(Manipulation):

    def __init__(self, column):
        self.column = column

    def _execute(self, df, evaluator=None):
        col_flat = pd.DataFrame([[i, x]
                                 for i, y in df[self.column].apply(list).iteritems()
                                 for x in y], columns=['I', self.column])
        col_flat = col_flat.set_index('I')
        df = df.drop(self.column, 1)
        df = df.merge(col_flat, left_index=True, right_index=True)
        df = df.reset_index(drop=True)
        return df

test_df = pd.DataFrame({'A': ['album_1', 'album_2'],
                            'B': [0, 0],
                            'C': [['tag_1', 'tag_2'], ['tag_3', 'tag_1']]})

test_df_2 = pd.DataFrame({'A': [{'city': 'bagdad', 'country': 'iraq'}, {'city': 'detroit', 'country': 'US'}],
                            'B': [0, 0],
                            'C': [['tag_1', 'tag_2'], ['tag_3', 'tag_1']],
                          'D': [{'first': {'second': 'lol1'}}, {'first': {'second': 'lol2'}}]})


class Unpack(Manipulation):

    def __init__(self, unpack_list):
        self.unpack_list = unpack_list

    @staticmethod
    def unpack_dict(row_dict, key_list):
        return reduce(dict.__getitem__, key_list, row_dict)

    def _execute(self, df, evaluator=None):
        for unpack_dict in self.unpack_list:
            print unpack_dict
            packed_col = unpack_dict['packed_col']
            key_list = unpack_dict['key_list']
            new_col_name = unpack_dict['new_col_name']
            df[new_col_name] = df[packed_col].apply(lambda x: self.unpack_dict(row_dict=x, key_list=key_list))
        return df

    @classmethod
    def parser(cls):
        unpack = pp.Suppress("unpack")

        packed_col_name = pp.Word(pp.alphas, pp.alphanums + "_$")
        dict_key = pp.Suppress("[") + pp.QuotedString(quoteChar="'") + pp.Suppress("]")
        dict_key_grp = pp.Group(pp.OneOrMore(dict_key))
        _as = pp.Keyword("AS")
        new_col_name = pp.Word(pp.alphas, pp.alphanums + "_$")

        unpack_arg = packed_col_name + dict_key_grp + pp.Suppress(_as) + new_col_name
        unpack_arg.setParseAction(lambda x: {'packed_col': x[0], 'key_list': x[1], 'new_col_name': x[2]})

        parser = unpack + pp.Suppress("(") + pp.delimitedList(unpack_arg) + pp.Suppress(")")
        parser.setParseAction(lambda x: Unpack(unpack_list=x))
        return parser


unpack_parser = Unpack.parser()

unpack = unpack_parser.parseString("unpack(A['city'] AS city, A['country'] AS country, D['first']['second'] AS fun)")[0]

print unpack._execute(df=test_df_2)


# =============================================
# Manipulation Set
# ---------------------------------------------


class ManipulationSet(object):

    def __init__(self):
        self.manipulations = list()

    def __contains__(self, manipulation_type):
        return any(isinstance(x, manipulation_type) for x in self.manipulations)

    def __add__(self, other):
        if not isinstance(other, Manipulation):
            raise ConfigurationException("Only Manipulation instances can be added to ManipulationSet.")
        else:
            self.manipulations.append(other)
        return self

    __radd__ = __add__

    def __iter__(self):
        for x in self.manipulations:
            yield x

    def __nonzero__(self):
        if len(self.manipulations) > 0:
            return True
        else:
            return False

    def execute(self, df):
        print "Executing manipulation set!"
        evaluator = Evaluator()
        for manipulation in self:
            df = manipulation.execute(df, evaluator)
        return df
