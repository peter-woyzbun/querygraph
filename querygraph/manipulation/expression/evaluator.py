import operator
import math
import re

import pandas as pd
from pyparsing import (Literal,
                       CaselessLiteral,
                       Word,
                       Combine,
                       Optional,
                       ZeroOrMore,
                       Forward,
                       nums,
                       alphas,
                       quotedString,
                       Suppress,
                       delimitedList,
                       Keyword,
                       removeQuotes,
                       originalTextFor)

from functions import all_functions


class Evaluator(object):

    # Todo: clean up.

    opn = {"+": operator.add,
           "-": operator.sub,
           "*": operator.mul,
           "/": operator.truediv,
           "^": operator.pow,
           "<": operator.lt,
           ">": operator.gt,
           "<=": operator.le,
           ">=": operator.ge,
           "|": operator.or_,
           "&": operator.and_}

    funcs = {func.name: func for func in all_functions}

    def __init__(self, deferred_eval=False, df=None, df_name=None, name_dict=None):
        self.deferred_eval = deferred_eval
        self.df = df
        self.df_name = df_name
        self.name_dict = name_dict
        self.expr_stack = list()
        self.func_input_stack = list()

        if self.df is not None:
            self._clean_col_names()

    def eval(self, expr_str):
        parser = self.parser()
        parser.parseString(expr_str)
        return self.output_value()

    def output_value(self):
        return self._evaluate_stack()

    def push_first(self, tok):
        """ Append token to expression stack. """
        self.expr_stack.append(tok)

    def push_unary_minus(self, toks):
        if toks and toks[0] == '-':
            self.expr_stack.append('unary -')

    def _add_func_inputs(self, input_dict):
        """
        Add a dictionary of function inputs to the function input stack. Each dictionary
        is retrieved when the function is called from the expression stack.

        """
        print "ADDED FUNCTION INPUTS!"
        print input_dict
        self.func_input_stack.append(input_dict)

    @property
    def col_names(self):
        return self.df.columns.values.tolist()

    def col_name_parser(self):
        if not self.deferred_eval:
            if self.df_name is not None:
                df_prefix = Suppress("%s." % self.df_name)
                col_names = [Optional(df_prefix) + Literal("%s" % col_name) for col_name in self.col_names]
            else:
                col_names = [Literal("%s" % col_name) for col_name in self.col_names]
            parser = reduce(lambda x, y: x | y, col_names)
            return parser
        else:
            parser = Word(alphas, alphas + nums + "_$")
            return parser

    def named_val_parser(self):
        names = [Literal("%s" % val_key) for val_key in self.name_dict.keys()]
        parser = reduce(lambda x, y: x | y, names)
        return parser

    def value_parser(self):
        if not self.deferred_eval:
            if self.df is not None:
                column = self.col_name_parser()
                if self.name_dict is not None:
                    named_val = self.named_val_parser()
                    value = column | named_val
                    return value
                else:
                    return column
            elif self.name_dict is not None:
                return self.named_val_parser()
        else:
            return self.col_name_parser()

    @property
    def op_strings(self):
        return self.opn.keys()

    @property
    def function_names(self):
        return self.funcs.keys()

    @property
    def func_name_parsers(self):
        return [Literal(func_name) for func_name in self.function_names]

    def _clean_col_names(self):
        """ Ensure all dataframe column names are valid Python names. """
        for col_name in self.col_names:
            cleaned_name = re.sub('\W|^(?=\d)', '_', col_name)
            if cleaned_name != col_name:
                print "Warning: renamed column '%s' to '%s'." % (col_name, cleaned_name)
                self.df = self.df.rename(columns={col_name: cleaned_name})

    def parser(self):
        point = Literal(".")
        e = CaselessLiteral("E")
        fnumber = Combine(Word("+-" + nums, nums) +
                          Optional(point + Optional(Word(nums))) +
                          Optional(e + Word("+-" + nums, nums)))
        # ident = reduce(lambda x, y: x | y, self.func_name_parsers)
        ident = Word(alphas, alphas + nums + "_$")

        plus = Literal("+")
        minus = Literal("-")
        mult = Literal("*")
        div = Literal("/")
        _or = Literal("|")
        _and = Literal("&")
        lpar = Literal("(").suppress()
        rpar = Literal(")").suppress()
        lt = Keyword("<")
        le = Keyword("<=")
        gt = Keyword(">")
        ge = Keyword(">=")
        addop = plus | minus | le | lt | gt | ge
        multop = mult | div | _or | _and
        expop = Literal("^")
        pi = CaselessLiteral("PI")

        # column_names = [Literal(col_name) for col_name in self.col_names]
        # Equivalent to: 'x_1 | x_2 | ... | x_n'
        # column = self.col_name_parser()

        # Function arguments
        plusorminus = Literal('+') | Literal('-')
        number = Word(nums)
        integer = Combine(Optional(plusorminus) + number).setParseAction(lambda x: int(x[0]))
        floatnumber = Combine(integer +
                              Optional(point + Optional(number)) +
                              Optional(e + integer)
                              ).setParseAction(lambda x: float(x[0]))

        arg = (integer | floatnumber | quotedString.addParseAction(removeQuotes).setParseAction(lambda x: x[0][1:-1])).setParseAction(lambda x: x[0])

        args = Optional(delimitedList(arg), default=None).setParseAction(lambda x: {'args': [z for z in x]})
        kwarg = (Word(alphas, alphas + nums + "_$") + Suppress("=") + arg).setParseAction(lambda x: {x[0]: x[1]})
        kwargs = Optional(Suppress(",") + delimitedList(kwarg), default=None)

        kwargs.setParseAction(lambda x:
                                  {'kwargs': dict(pair for d in x for pair in d.items())} if x[0] is not None else {
                                      'kwargs': None})

        func_inputs = (Suppress(",") + args + kwargs)
        func_input_block = Optional(func_inputs, default={'args': None, 'kwargs': None})

        func_input_block.setParseAction(lambda x: self._add_func_inputs(dict(pair for d in x for pair in d.items())))

        expr = Forward()

        kwarg.setParseAction(lambda x: {x[0]: x[1]})

        value = self.value_parser()

        atom = (Optional("-") + (value | pi | e | fnumber | ident + lpar + expr + func_input_block
                                 + rpar).setParseAction(lambda x: self.push_first(tok=x[0])) | (
                    lpar + expr.suppress() + rpar)).setParseAction(lambda x: self.push_unary_minus(toks=x))

        factor = Forward()
        factor << atom + ZeroOrMore((expop + factor).setParseAction(lambda x: self.push_first(tok=x[0])))

        term = factor + ZeroOrMore((multop + factor).setParseAction(lambda x: self.push_first(tok=x[0])))
        expr << term + ZeroOrMore((addop + term).setParseAction(lambda x: self.push_first(tok=x[0])))

        return expr

    def _evaluate_stack(self):
        op = self.expr_stack.pop()
        if op == 'unary -':
            return -self._evaluate_stack()
        if op in self.op_strings:
            op2 = self._evaluate_stack()
            op1 = self._evaluate_stack()
            return self.opn[op](op1, op2)
        elif self.df is not None and op in self.col_names:
            return self.df[op]
        elif op == "PI":
            return math.pi  # 3.1415926535
        elif op == "E":
            return math.e  # 2.718281828
        elif op in self.funcs:
            func_input_data = self.func_input_stack.pop()
            args = func_input_data['args']
            kwargs = func_input_data['kwargs']
            if args and kwargs:
                return self.funcs[op](self._evaluate_stack(), *args, **kwargs)
            elif args and not kwargs:
                return self.funcs[op](self._evaluate_stack(), *args)
            elif kwargs and not args:
                return self.funcs[op](self._evaluate_stack(), **kwargs)
            elif not kwargs and not args:
                return self.funcs[op](self._evaluate_stack())
        elif self.name_dict is not None and op in self.name_dict:
            return self.name_dict[op]
        elif op[0].isalpha():
            return 0
        else:
            return float(op)
