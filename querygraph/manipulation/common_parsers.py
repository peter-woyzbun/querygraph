import pyparsing as pp


column = pp.Word(pp.alphas + "_", pp.alphanums + "_$")