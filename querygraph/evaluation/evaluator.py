from querygraph.evaluation.expr_funcs import function_groups


class Evaluator(object):

    def __init__(self):
        self.funcs = {func_group_cls.GROUP_LABEL: func_group_cls() for func_group_cls in function_groups}

    def _create_locals(self, df=None, independent_params=None):
        df_names = dict()
        param_names = dict()
        if df is not None:
            col_names = df.columns.values.tolist()
            df_names = {c: df[c] for c in col_names}
            df_names['df'] = df
        if independent_params:
            param_names = dict()
            for k, v in independent_params.items():
                param_names[k] = v

        locals_dict = dict(self.funcs.items() + df_names.items() + param_names.items())
        return locals_dict

    def eval(self, eval_str, df=None, independent_params=None):
        _locals = self._create_locals(df=df, independent_params=independent_params)
        return eval(eval_str, _locals)