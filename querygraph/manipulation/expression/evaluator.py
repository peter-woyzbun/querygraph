from querygraph.manipulation.expression.functions import function_groups


class Evaluator(object):

    """ Evaluator for user-defined 'manipulation expressions'. """

    def __init__(self):
        self.funcs = {func_group_cls.GROUP_LABEL: func_group_cls() for func_group_cls in function_groups}

    def _create_locals(self, df=None, independent_param_vals=None):
        """
        Create a dictionary of 'local' variables callable in manipulation
        expression string.

        Parameters
        ----------
        df : DataFrame, optional
            Pandas DataFrame whose column names will be mapped to their Series instance (e.g. col_1 = df['col_1'])
        independent_param_vals : dict, optional
            Dictionary mapping independent parameter names to values.

        """
        df_names = dict()
        param_names = dict()
        if df is not None:
            col_names = df.columns.values.tolist()
            df_names = {c: df[c] for c in col_names}
            df_names['df'] = df
        if independent_param_vals:
            param_names = dict()
            for k, v in independent_param_vals.items():
                param_names[k] = v

        locals_dict = dict(self.funcs.items() + df_names.items() + param_names.items())
        return locals_dict

    def eval(self, eval_str, df=None, independent_param_vals=None):
        _locals = self._create_locals(df=df, independent_param_vals=independent_param_vals)
        return eval(eval_str, _locals)