import threading

from querygraph.exceptions import QueryGraphException
from querygraph.query.template import QueryTemplate


class ExecutionThread(threading.Thread):

    __lock = threading.Lock()

    def __init__(self, query_template, node_name, result_df_container, independent_param_vals):
        threading.Thread.__init__(self)
        if not isinstance(query_template, QueryTemplate):
            raise QueryGraphException
        self.query_template = query_template
        self.node_name = node_name
        self.result_df_container = result_df_container
        self.independent_param_vals = independent_param_vals

    def run(self):
        result_df = self.query_template.execute()
        self.result_df_container[self.node_name] = result_df

