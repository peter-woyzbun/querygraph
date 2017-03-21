import threading

from querygraph.exceptions import QueryGraphException


class ExecutionThread(threading.Thread):

    __lock = threading.Lock()

    def __init__(self, threads, query_node, query_template, independent_param_vals):
        threading.Thread.__init__(self)
        self.threads = threads
        self.query_node = query_node
        self.query_template = query_template
        self.independent_param_vals = independent_param_vals

    def run(self):
        result_df = self.query_template.execute()
        self.query_node.df = result_df
        self.query_node.execute_manipulation_set()
        for child_node in self.query_node.children:
            child_thread = self._create_child_thread(child_query_node=child_node)
            child_thread.start()
            self.threads.append(child_thread)

    def _create_child_thread(self, child_query_node):
        child_thread = ExecutionThread(threads=self.threads,
                                       query_node=child_query_node,
                                       independent_param_vals=self.independent_param_vals)
        return child_thread


