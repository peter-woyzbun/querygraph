import threading

from querygraph.exceptions import QueryGraphException


class ExecutionThread(threading.Thread):

    __lock = threading.Lock()

    def __init__(self, threads, query_node, independent_param_vals):
        threading.Thread.__init__(self)
        self.threads = threads
        self.query_node = query_node
        self.independent_param_vals = independent_param_vals
        self.has_error = False
        self.exception = None

    def run(self):
        try:
            self.query_node.retrieve_dataframe(independent_param_vals=self.independent_param_vals)
        except QueryGraphException, e:
            self.has_error = True
            self.exception = e
            raise
        except Exception, e:
            self.has_error = True
            self.exception = e
            raise

        if not self.query_node.result_set_empty:
            for child_node in self.query_node.children:
                child_thread = self._create_child_thread(child_query_node=child_node)
                child_thread.start()
                self.threads.append(child_thread)

    def _create_child_thread(self, child_query_node):
        child_thread = ExecutionThread(threads=self.threads,
                                       query_node=child_query_node,
                                       independent_param_vals=self.independent_param_vals)
        return child_thread


