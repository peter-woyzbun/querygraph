import datetime

import tabulate


class ExecutionLog(object):

    def __init__(self, stdout_print=False):
        self.stdout_print = stdout_print
        self.entries = list()

    def _add_entry(self, source, msg):
        entry_str = "%s[%s]: %s" % (datetime.datetime.now(), source, msg)
        if self.stdout_print:
            print entry_str
        self.entries.append(entry_str)

    def graph_info(self, msg):
        self._add_entry(source='graph', msg=msg)

    def graph_error(self, msg):
        self._add_entry(source='graph', msg="ERROR: %s" % msg)

    def dataframe_header(self, source_node, df):
        msg = '[%s] Dataframe retrieved: \n %s' % (source_node,
                                                   str(tabulate.tabulate(df[1:5], headers='keys', tablefmt='psql')))
        self._add_entry(source='node', msg=msg)



