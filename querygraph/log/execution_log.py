import datetime

import tabulate


class ExecutionLog(object):

    def __init__(self, stdout_print=False):
        self.stdout_print = stdout_print
        self.entries = list()

    def _add_entry(self, source_prefix, msg):
        entry_str = "%s%s: %s" % (datetime.datetime.now(), source_prefix, msg)
        if self.stdout_print:
            print entry_str
        self.entries.append(entry_str)

    def graph_info(self, msg):
        self._add_entry(source_prefix='[GRAPH]', msg=msg)

    def graph_error(self, msg):
        self._add_entry(source_prefix='[GRAPH]', msg="ERROR: %s" % msg)

    def dataframe_header(self, source_node, df):
        n_rows, n_cols = df.shape
        msg = 'Dataframe retrieved (%s rows, %s columns). First four rows shown below: \n %s' \
              % (n_rows,
                 n_cols,
                 str(tabulate.tabulate(df[1:5], headers='keys', tablefmt='psql')))
        self._add_entry(source_prefix='[NODE:%s]' % source_node, msg=msg)

    def node_info(self, source_node, msg):
        self._add_entry(source_prefix='[NODE:%s]' % source_node, msg=msg)

    def node_error(self, source_node, msg):
        self._add_entry(source_prefix='[NODE:%s][ERROR]' % source_node, msg=msg)



