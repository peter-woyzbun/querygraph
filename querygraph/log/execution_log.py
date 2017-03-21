import datetime


class ExecutionLog(object):

    def __init__(self, stdout_print=False):
        self.stdout_print = stdout_print
        self.entries = list()

    def _add_entry(self, msg):
        entry_str = "%s: %s" % (datetime.datetime.now(), msg)
        if self.stdout_print:
            print entry_str
        self.entries.append(entry_str)

    def info(self, msg):
        self._add_entry(msg)

