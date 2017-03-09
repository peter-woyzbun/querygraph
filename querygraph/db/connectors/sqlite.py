from querygraph.db.connectors import base

import sqlite3
import pandas as pd


# =================================================
# DATABASE CONNECTOR CHILD CLASSES
# -------------------------------------------------

class SQLite(base.DatabaseConnector):

    def __init__(self, host):
        """
        Connector for SQLite databases.

        :param host: name of SQLite database file.
        """

        base.DatabaseConnector.__init__(self, database_type='SQLite', host=host)

    def execute_query(self, query):
        """ See abstract base class method docs. """
        connector = sqlite3.connect('%s' % self.host)
        # cursor = connector.cursor()
        df = pd.read_sql_query(query, connector)
        connector.close()
        return df

    def execute_write_query(self, query):
        connector = sqlite3.connect('%s' % self.host)
        c = connector.cursor()
        c.execute(query)
        connector.commit()
        c.close()
        connector.close()