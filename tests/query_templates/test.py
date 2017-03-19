import unittest

from tests import config


@unittest.skipIf(config.SQLITE_DISABLED, reason='Sqlite tests disabled in config.')
class SqliteTests(unittest.TestCase):

    def test_execution(self):
        pass


@unittest.skipIf(config.MYSQL_DISABLED, reason='MySql tests disabled in config.')
class MySqlTests(unittest.TestCase):

    def test_execution(self):
        pass