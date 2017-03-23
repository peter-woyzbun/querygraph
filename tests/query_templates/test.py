import unittest

from querygraph import query_templates
from tests import config
from tests.db.connectors import sqlite_chinook, mysql_chinook, postgres_chinook, mongodb_albums, elastic_search_albums


# =================================================
# Relational Databases
# -------------------------------------------------

@unittest.skipIf(config.SQLITE_DISABLED, reason='Sqlite tests disabled in config.')
class SqliteTests(unittest.TestCase):

    def test_execution(self):
        query = """
        SELECT *
        FROM Album
        WHERE AlbumId IN {% album_ids|list:int %}
        """

        query_template = query_templates.Sqlite(template_str=query, db_connector=sqlite_chinook)
        df = query_template.execute(album_ids=[346])
        self.assertEquals(df['Title'].unique(), ['Mozart: Chamber Music'])


@unittest.skipIf(config.MYSQL_DISABLED, reason='MySql tests disabled in config.')
class MySqlTests(unittest.TestCase):

    def test_execution(self):
        query = """
        SELECT *
        FROM Album
        WHERE AlbumId IN {% album_ids|list:int %}
        """

        query_template = query_templates.Sqlite(template_str=query, db_connector=mysql_chinook)
        df = query_template.execute(album_ids=[346])
        self.assertEquals(df['Title'].unique(), ['Mozart: Chamber Music'])


@unittest.skipIf(config.POSTGRES_DISABLED, reason='Postgres tests disabled in config.')
class PostgresTests(unittest.TestCase):

    def test_execution(self):
        query = """
        SELECT *
        FROM "Album"
        WHERE "AlbumId" IN {% album_ids|list:int %}
        """

        query_template = query_templates.Sqlite(template_str=query, db_connector=postgres_chinook)
        df = query_template.execute(album_ids=[346])
        self.assertEquals(df['Title'].unique(), ['Mozart: Chamber Music'])


# =================================================
# NoSQL Databases
# -------------------------------------------------

@unittest.skipIf(config.MONGODB_DISABLED, reason='Mongo DB tests disabled in config.')
class MongoDbTests(unittest.TestCase):

    def test_execution(self):
        query = """{'tags': {'$in': {% album_tags|list:str %}}}"""

        query_template = query_templates.MongoDb(template_str=query, db_connector=mongodb_albums, fields=['album'])
        df = query_template.execute(album_tags=["canada"])
        self.assertEquals(df['album'].unique(), ['Jagged Little Pill'])


@unittest.skipIf(config.ELASTIC_SEARCH_DISABLED, reason='ElasticSearch tests disabled in config.')
class ElasticSearchTests(unittest.TestCase):

    def test_execution(self):
        query = """{'terms': {'tags': {% album_tags|list:str %}}}"""

        query_template = query_templates.ElasticSearch(template_str=query,
                                                       db_connector=elastic_search_albums,
                                                       fields=['album'])
        df = query_template.execute(album_tags=["canada"])
        self.assertEquals(df['album'].unique(), ['Jagged Little Pill'])


def main():
    unittest.main()

if __name__ == '__main__':
    main()