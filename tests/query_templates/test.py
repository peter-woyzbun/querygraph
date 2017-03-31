import unittest

from querygraph.query_template import QueryTemplate
from querygraph.db import interfaces
from tests import config
from tests.db import interfaces as test_db_interfaces
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
        WHERE AlbumId IN {% album_ids -> list:int %}
        """

        query_template = QueryTemplate(template_str=query,
                                       type_converter=test_db_interfaces.sqlite_chinook.type_converter)
        rendered_query = query_template.render(independent_param_vals={'album_ids': [346]})
        df = test_db_interfaces.sqlite_chinook.execute_query(query=rendered_query)
        self.assertEquals(df['Title'].unique(), ['Mozart: Chamber Music'])


@unittest.skipIf(config.MYSQL_DISABLED, reason='MySql tests disabled in config.')
class MySqlTests(unittest.TestCase):

    def test_execution(self):
        query = """
        SELECT *
        FROM Album
        WHERE AlbumId IN {% album_ids -> list:int %}
        """

        query_template = QueryTemplate(template_str=query,
                                       type_converter=test_db_interfaces.mysql_chinook.type_converter)
        rendered_query = query_template.render(independent_param_vals={'album_ids': [346]})
        df = test_db_interfaces.mysql_chinook.execute_query(query=rendered_query)
        self.assertEquals(df['Title'].unique(), ['Mozart: Chamber Music'])


@unittest.skipIf(config.POSTGRES_DISABLED, reason='Postgres tests disabled in config.')
class PostgresTests(unittest.TestCase):

    def test_execution(self):
        query = """
        SELECT *
        FROM "Album"
        WHERE "AlbumId" IN {% album_ids -> list:int %}
        """

        query_template = QueryTemplate(template_str=query,
                                       type_converter=test_db_interfaces.postgres_chinook.type_converter)

        rendered_query = query_template.render(independent_param_vals={'album_ids': [346]})
        df = test_db_interfaces.postgres_chinook.execute_query(query=rendered_query)
        self.assertEquals(df['Title'].unique(), ['Mozart: Chamber Music'])


# =================================================
# NoSQL Databases
# -------------------------------------------------

@unittest.skipIf(config.MONGODB_DISABLED, reason='Mongo DB tests disabled in config.')
class MongoDbTests(unittest.TestCase):

    def test_execution(self):
        query = """{'tags': {'$in': {% album_tags -> list:str %}}}"""

        query_template = QueryTemplate(template_str=query,
                                       type_converter=test_db_interfaces.mongodb_albums.type_converter)
        rendered_query = query_template.render(independent_param_vals={'album_tags': ["canada"]})
        df = test_db_interfaces.mongodb_albums.execute_query(query=rendered_query, fields=['album'])
        self.assertEquals(df['album'].unique(), ['Jagged Little Pill'])


@unittest.skipIf(config.ELASTIC_SEARCH_DISABLED, reason='ElasticSearch tests disabled in config.')
class ElasticSearchTests(unittest.TestCase):

    def test_execution(self):
        query = """{'terms': {'tags': {% album_tags -> list:str %}}}"""

        query_template = QueryTemplate(template_str=query,
                                       type_converter=test_db_interfaces.elastic_search_albums.type_converter)
        rendered_query = query_template.render(independent_param_vals={'album_tags': ["canada"]})

        df = test_db_interfaces.elastic_search_albums.execute_query(query=rendered_query, fields=['album'])
        self.assertEquals(df['album'].unique(), ['Jagged Little Pill'])


def main():
    unittest.main()

if __name__ == '__main__':
    main()