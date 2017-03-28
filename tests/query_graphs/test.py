import unittest

from tests import config
from querygraph.graph import QueryGraph


class MongoDbPostgresTests(unittest.TestCase):

    def test_execute(self):
        query = """
        CONNECT
            postgres_conn <- Postgres(db_name='%s', user='%s', password='%s', host='%s', port='%s')
            mongodb_conn <- Mongodb(host='%s', port='%s', db_name='%s', collection='%s')
        RETRIEVE
            QUERY |
                {'tags': {'$in': {%% album_tags -> list:str %%}}};
            FIELDS album, tags
            USING mongodb_conn
            THEN |
                flatten(tags) >>
                remove(_id);
            AS mongo_node
            ---
            QUERY |
                SELECT *
                FROM "Album"
                WHERE "Title" IN {{ album -> list:str }};
            USING postgres_conn
            AS postgres_node
            JOIN
                LEFT (postgres_node[Title] ==> mongo_node[album])
                        """ % (config.DATABASES['postgres']['DB_NAME'],
                               config.DATABASES['postgres']['USER'],
                               config.DATABASES['postgres']['PASSWORD'],
                               config.DATABASES['postgres']['HOST'],
                               config.DATABASES['postgres']['PORT'],
                               config.DATABASES['mongodb']['HOST'],
                               config.DATABASES['mongodb']['PORT'],
                               config.DATABASES['mongodb']['DB_NAME'],
                               config.DATABASES['mongodb']['COLLECTION'])
        query_graph = QueryGraph(qgl_str=query)

        df = query_graph.execute(album_tags=['canada', 'rock'])
        self.assertEquals(set(df['album'].unique()),
                              {'Jagged Little Pill',
                               'Facelift', 'Big Ones',
                               'Let There Be Rock',
                               'For Those About To Rock We Salute You'})


class ElasticSearchPostgresTests(unittest.TestCase):

    def test_execute(self):
        query = """
                CONNECT
                    postgres_conn <- Postgres(db_name='%s', user='%s', password='%s', host='%s', port='%s')
                    es_conn <- ElasticSearch(host='%s', port='%s', doc_type='%s', index='%s')
                RETRIEVE
                    QUERY |
                        {'terms': {'tags': {%% album_tags -> list:str %%}}};
                    FIELDS album, tags
                    USING es_conn
                    AS es_node
                    ---
                    QUERY |
                        SELECT *
                        FROM "Album"
                        WHERE "Title" IN {{ album -> list:str }};
                    USING postgres_conn
                    AS postgres_node
                    JOIN
                        LEFT (postgres_node[Title] ==> es_node[album])
                                """ % (config.DATABASES['postgres']['DB_NAME'],
                                       config.DATABASES['postgres']['USER'],
                                       config.DATABASES['postgres']['PASSWORD'],
                                       config.DATABASES['postgres']['HOST'],
                                       config.DATABASES['postgres']['PORT'],
                                       config.DATABASES['elasticsearch']['HOST'],
                                       config.DATABASES['elasticsearch']['PORT'],
                                       config.DATABASES['elasticsearch']['DOC_TYPE'],
                                       config.DATABASES['elasticsearch']['INDEX'])
        query_graph = QueryGraph(qgl_str=query)

        df = query_graph.execute(album_tags=['canada', 'rock'])
        self.assertEquals(set(df['album'].unique()),
                          {'Jagged Little Pill',
                           'Facelift', 'Big Ones',
                           'Let There Be Rock',
                           'For Those About To Rock We Salute You'})



def main():
    unittest.main()


if __name__ == '__main__':
    main()
