import unittest
import datetime


from querygraph.graph import QueryGraph
from querygraph.query.node import QueryNode
from tests.db import connectors
from tests import config



class ExecutionTests(unittest.TestCase):

    def test_sqlite_postgres_mysql(self):
        query = """
        SELECT *
        FROM Album
        JOIN Artist ON Artist.ArtistId = Album.ArtistId
        JOIN Track ON Track.AlbumId = Album.AlbumId
        WHERE Artist.Name == {% artist_name|value:str %}
        """

        sqlite_query = """
        SELECT *
        FROM Artist
        WHERE Artist.Name == {% artist_name|value:str %}
        """

        sqlite_node = QueryNode(name='sqlite_node', query=sqlite_query, db_connector=connectors.sqlite_chinook)


class QGLTests(unittest.TestCase):

    def test_read_and_execution(self):
        query_str = """
        CONNECT
            sqlite_chinook <- Sqlite(host='%s')
            postgres_chinook <- Postgres(host='%s', user='%s', password='%s', db_name='%s', port='%s')
            mysql_chinook <- MySql(host='%s', user='%s', password='%s', db_name='%s')
        RETRIEVE
            QUERY |
                SELECT *
                FROM Artist
                WHERE Artist.Name == {%% artist_name|value:str %%};
            USING sqlite_chinook
            AS sqlite_node
            ---
            QUERY |
                SELECT *
                FROM "Album"
                WHERE "ArtistId" IN {{ ArtistId|value_list:int }};
            USING postgres_chinook
            AS postgres_node
            ---
            QUERY |
                SELECT *
                FROM Track
                WHERE AlbumId IN {{ AlbumId|value_list:int }};
            USING mysql_chinook
            AS mysql_node
        JOIN
            RIGHT (postgres_node[ArtistId] ==> sqlite_node[ArtistId]);
            INNER (mysql_node[AlbumId] ==> postgres_node[AlbumId]);

        """ % (config.DATABASES['sqlite']['HOST'],
               config.DATABASES['postgres']['HOST'],
               config.DATABASES['postgres']['USER'],
               config.DATABASES['postgres']['PASSWORD'],
               config.DATABASES['postgres']['DB_NAME'],
               config.DATABASES['postgres']['PORT'],
               config.DATABASES['mysql']['HOST'],
               config.DATABASES['mysql']['USER'],
               config.DATABASES['mysql']['PASSWORD'],
               config.DATABASES['mysql']['DB_NAME'])

        query_graph = QueryGraph(qgl_str=query_str)
        df = query_graph.parallel_execute(artist_name='Smashing Pumpkins')
        self.assertEquals(set(df['AlbumId'].unique()), {201, 202})

    def test_mongodb(self):
        query_str = """
        CONNECT
            mongodb_albums <- MongoDb(host='%s', port='%s', db_name='%s', collection='%s')
            postgres_chinook <- Postgres(host='%s', user='%s', password='%s', db_name='%s', port='%s')
        RETRIEVE
            QUERY |
                {"tags" : {"$in" : {%% album_tags|value_list:str %%} }};
            USING mongodb_albums
            AS mongodb_node
            ---
            QUERY |
                SELECT *
                FROM "Album" album
                WHERE "Title" IN {{ album|value_list:str }}
                JOIN "Artist" artist ON artist.artistid = album.artistid;
            USING postgres_chinook
            AS postgres_node
        JOIN
            LEFT (postgres_node[ArtistId] ==> sqlite_node[ArtistId]);

        """ % (config.DATABASES['sqlite']['HOST'],
               config.DATABASES['postgres']['HOST'],
               config.DATABASES['postgres']['USER'],
               config.DATABASES['postgres']['PASSWORD'],
               config.DATABASES['postgres']['DB_NAME'],
               config.DATABASES['postgres']['PORT'],
               config.DATABASES['mysql']['HOST'],
               config.DATABASES['mysql']['USER'],
               config.DATABASES['mysql']['PASSWORD'],
               config.DATABASES['mysql']['DB_NAME'])



