import unittest
import datetime


from querygraph.query.node import QueryNode
from tests.db import connectors



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



