import pandas as pd
import numpy as np

from tests import config
from tests.db import connectors


def create_postgres_db_tables():
    """ Generate Postgres database containing Chinook tables. """
    with open(config.DB_SQL_PATHS['postgres'], 'r') as sql_file:
        query_str = sql_file.read()
    connectors.postgres_chinook.execute_write_query(query=query_str, latin_encoding=True)


def create_mysql_db_tables():
    """ Generate Postgres database containing Chinook tables. """
    with open(config.DB_SQL_PATHS['mysql'], 'r') as sql_file:
        query_str = sql_file.read()
    connectors.mysql_chinook.execute_write_query(query=query_str, latin_encoding=True)


def create_mongo_album_data():
    from pymongo import MongoClient

    client = MongoClient()

    db = client['querygraph-test']
    seasons = db['seasons']

    seasons_data = [{'season': 'Spring',
                     'tags': ['growing', 'melting', 'warm', 'dog poop']},
                    {'season': 'Summer',
                     'tags': ['sunny', 'hot', 'fun', 'beach', 'vacation']},
                    {'season': 'Fall',
                     'tags': ['colours', 'colder', 'leaves', 'halloween']},
                    {'season': 'Winter',
                     'tags': ['cold', 'snow', 'christmas', 'skiing']}]

    result = seasons.insert_many(seasons_data)


def insert_mongo_album_data():

    albums_data = [{'album': 'For Those About To Rock We Salute You',
                    'tags': ['rock', 'classic rock', 'australia', 'band']},
                   {'album': 'Balls to the Wall',
                    'tags': ['metal', 'heavy metal', 'germany', 'band']},
                   {'album': 'Restless and Wild',
                    'tags': ['metal', 'heavy metal', 'germany', 'band']},
                   {'album': 'Let There Be Rock',
                    'tags': ['rock', 'classic rock', 'australia', 'band']},
                   {'album': 'Big Ones',
                    'tags': ['rock', 'united states', 'band']},
                   {'album': 'Jagged Little Pill',
                    'tags': ['canada', 'pop rock', 'post-grunge', 'female']},
                   {'album': 'Facelift',
                    'tags': ['rock', 'united states', 'band']},
                   ]

    connectors.mongodb_albums.insert_many(albums_data)