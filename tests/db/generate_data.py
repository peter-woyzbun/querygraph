import pandas as pd
import numpy as np

from tests import config
from tests.db import connectors


nosql_data = [{'album': 'For Those About To Rock We Salute You',
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


def insert_elastic_search_album_data():

    connector = connectors.elastic_search_albums

    for id, data in enumerate(nosql_data):
        connector.insert_entry(index='album', id=id, data=data)
