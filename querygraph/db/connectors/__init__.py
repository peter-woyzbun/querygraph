from querygraph.utils.optional_imports import NotInstalled


try:
    from sqlite import SQLite
except ImportError:
    SQLite = NotInstalled(name='sqlite3')


try:
    from my_sql import MySQL
except ImportError:
    MySQL = NotInstalled(name='mysql')


try:
    from postgres import Postgres
except ImportError:
    Postgres = NotInstalled(name='psycopg2')


try:
    from elastic_search import ElasticSearch
except ImportError:
    ElasticSearch = NotInstalled(name='elasticsearch')


try:
    from mongo_db import MongoDb
except ImportError:
    MongoDb = NotInstalled(name='pymongo')


try:
    from influx_db import InfluxDb
except ImportError:
    # Todo: get proper package name.
    InfluxDb = NotInstalled(name='influxdb')