from querygraph.utils import optional_imports


try:
    from sqlite import SQLite
except ImportError:
    SQLite = optional_imports.NotInstalled(name='sqlite3')


try:
    from my_sql import MySQL
except ImportError:
    MySQL = optional_imports.NotInstalled(name='mysql')


try:
    from postgres import Postgres
except ImportError:
    Postgres = optional_imports.NotInstalled(name='psycopg2')


try:
    from elastic_search import ElasticSearch
except ImportError:
    ElasticSearch = optional_imports.NotInstalled(name='elasticsearch')


try:
    from mongo_db import MongoDb
except ImportError:
    MongoDb = optional_imports.NotInstalled(name='pymongo')


try:
    from influx_db import InfluxDb
except ImportError:
    InfluxDb = optional_imports.NotInstalled(name='influxdb')