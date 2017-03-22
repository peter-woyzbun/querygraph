from querygraph.utils import optional_import


try:
    from _sqlite import SqliteConnector as Sqlite
except ImportError:
    Sqlite = optional_import.NotInstalled(name='sqlite3')


try:
    from _postgres import PostgresConnector as Postgres
except ImportError:
    Postgres = optional_import.NotInstalled(name='psycopg2')


try:
    from _mysql import MySqlConnector as MySql
except ImportError:
    MySql = optional_import.NotInstalled(name='mysql-connector-python')


try:
    from _mongodb import MongoDbConnector as MongoDb
except ImportError:
    MongoDb = optional_import.NotInstalled(name='pymongo')


try:
    from _elasticsearch import ElasticSearchConnector as ElasticSearch
except ImportError:
    ElasticSearch = optional_import.NotInstalled(name='elasticsearch')

try:
    from _cassandra import CassandraConnector as Cassandra
except ImportError:
    Cassandra = optional_import.NotInstalled(name='cassandra-driver')

try:
    from _redis import RedisConnector as Redis
except ImportError:
    Redis = optional_import.NotInstalled(name='redis')