import os


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))


# =================================================
# TEST DATA CONFIG
# -------------------------------------------------

# Directory containing SQL files for data generating.
DB_SQL_DIR = os.path.join(TESTS_DIR, 'db', 'data')

# Paths for SQL files that generate relation database tables.
DB_SQL_PATHS = {
    'sqlite': os.path.join(DB_SQL_DIR, 'Chinook_Sqlite.sql'),
    'mysql': os.path.join(DB_SQL_DIR, 'Chinook_MySql.sql'),
    'postgres': os.path.join(DB_SQL_DIR, 'Chinook_PostgreSql.sql'),
}

DATABASES = {
    'sqlite': {
        'HOST': os.path.join(DB_SQL_DIR, 'Chinook_Sqlite.sqlite'),
    },
    'postgres': {
        'HOST': 'localhost',
        'USER': 'postgres',
        'PASSWORD': 'Postgres-126',
        'PORT': '5432',
        'DB_NAME': 'query_graph_test',
    },
    'mysql': {
        'HOST': 'localhost',
        'USER': 'admin',
        'PASSWORD': 'MySql-126',
        'DB_NAME': 'chinook',
    },
    'mongodb': {
        'HOST': 'localhost',
        'PORT': 27017,
        'DB_NAME': 'querygraph-test'
    },
    'elasticsearch': {
        'HOST': 'localhost',
        'PORT': 9200,
        'DOC_TYPE': 'chinook',
        'INDEX': 'album'
    }
}