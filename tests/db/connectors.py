from querygraph.db import connectors
from tests import config


sqlite_chinook = connectors.SQLite(host=config.DATABASES['sqlite']['HOST'])


postgres_chinook = connectors.Postgres(host=config.DATABASES['postgres']['HOST'],
                                       user=config.DATABASES['postgres']['USER'],
                                       password=config.DATABASES['postgres']['PASSWORD'],
                                       port=config.DATABASES['postgres']['PORT'],
                                       db_name=config.DATABASES['postgres']['DB_NAME'])


mysql_chinook = connectors.MySQL(host=config.DATABASES['mysql']['HOST'],
                                 user=config.DATABASES['mysql']['USER'],
                                 password=config.DATABASES['mysql']['PASSWORD'],
                                 db_name=config.DATABASES['mysql']['DB_NAME'])


mongodb_albums = connectors.MongoDb(host=config.DATABASES['mongodb']['HOST'],
                                    port=config.DATABASES['mongodb']['PORT'],
                                    db_name=config.DATABASES['mongodb']['DB_NAME'],
                                    collection='albums')