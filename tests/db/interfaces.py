from querygraph.db import interfaces

from tests import config


sqlite_chinook = interfaces.Sqlite(name='sqlite_chinook', host=config.DATABASES['sqlite']['HOST'])


postgres_chinook = interfaces.Postgres(name='postgres_chinook',
                                       host=config.DATABASES['postgres']['HOST'],
                                       user=config.DATABASES['postgres']['USER'],
                                       password=config.DATABASES['postgres']['PASSWORD'],
                                       port=config.DATABASES['postgres']['PORT'],
                                       db_name=config.DATABASES['postgres']['DB_NAME'])


mysql_chinook = interfaces.MySql(name='mysql_chinook',
                                 host=config.DATABASES['mysql']['HOST'],
                                 user=config.DATABASES['mysql']['USER'],
                                 password=config.DATABASES['mysql']['PASSWORD'],
                                 db_name=config.DATABASES['mysql']['DB_NAME'],
                                 port=config.DATABASES['mysql']['PORT'])


mongodb_albums = interfaces.MongoDb(name='mongodb_albums',
                                    host=config.DATABASES['mongodb']['HOST'],
                                    port=config.DATABASES['mongodb']['PORT'],
                                    db_name=config.DATABASES['mongodb']['DB_NAME'],
                                    collection=config.DATABASES['mongodb']['COLLECTION'])


elastic_search_albums = interfaces.ElasticSearch(name='elastic_search_albums',
                                                 host=config.DATABASES['elasticsearch']['HOST'],
                                                 port=config.DATABASES['elasticsearch']['PORT'],
                                                 doc_type=config.DATABASES['elasticsearch']['DOC_TYPE'],
                                                 index=config.DATABASES['elasticsearch']['INDEX'])