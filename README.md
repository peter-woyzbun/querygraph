Query Graph
===========


```
CONNECT
    sqlite_conn <- Sqlite(host=...)
    postgres_conn <- Postgres(host=..., user=...)
    mysql_conn <- MySql(host=...user=...)
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
```


```
CONNECT
    mongodb_conn <- MongDb(host='', client='',...)
    postgres_conn <- Postgres(host='', user='',...)
RETRIEVE
    QUERY |
        {"tags" : {"$in" : {% user_tags|value_list:str %} }};
    FIELDS user_id, field_x
    USING mongodb_conn
    AS mongo_node
    ---
    QUERY |
        SELECT *
        FROM user
        WHERE user_id IN {{ mongo_node.user_id|value_list:int }};
    USING postgres_conn
    AS postgres_node
JOIN
    LEFT (postgres_node[user_id] ==> mongo_node[user_id]);
```

## Query Graph Language

A QGL query consists of three primary 'blocks': the `CONNECT` block, 
the `RETRIEVE` block, and the `JOIN` block. The `CONNECT` block 
established database connections, the `RETRIEVE` block creates query
nodes, and the `JOIN` describes how the nodes are joined.

![QGL Syntax Railroad Diagram](docs/_static/images/qgl_syntax.png)

### Connect Block

![QGL Syntax Railroad Diagram](docs/_static/images/connect_block.png)