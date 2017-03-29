Query Graph
===========

Query Graph is a framework/language for joining data from different
database management systems - i.e. joins that can't typically be 
accomplished with a single query. For example, joining Postgres data
and Mongo Db data.

![QGL Syntax Railroad Diagram](docs/_static/images/syntax_diagram.png)

```
CONNECT
    postgres_conn <- Postgres(db_name='%s', user='%s', password='%s', host='%s', port='%s')
    mongodb_conn <- Mongodb(host='%s', port='%s', db_name='%s', collection='%s')
RETRIEVE
    QUERY |
        {'tags': {'$in': {% album_tags -> list:str %}}};
    FIELDS album, tags
    USING mongodb_conn
    THEN |
        flatten(tags) >>
        remove(_id);
    AS mongo_node
    ---
    QUERY |
        SELECT *
        FROM "Album"
        WHERE "Title" IN {{ album -> list:str }};
    USING postgres_conn
    AS postgres_node
JOIN
    LEFT (postgres_node[Title] ==> mongo_node[album])
```

## Query Graph Language

A QGL query consists of three primary 'blocks': the `CONNECT` block, 
the `RETRIEVE` block, and the `JOIN` block. The `CONNECT` block 
established database connections, the `RETRIEVE` block creates query
nodes, and the `JOIN` describes how the nodes are joined.

![QGL Syntax Railroad Diagram](docs/_static/images/syntax_railroad.png)

### Connect Block

Each connector in the `CONNECT` block points a name to a database
type and associated credentials. The railroad diagram below outlines
the syntax.

![Connect Block Railroad Diagram](docs/_static/images/connect_block.png)

### Retrieve Block

The `RETRIEVE` block defines the graph's query nodes.

![Retrieve Block Railroad Diagram](docs/_static/images/retrieve_block.png)

### Join Block

The `JOIN` block defines how the graph's query nodes are joined.

![Join Block Railroad Diagram](docs/_static/images/join_block.png)