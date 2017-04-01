<img src="docs/_static/images/qg_logo.png" alt="Drawing" />

***

Query Graph is a framework/language, written in Python, for joining data 
from different database management systems - i.e. joins that can't 
typically be accomplished with a single query. For example, joining 
Postgres data and Mongo Db data. It also provides tools for easily 
converting non-tabular data (e.g. JSON) into tabular form.

The following databases are currently supported:

* Sqlite
* MySQL
* Postgres
* Mongo Db
* Elastic Search
* Apache Cassandra (untested)
* Maria Db (untested)
* InfluxDB (untested)
* MS Sql (untested)

## Main Features

* Join data from any number of different database types in a single query.
* Manipulate data using "manipulation sets", which are chained together
  statements very similar to those used in the `dplyr` package for R.
  ```
  mutate(new_col=log(old_col)) >>
  remove(some_column)
  ```
* Easily transform JSON-like data into relational form.
* Threading can optionally be used to run queries on different databases
  simultaneously, based the structure of the query graph.

## Getting Started

* To install Query Graph, see below.
* For a brief introduction to Query Graph, see here.
* For a more complete introduction, see here.
* For documentation, see here.

## Installation

To install Query Graph...

## Query Graph Language - Brief Introduction

Query Graph Language (QGL) is a simple, domain specific declarative 
language. The best way to give an idea of how it works is through
an example. In the example, we'll be joining data from two databases:
a Mongo Db database, and a Postgres database.

### The Mongo Db Database

```
{
      "album": "Jagged Little Pill",
      "tags": [
        "canada",
        "pop rock",
        "post-grunge",
        "female"
      ],
      "data": {"record_label": "Maverick",
               "year": 1995}
}
```


Below is a QGL query that joins data from a Mongo DB
database and a Postgres database.

```
CONNECT
    postgres_conn <- Postgres(db_name='', user='', password='', host='', port='')
    mongodb_conn <- Mongodb(host='', port='', db_name='', collection='')
RETRIEVE
    QUERY |
        {'tags': {'$in': {% album_tags -> list:str %}}};
    FIELDS album, tags
    USING mongodb_conn
    THEN |
        unpack(record_label=data['record_label']) >>
        remove(data) >>
        flatten(tags);
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

To execute the query in Python, the code is

```python
query_str = "..."

# Create the graph instance.
query_graph = QueryGraph(qgl_str=query_str)

# Execute the graph and get a dataframe in return.
df = query_graph.execute(album_tags=['canada', 'rock'])
```