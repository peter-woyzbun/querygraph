Query Graph
===========


```
CONNECT
            sqlite_chinook <- Sqlite(host='%s')
            postgres_chinook <- Postgres(host='%s', user='%s', password='%s', db_name='%s', port='%s')
            mysql_chinook <- MySql(host='%s', user='%s', password='%s', db_name='%s')
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