import unittest

from querygraph.graph import QueryGraph


class ReadTests(unittest.TestCase):

    def test_read(self):
        query = """
        CONNECT
            postgres_conn <- Postgres(db_name='', user='', password='', host='', port='')
            mongodb_conn <- Mongodb(host='', port='', db_name='', collection='')
        RETRIEVE
            QUERY |
                {'tags': {'$in': {% album_tags|value_list:str %}}};
            FIELDS album
            USING mongodb_conn
            AS mongo_node
            ---
            QUERY |
                SELECT *
                FROM "Album"
                WHERE "Title" IN {{ album|value_list:str }};
            USING postgres_conn
            AS postgres_node
        JOIN
            LEFT (postgres_node[Title] ==> mongo_node[album])
        """
        query_graph = QueryGraph(qgl_str=query)
        self.assertTrue('mongo_node' in query_graph.nodes)


def main():
    unittest.main()

if __name__ == '__main__':
    main()