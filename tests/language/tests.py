import unittest


from querygraph.language.compiler import QGLCompiler


class ConnectorTests(unittest.TestCase):

    def test_connectors(self):

        test_query = """
        CONNECT
            sqlite_conn <- Sqlite(host='fun host!')
            sqlite_conn_2 <- Sqlite(host='fun host!')
        RETRIEVE
        	QUERY |
        		SELECT *
                FROM month_seasons
                WHERE season IN {% seasons|value_list:str %};
        	USING sqlite_conn
        	AS NODE_X

        	QUERY |
        		{"tags" : {"$in" : {{ seasons_tags|value_list:str }} }};
        	USING sqlite_conn_2
        	AS NODE_Y
        JOIN
        	LEFT (NODE_Y[col_1, col_2] ==> NODE_X[col_1, col_2]);
        """

        query_compiler = QGLCompiler(qgl_str=test_query)
        query_graph = query_compiler.compile()

        self.assertTrue('sqlite_conn' and 'sqlite_conn_2' in query_compiler.connect_block.db_connectors)