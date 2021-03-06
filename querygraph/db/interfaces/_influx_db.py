import datetime
import time

from influxdb import DataFrameClient

from querygraph.db.interface import DatabaseInterface
from querygraph.db.type_converter import TypeConverter


class InfluxDb(DatabaseInterface):

    TYPE_CONVERTER = TypeConverter(
        type_converters={
            # Render type 'abstime' - "epoch time".
            # Parameter string: '<parameter_expression> -> [<container_type>:]abstime'
            'abstime': {
                datetime.datetime: lambda x: "%ss" % int(time.mktime(x.timetuple())),
                str: lambda x: "%s" % x,
                unicode: lambda x: "%s" % x,
                float: lambda x: "%ss" % int(x),
                int: lambda x: "%ss" % x,
            }
        }
    )

    def __init__(self, name, host, port, user, password, db_name):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        DatabaseInterface.__init__(self,
                                   name=name,
                                   db_type="InfluxDB",
                                   conn_exception=Exception,
                                   execution_exception=Exception,
                                   type_converter=TypeConverter())

    def _conn(self):
        conn = DataFrameClient(self.host, self.port, self.user, self.password, self.db_name)
        return conn

    def _execute_query(self, query):
        conn = self.conn()
        df = conn.query(query)
        return df

    def execute_insert_query(self, query):
        pass