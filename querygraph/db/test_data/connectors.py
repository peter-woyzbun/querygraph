from querygraph.db.connectors import SQLite
from querygraph.db.test_data import daily_ts_path, hourly_ts_path


daily_ts_connector = SQLite(host=daily_ts_path)
hourly_ts_connector = SQLite(host=hourly_ts_path)

