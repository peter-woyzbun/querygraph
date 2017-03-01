from querygraph.db.connectors import SQLite
from querygraph.db.test_data import daily_ts_path, hourly_ts_path, month_seasons_path


daily_ts_connector = SQLite(host=daily_ts_path)
hourly_ts_connector = SQLite(host=hourly_ts_path)
mo_seasons_connector = SQLite(host=month_seasons_path)
