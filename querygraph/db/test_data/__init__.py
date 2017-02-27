import os

TEST_DATA_DIR = os.path.dirname((os.path.abspath(__file__)))

# Time series databases.
TS_DIR = os.path.join(TEST_DATA_DIR, 'ts')

daily_ts_path = os.path.join(TS_DIR, 'daily_ts.db')
hourly_ts_path = os.path.join(TS_DIR, 'hourly_ts.db')


