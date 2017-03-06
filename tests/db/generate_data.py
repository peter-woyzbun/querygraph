import pandas as pd
import numpy as np


def daily_ts_df():
    """ Generate a daily time series dataframe. """
    # Create datetime index range.
    dt_index = pd.date_range(start='2016-01-01', end='2017-01-01', freq='D')
    # Convert index to series.
    dt_series = dt_index.to_series()
    # Convert series to dataframe.
    df = dt_series.to_frame(name='date_a')
    # Reset the index and only keep the 'date_a' column.
    df = df.reset_index()
    df = df[['date_a']]
    # Create second date column with different format.
    df['date_b'] = df['date_a'].dt.strftime('%m/%d/%Y')
    # Create extra columns.
    df['year'] = df['date_a'].dt.strftime('%Y')
    df['month'] = df['date_a'].dt.strftime('%m')
    df['month_name'] = df['date_a'].dt.strftime('%B')
    df['day'] = df['date_a'].dt.strftime('%d')
    df['weekday_name'] = df['date_a'].dt.weekday_name
    df['is_january'] = df['month'] == '01'
    df['is_friday'] = df['weekday_name'] == 'Friday'
    df['random_int'] = np.random.choice(range(1, 10), df.shape[0])
    df['id'] = df.index
    return df


def create_postgres_tables():
    pass


def create_postgres_daily_ts():

    query = """
    CREATE TABLE daily_ts (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
    """


def create_mongo_season_data():

    from pymongo import MongoClient

    client = MongoClient()

    db = client['querygraph-test']
    seasons = db['seasons']

    seasons_data = [{'season': 'Spring',
                     'tags': ['growing', 'melting', 'warm', 'dog poop']},
                    {'season': 'Summer',
                     'tags': ['sunny', 'hot', 'fun', 'beach', 'vacation']},
                    {'season': 'Fall',
                     'tags': ['colours', 'colder', 'leaves', 'halloween']},
                    {'season': 'Winter',
                     'tags': ['cold', 'snow', 'christmas', 'skiing']}]

    result = seasons.insert_many(seasons_data)
