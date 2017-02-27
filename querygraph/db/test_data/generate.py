import pandas as pd
import numpy as np


# =============================================
# Time Series DataFrames
# ---------------------------------------------

def generate_daily_ts():
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
    return df


def generate_hourly_ts():
    """ Generate an hourly time series dataframe. """
    # Create datetime index range.
    dt_index = pd.date_range(start='2016-01-01', end='2016-07-01', freq='h')
    # Convert index to series.
    dt_series = dt_index.to_series()
    # Convert series to dataframe.
    df = dt_series.to_frame(name='datetime_a')
    # Reset the index and only keep the 'datetime_a' column.
    df = df.reset_index()
    df = df[['datetime_a']]
    # Create second date column with different format.
    df['datetime_b'] = df['datetime_a'].dt.strftime('%m/%d/%Y %H:%M:%S')
    # Create extra columns.
    df['hour'] = df['datetime_a'].dt.hour
    df['is_evening'] = df['datetime_a'].dt.hour > 17
    return df


# =============================================
# Object DataFrames
# ---------------------------------------------


def generate_month_seasons():

    month = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    season = ['winter', 'winter', 'winter', 'spring', 'spring', 'summer', 'summer', 'summer', 'fall', 'fall', 'winter',
              'winter']

    df = pd.DataFrame({'month': month, 'season': season})
    return df
