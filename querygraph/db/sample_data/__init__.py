import os

SAMPLE_DATA_DIR = os.path.dirname((os.path.abspath(__file__)))

flights = os.path.join(SAMPLE_DATA_DIR, 'flights.db')
weather = os.path.join(SAMPLE_DATA_DIR, 'weather.db')
carriers = os.path.join(SAMPLE_DATA_DIR, 'carriers.db')

