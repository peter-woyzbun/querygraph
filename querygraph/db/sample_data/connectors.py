from querygraph.db.connectors import SQLite
from querygraph.db.sample_data import flights, carriers, weather


flights_connector = SQLite(host=flights)
carriers_connector = SQLite(host=carriers)
weather_connector = SQLite(host=weather)