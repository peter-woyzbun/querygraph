from querygraph.db.connectors import SQLite
from querygraph.db.sample_data import flights, carriers, weather


flights_connector = SQLite(host=flights)