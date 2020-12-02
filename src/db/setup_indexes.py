
from pymongo import MongoClient
from pprint import pprint

from src.db.setup_mongo import connect_db

database = connect_db()

col = database.daily_data

compound_index = [("ship_imo", 1),("date", 1)]
single_index = [("ship_imo", 1)]

database.daily_data.create_index(compound_index,unique=True)
database.main_db.create_index(compound_index,unique=True)
database.report_data.create_index(compound_index,unique=True)
database.ship_events.create_index(compound_index,unique=True)
database.ship_configs.create_index(single_index,unique=True)
database.ship_stats.create_index(single_index,unique=True)
database.training_logs.create_index(single_index,unique=True)

# No indexes for model_configs

pprint(database.daily_data.index_information())
pprint(database.main_db.index_information())
pprint(database.report_data.index_information())
pprint(database.ship_events.index_information())
pprint(database.ship_configs.index_information())
pprint(database.ship_stats.index_information())
pprint(database.training_logs.index_information())

