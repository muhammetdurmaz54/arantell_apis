import os

from pymongo import MongoClient

from schema.ship_configs import document as ship_configs_doc
from schema.ship_stats import document as ship_stats_doc
from schema.daily_data import document as daily_data_doc
from schema.main_db import document as main_db__doc
from src.db.setup_mongo import connect_db

db = connect_db()


#print(db.ship_configs.insert_one(ship_configs_doc).inserted_id)
#print(db.ship_stats.insert_one(ship_stats_doc).inserted_id)
#print(db.daily_data.insert_one(daily_data_doc).inserted_id)
print(db.main_db.insert_one(main_db__doc).inserted_id)


