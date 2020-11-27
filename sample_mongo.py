from pymongo import MongoClient
from schema.ship_configs import document as ship_configs_doc
from schema.ship_stats import document as ship_stats_doc
from schema.daily_data import document as daily_data_doc


client = MongoClient()
db = client.aranti

print(db.ship_configs.insert_one(ship_configs_doc).inserted_id)
print(db.ship_stats.insert_one(ship_stats_doc).inserted_id)
print(db.daily_data.insert_one(daily_data_doc).inserted_id)



