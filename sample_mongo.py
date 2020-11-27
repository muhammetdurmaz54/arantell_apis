from pymongo import MongoClient

collection = "daily_data"

client = MongoClient()
db = client.test
collection = db.main

print(collection.insert_one(a).inserted_id)





# Questions:

"""
1. Which Date Time format to use for faster indexing? MongoDB ISODate(), Date() or linux epoch time for faster matching?
2. 
"""

