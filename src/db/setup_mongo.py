import os
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()


def connect_db():
    client = MongoClient(os.getenv('MONGODB_URI'))
    return  client.arantell

