from pymongo import MongoClient
import base64
import os
from dotenv import load_dotenv

load_dotenv()


class Change_Password():
    def __init__(self, id, new_password):
        self.id = int(id)
        self.new_password = new_password
        # self.ship_imo = int(ship_imo)
    
    def insert_into_collection(self):
        client = MongoClient(os.getenv('MONGODB_ATLAS'))
        db=client.get_database("aranti")
        login_info_collection = db.get_collection('login_info')
        new_password_enc = self.new_password.encode('utf-8')
        final_new_password_enc = base64.b64encode(new_password_enc)

        result = login_info_collection.update_one(
            # login_info_collection.find({'id': self.id})[0]
            {'organization_id': self.id},
            {'$set': {'password': final_new_password_enc}}
        )

        return result