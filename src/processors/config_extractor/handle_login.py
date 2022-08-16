import base64
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

class Login():
    def __init__(self, username, password):
        self.username = username
        self.password = password
    
    def connect_db(self):
        client = MongoClient(os.getenv('MONGODB_ATLAS'))
        database = client.get_database('aranti')
        login_collection = database.get_collection('login_info')
        return login_collection
    
    def check_login_credentials(self):
        ''' Checks the login credentials of a user and returns user information if credentials match.'''
        login_info_collection = self.connect_db()
        result={}

        email_list = login_info_collection.distinct('organization_email')

        if self.username in email_list:
            for doc in login_info_collection.find({'organization_email': self.username}):
                if 'admin' in self.username:
                    # enc_pwd = self.password.encode('utf-8')
                    # base_enc_pwd = base64.b64encode(enc_pwd)
                    if self.password == doc['password']:
                        result['id'] = doc['organization_id']
                        result['name'] = doc['organization_name']
                        return result
                    else:
                        result = None
                        return result
                else:
                    enc_pwd = self.password.encode('utf-8')
                    base_enc_pwd = base64.b64encode(enc_pwd)
                    if base_enc_pwd == doc['password']:
                        result['id'] = doc['organization_id']
                        result['name'] = doc['organization_name']
                        return result
                    else:
                        result = None
                        return result
        else:
            result = None
            return result