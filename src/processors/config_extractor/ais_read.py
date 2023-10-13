# not react application backend, this code is for reading ais data from zylalabs api and storing in database in each secified hours repectively


import requests
# import pandas
# from bs4 import BeautifulSoup
# import re
from pymongo import MongoClient
import os
from dotenv import load_dotenv
# from apscheduler.schedulers.background import BackgroundScheduler
import time
import datetime



load_dotenv()

MONGODB_URI = os.getenv('MONGODB_ATLAS')
TOKEN_TYPE = os.getenv('AUTHORIZATION_TOKEN_TYPE')
ACCESS_TOKEN = os.getenv('AUTHORIZATION_ACCESS_TOKEN')
AUTHORIZATION_STRING = TOKEN_TYPE + " " + ACCESS_TOKEN
client = MongoClient(MONGODB_URI)

# client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
database = db



# print(AUTHORIZATION_STRING)

def get_route(url, return_dict):
    raw_json = requests.get(url,headers={'Authorization': AUTHORIZATION_STRING}).json()
    # print(raw_json)
    # print(raw_json['status'])
    if raw_json['success']==True:
        return_dict['departure_port'] = raw_json['data']['departure_port']
        return_dict['departure_atd'] = raw_json['data']['departure_atd']
        return_dict['arrival_port'] = raw_json['data']['arrival_port']
        return_dict['arrival_atd'] = raw_json['data']['arrival_atd']
        return_dict['get_route_success'] = True
        return return_dict
    else:
        return_dict['departure_port'] = None
        return_dict['departure_atd'] = None
        return_dict['arrival_port'] = None
        return_dict['arrival_atd'] = None
        return_dict['get_route_success'] = False
        return return_dict
def get_current_position(url, return_dict):
    raw_json = requests.get(url,headers={'Authorization': AUTHORIZATION_STRING}).json()
    # print(raw_json)
    # print(raw_json['status'])
    if raw_json['success']==True:
        latitude, longitude = raw_json['data']['latitude_longitude'].split(' / ')
        speed, course = raw_json['data']['speed_course'].split(' / ')
        position_received = raw_json['data']['position_received']
        heading = None
        return_dict['latitude'] = latitude
        return_dict['longitude'] = longitude
        return_dict['speed'] = speed
        return_dict['course'] = course
        return_dict['position_received'] = position_received
        return_dict['heading'] = heading
        return_dict['get_current_position_success'] = True

        return return_dict
    else:
        heading = None
        return_dict['latitude'] = None
        return_dict['longitude'] = None
        return_dict['speed'] = None
        return_dict['course'] = None
        return_dict['position_received'] = None
        return_dict['heading'] = None
        return_dict['get_current_position_success'] = False
        return return_dict

def job(ship_imo):
    ais_collection=database.get_collection("Ais_collection")
    return_dict = {}
    return_dict['departure_port'] = None
    return_dict['departure_atd'] = None
    return_dict['arrival_port'] = None
    return_dict['arrival_atd'] = None
    return_dict['latitude'] = None
    return_dict['longitude'] = None
    return_dict['speed'] = None
    return_dict['course'] = None
    return_dict['position_received'] = None
    return_dict['heading'] = None
    return_dict['get_route_success'] = False
    return_dict['get_current_position_success'] = False
    return_dict = get_route("https://zylalabs.com/api/1835/vessel+information+and+route+tracking+api/1499/get+route?imoCode={}".format(int(ship_imo)), return_dict)
    return_dict = get_current_position('https://zylalabs.com/api/1835/vessel+information+and+route+tracking+api/1575/get+current+position?imoCode={}'.format(int(ship_imo)), return_dict)
    return_dict['Date Captured'] = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
    return_dict['Ship Imo'] = str(ship_imo)

    if ship_imo == '9595917':
        data = requests.get('https://zylalabs.com/api/1835/vessel+information+and+route+tracking+api/1592/get+position+by+mmsi?mmsiCode=538004383', headers={'Authorization': AUTHORIZATION_STRING}).json()
        if data['success'] == True:
            return_dict['data'] = data['data']
        else:
            return_dict['data'] = None
    else:
        data = requests.get('https://zylalabs.com/api/1835/vessel+information+and+route+tracking+api/1592/get+position+by+mmsi?mmsiCode=538006786', headers={'Authorization': AUTHORIZATION_STRING}).json()
        if data['success'] == True:
            return_dict['data'] = data['data']
        else:
            return_dict['data'] = None

    ais_collection.insert_one(return_dict)

def main_job():
    ship_imo_list = ['9595917', '9205926']

    for i in ship_imo_list:
        job(i)


# bs = BackgroundScheduler()
# bs.add_job(job, trigger='interval', hours=2)
# bs.start()

while True:
    print("STARTED")
    main_job()
    print("INSERTED")
    time.sleep(7200)