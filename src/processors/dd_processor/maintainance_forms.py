# not react application backend, to create maintainance forms if needed

from datetime import date, datetime
from dotenv import load_dotenv
import sys
import os
from pymongo import MongoClient
import pandas as pd
from pymongo import ASCENDING

load_dotenv()

MONGODB_URI = os.getenv('MONGODB_ATLAS')
client = MongoClient(MONGODB_URI)

db=client.get_database("aranti")
database = db


def maintainance_forms(input_dict,ship_imo,update_flag):
    input_dict['fromDate']=datetime.strptime(input_dict["fromDate"], '%Y-%m-%d')
    input_dict['toDate']=datetime.strptime(input_dict["toDate"], '%Y-%m-%d')
    input_dict['ship_imo']=int(ship_imo)
    maintainance_collection=database.get_collection("maintainance_forms")

    for i in input_dict['jobs']:
        new_date=datetime.strptime(i['jobFromDate'], '%Y-%m-%d')
        i['jobFromDate']=new_date

    flag=False
    for i in maintainance_collection.find({"ship_imo":int(ship_imo)}):
        if i['fromDate']==input_dict['fromDate'] and i['toDate']==input_dict['toDate']:
            flag=True
        
        
        
    if flag==False:
        maintainance_collection.insert_one(input_dict)
        print("inserted no dup")
    else:
        if update_flag==False:
            print("dup")
        elif update_flag==True:
            prev_date=input_dict['previousDateSelection']
            spl=prev_date.split('-')
            prev_from=datetime.strptime(spl[0], '%Y/%m/%d')
            prev_to=datetime.strptime(spl[1], '%Y/%m/%d')
            maintainance_collection.delete_one({"ship_imo":int(ship_imo),'fromDate':prev_from,'toDate':prev_to})
            print("deltetrd")
            maintainance_collection.insert_one(input_dict)
            print("inserted")

    



input_dict={
    "fromDate": "2023-02-01",
    "toDate": "2023-02-02",
    "iv_category": "Dry Docking",
    "category_code": "DD",
    "category_description": "Dry Docking",
    "iv_type": "Major",
    "location": "location",
    "jobs": [
        {
            "jobFromDate": "2023-02-02",
            "jobToDate": "2023-02-03",
            "job_code": "Code1"
        },
        {
            "jobFromDate": "2023-02-03",
            "jobToDate": "2023-02-04",
            "job_code": "Code2"
        }
    ],
    "equipments": [
        {
            "equip_desc": "short description",
            "equip_code": "Code 1",
            "job_code": "Code 1"
        },
        {
            "equip_desc": "short description 2",
            "equip_code": "Code 2",
            "job_code": "Code 2"
        }
    ]
}
previousDateSelection="2023/02/01-2023/02/02"
input_dict['previousDateSelection']=previousDateSelection


maintainance_forms(input_dict,6185798,True)