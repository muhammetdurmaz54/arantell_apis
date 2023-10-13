# react application backend for getting vti value in daily report

import sys
import os
from dotenv import load_dotenv
import pandas
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from datetime import date, datetime
import numpy as np
from pymongo import MongoClient
from pymongo import ASCENDING
from dateutil.relativedelta import relativedelta
log = CommonLogger(__name__,debug=True).setup_logger()
from bson import json_util
import time
from pandas.core.indexes.datetimes import date_range
load_dotenv()

MONGODB_URI = os.getenv('MONGODB_ATLAS')
client = MongoClient(MONGODB_URI)

# client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
database = db




def vti_process(imo):
    maindb = database.get_collection("Main_db")
    tempList=[]
    final_vti=None
    avg_vti=None
    for doc in maindb.find({'ship_imo': int(imo),'vessel_loaded_check':"Loaded"}, {'vti': 1}).sort('final_rep_dt', ASCENDING):
        try:
            if pandas.isnull(doc['vti'])==False:
                tempList.append(doc['vti'])
        except:
            tempList.append(None)
        try:
            final_vti=round(tempList[-1],2)
            avg_vti=round(np.average(tempList[-40]),2)
        except:
            final_vti=None
            avg_vti=None
    # print(tempList[-1],np.average(tempList[-30]))
    return final_vti,avg_vti


# vti_process(9205926)