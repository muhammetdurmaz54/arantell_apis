from asyncio.windows_events import NULL
from math import nan
from os import error
import os
import sys
from xmlrpc.client import _datetime_type 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.helpers.check_status import check_status
from src.db.schema.ship import Ship # importing ship config schema
from src.db.schema.ddschema import DailyData  # importing dd schema
from mongoengine import *
from datetime import datetime
import pandas as pd
import numpy as np
import ntpath
import boto3

tabs = pd.ExcelFile('F:\Afzal_cs\Internship\Arvind data files\9592301logsengine_tesing.xlsx').sheet_names 
dataframe_dicts={}
maindata=pd.DataFrame({"rep_dt":[],"timestamp":[]})

j=0
for i in tabs:
    tempdata = pd.read_excel('F:\Afzal_cs\Internship\Arvind data files\9592301logsengine_tesing.xlsx',sheet_name=i,skiprows = [1, 2, 3]).fillna("  ")
    if "rep_dt" and "timestamp" in tempdata.columns:
        dataframe_dicts["eng_"+str(j)]=tempdata
        if len(dataframe_dicts["eng_"+str(j)]['rep_dt'])>len(maindata['rep_dt']):
            maindata=pd.merge(maindata,dataframe_dicts["eng_"+str(j)],on=["rep_dt",'timestamp'],how="right")
        elif len(dataframe_dicts["eng_"+str(j)]['rep_dt'])<len(maindata['rep_dt']):
            maindata=pd.merge(maindata,dataframe_dicts["eng_"+str(j)],on=["rep_dt",'timestamp'],how="left")
        # print(maindata)
    j=j+1
maindata = maindata.drop_duplicates(subset=["rep_dt", "timestamp"])
maindata=maindata.reset_index(drop=True)
print(maindata)