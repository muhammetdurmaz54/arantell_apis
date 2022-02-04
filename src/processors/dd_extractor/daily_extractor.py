from math import nan
from os import error
import re
import sys
from timeit import repeat 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.helpers.check_status import check_status
from mongoengine import *
from datetime import datetime
import pandas as pd
import numpy as np
from pymongo import ASCENDING,DESCENDING

log = CommonLogger(__name__, debug=True).setup_logger()
# connect("aranti")




class DailyDataExtractor:
    def __init__(self,fuelfile,engfile,imo,override):
        self.fuel=fuelfile
        self.eng=engfile
        self.imo=int(imo)
        self.error = False
        self.override=override
        


    def do_steps(self):
        self.connect()
        inserted_id = self.dailydata_insert()
        # if self.error:
        #     return False, str(self.traceback_msg)
        # else:
        #     return True, str(inserted_id)
 
    def connect(self):
        self.db = connect_db()

    def dailydata_insert(self):
        if self.fuel!=None:
            fuel = pd.DataFrame(self.fuel)
            database=self.db.get_database("aranti")
            ship_configs_collection=database.get_collection("ship")
            self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
            daily_data_collection =database.get_collection("daily_data")
            ship_name=self.ship_configs['ship_name']
            data_available_nav=self.ship_configs['data_available_nav']
            data_available_engine=self.ship_configs['data_available_engine']
            identifier_mapping=self.ship_configs['identifier_mapping']
            input_rep_dt=fuel[identifier_mapping["rep_dt"]][0].strip()
            input_timestamp=int(fuel['timestamp'][0])
            try:
                try:
                    try:
                        rep_dt = datetime.strptime(input_rep_dt, '%d/%m/%y %H:%M:%S')
                    except:
                        rep_dt = datetime.strptime(input_rep_dt, '%d/%m/%y')
                except:
                    rep_dt = datetime.strptime(input_rep_dt, '%d-%m-%y %H:%M:%S')
            except:
                rep_dt = datetime.strptime(input_rep_dt, '%d-%m-%y')
            ship_imos=daily_data_collection.distinct("ship_imo")
            if self.imo in ship_imos:
                # try:
                rep_dt_list=daily_data_collection.distinct("data.rep_dt")
                daily_data=None
                for dates in rep_dt_list:
                    if dates.date()==rep_dt.date():
                        self.dates=dates
                        daily_data=daily_data_collection.find({"ship_imo":self.imo,"data.rep_dt":dates,"timestamp":input_timestamp})[0]
                if daily_data:
                    if 'data_available_engine' in daily_data and len(daily_data['data_available_engine'])>0 and daily_data['engine_data_available']==True:
                        historical=False
                        nav_data_available=True
                        previous_data=daily_data['data']
                        data=self.getdata(fuel,data_available_nav,identifier_mapping,previous_data)
                        data['rep_dt']=self.dates
                        daily_data_collection.update_one(daily_data_collection.find({"ship_imo":self.imo,"data.rep_dt":self.dates,"timestamp":input_timestamp})[0],{"$set":{"historical":False,"nav_data_available":True,"data_available_nav":data_available_nav,"data":data}})
                        print(data)
                        return "engine data avaailable and fuel data updated"

                    elif 'data_available_nav' in daily_data and len(daily_data['data_available_nav'])>0 and daily_data['nav_data_available']==True:
                        print("hereeeee")
                        if self.override==True:
                            historical=False
                            nav_data_available=True
                            previous_data=daily_data['data']
                            data=self.getdata(fuel,data_available_nav,identifier_mapping,previous_data)
                            data['rep_dt']=self.dates
                            daily_data_collection.update_one(daily_data_collection.find({"ship_imo":self.imo,"data.rep_dt":self.dates,"timestamp":input_timestamp})[0],{"$set":{"historical":False,"nav_data_available":True,"data_available_nav":data_available_nav,"data":data}})
                            return "eng data not there but fuel data override"
                        elif self.override==False:
                            return "fuel data already present"   
                    
                else:
                    data=self.getdata(fuel,data_available_nav,identifier_mapping,{})
                    data['rep_dt']=rep_dt
                    daily_nav={
                        "ship_imo":self.imo,
                        "ship_name":ship_name,
                        "date":datetime.now(),
                        "timestamp":input_timestamp,
                        "historical":False,
                        "nav_data_available":True,
                        "engine_data_available":False,
                        "nav_data_details":{"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                        "engine_data_details":None,
                        "data_available_nav":data_available_nav,
                        "data_available_engine":[],
                        "data":data
                    }
                    daily_data_collection.insert_one(daily_nav).inserted_id
                    return "inserted new fuel data"

        elif self.eng!=None:
            eng = pd.DataFrame(self.eng)
            database=self.db.get_database("aranti")
            ship_configs_collection=database.get_collection("ship")
            self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
            daily_data_collection =database.get_collection("daily_data")
            ship_name=self.ship_configs['ship_name']
            data_available_nav=self.ship_configs['data_available_nav']
            data_available_engine=self.ship_configs['data_available_engine']
            identifier_mapping=self.ship_configs['identifier_mapping']
            input_rep_dt=eng[identifier_mapping["rep_dt"]][0].strip()
            input_timestamp=int(eng['timestamp'][0])
            try:
                try:
                    try:
                        rep_dt = datetime.strptime(input_rep_dt, '%d/%m/%y %H:%M:%S')
                    except:
                        rep_dt = datetime.strptime(input_rep_dt, '%d/%m/%y')
                except:
                    rep_dt = datetime.strptime(input_rep_dt, '%d-%m-%y %H:%M:%S')
            except:
                rep_dt = datetime.strptime(input_rep_dt, '%d-%m-%y')
            ship_imos=daily_data_collection.distinct("ship_imo")
            if self.imo in ship_imos:
                # try:
                rep_dt_list=daily_data_collection.distinct("data.rep_dt")
                daily_data=None
                for dates in rep_dt_list:
                    if dates.date()==rep_dt.date():
                        self.dates=dates
                        daily_data=daily_data_collection.find({"ship_imo":self.imo,"data.rep_dt":dates,"timestamp":input_timestamp})[0]
                if daily_data:
                    if 'data_available_nav' in daily_data and len(daily_data['data_available_nav'])>0 and daily_data['nav_data_available']==True:
                        historical=False
                        eng_data_available=True
                        previous_data=daily_data['data']
                        data=self.getdata(eng,data_available_engine,identifier_mapping,previous_data)
                        data['rep_dt']=self.dates
                        print(data)
                        daily_data_collection.update_one(daily_data_collection.find({"ship_imo":self.imo,"data.rep_dt":self.dates,"timestamp":input_timestamp})[0],{"$set":{"historical":False,"engine_data_available":True,"data_available_engine":data_available_engine,"data":data}})
                        return "fuel data available and eng ddata updated"
                    elif 'data_available_engine' in daily_data and len(daily_data['data_available_engine'])>0 and daily_data['engine_data_available']==True:
                        print("hereeeee")
                        if self.override==True:
                            historical=False
                            eng_data_available=True
                            previous_data=daily_data['data']
                            data=self.getdata(eng,data_available_engine,identifier_mapping,previous_data)
                            data['rep_dt']=self.dates
                            daily_data_collection.update_one(daily_data_collection.find({"ship_imo":self.imo,"data.rep_dt":self.dates,"timestamp":input_timestamp})[0],{"$set":{"historical":False,"engine_data_available":True,"data_available_engine":data_available_engine,"data":data}})
                            return "fuel data not there but eng data override"
                        else:
                            return "eng data already present"
                    
                else:
                    data=self.getdata(eng,data_available_engine,identifier_mapping,{})
                    data['rep_dt']=rep_dt
                    daily_nav={
                        "ship_imo":self.imo,
                        "ship_name":ship_name,
                        "date":datetime.now(),
                        "timestamp":input_timestamp,
                        "historical":False,
                        "nav_data_available":False,
                        "engine_data_available":True,
                        "nav_data_details":None,
                        "engine_data_details":{"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"},},
                        "data_available_nav":[],
                        "data_available_engine":data_available_engine,
                        "data":data
                    }
                    daily_data_collection.insert_one(daily_nav).inserted_id    
                    return "inserted new engine data"
            



    def getdata(self,row,data_available_nav,identifier_mapping,dest):
        for w in data_available_nav:
            try:
                if w in row:
                    dest[w]=self.is_float(str(row[w].iloc[0]))
                elif identifier_mapping[w].strip() in row:
                    dest[w]=self.is_float(str(row[identifier_mapping[w]].iloc[0]))
            except KeyError:
                continue    
     
        return dest
    
    def is_float(self,floatnum):
        float_regex = '[+-]?[0-9]+\.[0-9]+'
        int_regex = '^[0-9]+$'
        if(re.search(float_regex, floatnum)): 
            try:
                return float(floatnum)
            except:
                return floatnum
        elif(re.search(int_regex,floatnum)):
            try:
                return int(floatnum)
            except:
                return floatnum
        else:
            return floatnum


obj=DailyDataExtractor(None,{"timestamp":[1400],"pwr": [11000],"ship_imo": [9591301],"rep_dt": ['22-3-18'],"ext_tempavg":[228]},9591301,True)
obj.connect()
msg=obj.dailydata_insert()
print(msg)
