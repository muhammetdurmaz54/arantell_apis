from math import nan
from os import error
import os
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
import boto3
from io import StringIO

log = CommonLogger(__name__, debug=True).setup_logger()
# connect("aranti")




class DailyDataExtractor:
    def __init__(self,fuelfile,engfile,imo,logs,override):
        self.fuel=fuelfile
        self.eng=engfile
        self.imo=int(imo)
        self.logs=logs
        if logs==True:
            self.Noon=False
        elif logs==False:
            self.Noon=True
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

    def closest(self,lst, K):
        return lst[min(range(len(lst)), key = lambda i: abs(lst[i]-K))]

    def final_rep_dt(self,rep_dt,timestamp):
        timestamp_dict={"400":4,"800":8,"1200":12,"1600":16,"2000":20,"2400":0}
        stamp_list=[400,800,1200,1600,2000,2400]
        if self.logs==False or timestamp==None:
            final_rep_dt=rep_dt
        elif self.logs==True:
            logs_time=int(timestamp)
            final_log_time=self.closest(stamp_list,logs_time)
            final_rep_dt=rep_dt.replace(hour=timestamp_dict[str(final_log_time)])
        return final_rep_dt

    def create_data(self,full_dict):
        temp_dict={}
        for i in full_dict:
            try:
                del full_dict[i]['undefined']
            except:
                pass
            temp_dict[i]=len(full_dict[i]['rep_dt'])
        var=max(temp_dict, key= lambda x: temp_dict[x])
        maindata=pd.DataFrame(full_dict[var])
        for i in full_dict:
            if i!=var:
                data=pd.DataFrame(full_dict[i])
                maindata=pd.merge(maindata,data,on=["rep_dt",'timestamp'],how="left")
            # maindata=pd.concat([maindata,data],axis=1)

        # maindata = maindata.loc[:,~maindata.columns.duplicated()]
        maindata=maindata.sort_values(by=['timestamp'])
        maindata=maindata.reset_index(drop=True)
        return maindata


    def get_rep_dt(self,input_rep_dt):
        try:
            try:
                try:
                    try:
                        rep_dt = datetime.strptime(input_rep_dt, '%d/%m/%y %H:%M:%S')
                    except:
                        rep_dt = datetime.strptime(input_rep_dt, '%d/%m/%Y %H:%M:%S')
                except:
                    try:
                        rep_dt = datetime.strptime(input_rep_dt, '%d/%m/%y')
                    except:
                        rep_dt = datetime.strptime(input_rep_dt, '%d/%m/%Y')
            except:
                try:
                    rep_dt = datetime.strptime(input_rep_dt, '%d-%m-%y %H:%M:%S')
                except:
                    rep_dt = datetime.strptime(input_rep_dt, '%d-%m-%Y %H:%M:%S')
        except:
            try:
                rep_dt = datetime.strptime(input_rep_dt, '%d-%m-%y')
            except:
                rep_dt = datetime.strptime(input_rep_dt, '%d-%m-%Y')
        return rep_dt

    def fuel_insert(self,fuel,identifier_mapping,daily_data_collection,data_available_nav,ship_name,i):
        input_rep_dt=fuel[identifier_mapping["rep_dt"]][i].strip()
        try:
            input_timestamp=int(fuel[identifier_mapping['timestamp']][i])
        except:
            input_timestamp=None
        rep_dt = self.get_rep_dt(input_rep_dt)
                    
        ship_imos=daily_data_collection.distinct("ship_imo")
        if self.imo in ship_imos:
            # try:
            rep_dt_list=daily_data_collection.distinct("data.rep_dt")
            daily_data=None
            try:
                for dates in rep_dt_list:
                    if dates.date()==rep_dt.date():
                        try:
                            daily_data=daily_data_collection.find({"ship_imo":self.imo,"historical":False,"data.rep_dt":dates,"timestamp":input_timestamp})[0]
                            self.dates=dates
                        except:
                            continue
            except:
                daily_data=None
            if daily_data:
                if 'data_available_engine' in daily_data and len(daily_data['data_available_engine'])>0 and daily_data['engine_data_available']==True:
                    historical=False
                    nav_data_available=True
                    previous_data=daily_data['data']
                    previous_common_data=daily_data['common_data']
                    data=self.getdata(fuel,data_available_nav,identifier_mapping,previous_data, i)
                    previous_common_data.update(self.common_data)
                    new_common_data=previous_common_data
                    data['rep_dt']=self.dates
                    print(data['rep_dt'])
                    daily_data_collection.update_one(daily_data_collection.find({"ship_imo":self.imo,"historical":False,"data.rep_dt":self.dates,"timestamp":input_timestamp})[0],{"$set":{"historical":False,"nav_data_available":True,"data_available_nav":data_available_nav,"data":data,"common_data":new_common_data}})
                    print(data)
                    return input_rep_dt, input_timestamp

                elif 'data_available_nav' in daily_data and len(daily_data['data_available_nav'])>0 and daily_data['nav_data_available']==True:
                    print("hereeeee")
                    if self.override==True:
                        historical=False
                        nav_data_available=True
                        previous_data=daily_data['data']
                        data=self.getdata(fuel,data_available_nav,identifier_mapping,previous_data, i)
                        new_common_data=self.common_data
                        data['rep_dt']=self.dates
                        print(data['rep_dt'])
                        daily_data_collection.update_one(daily_data_collection.find({"ship_imo":self.imo,"historical":False,"data.rep_dt":self.dates,"timestamp":input_timestamp})[0],{"$set":{"historical":False,"nav_data_available":True,"data_available_nav":data_available_nav,"data":data,"common_data":new_common_data}})
                        return input_rep_dt, input_timestamp
                    # elif self.override==False:
                    #     return None, None
                
            else:
                data=self.getdata(fuel,data_available_nav,identifier_mapping,{}, i)
                data['rep_dt']=rep_dt
                print(data['rep_dt'])
                daily_nav={
                    "ship_imo":self.imo,
                    "ship_name":ship_name,
                    "timestamp":input_timestamp,
                    "historical":False,
                    "Noon":self.Noon,
                    "Logs":self.logs,
                    "nav_data_available":True,
                    "engine_data_available":False,
                    "nav_data_details":{"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                    "engine_data_details":None,
                    "data_available_nav":data_available_nav,
                    "data_available_engine":[],
                    "data":data,
                    "common_data":self.common_data
                }
                try:
                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],daily_nav['data']['timestamp'])
                except:
                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],None)
                daily_data_collection.insert_one(daily_nav).inserted_id
                return input_rep_dt, input_timestamp

    def eng_insert(self,eng,identifier_mapping,daily_data_collection,data_available_engine,ship_name,i):
        input_rep_dt=eng[identifier_mapping["rep_dt"]][i].strip()
        try:
            input_timestamp=int(eng[identifier_mapping['timestamp']][i])
        except:
            input_timestamp=None
        rep_dt = self.get_rep_dt(input_rep_dt)
        ship_imos=daily_data_collection.distinct("ship_imo")
        if self.imo in ship_imos:
            # try:
            rep_dt_list=daily_data_collection.distinct("data.rep_dt")
            daily_data=None
            try:
                for dates in rep_dt_list:
                    if dates.date()==rep_dt.date():
                        try:
                            daily_data=daily_data_collection.find({"ship_imo":self.imo,"historical":False,"data.rep_dt":dates,"timestamp":input_timestamp})[0]
                            self.dates=dates
                        except:
                            continue
            except:
                daily_data=None 
            if daily_data:
                if 'data_available_nav' in daily_data and len(daily_data['data_available_nav'])>0 and daily_data['nav_data_available']==True:
                    historical=False
                    eng_data_available=True
                    previous_data=daily_data['data']
                    previous_common_data=daily_data['common_data']
                    print("PREVIOUS", previous_common_data)
                    data=self.getdata(eng,data_available_engine,identifier_mapping,previous_data, i)
                    previous_common_data.update(self.common_data)
                    new_common_data=previous_common_data
                    print("NEW", new_common_data)
                    data['rep_dt']=self.dates
                    print(data['rep_dt'])
                    daily_data_collection.update_one(daily_data_collection.find({"ship_imo":self.imo,"historical":False,"data.rep_dt":self.dates,"timestamp":input_timestamp})[0],{"$set":{"historical":False,"engine_data_available":True,"data_available_engine":data_available_engine,"data":data,"common_data":new_common_data}})
                    return input_rep_dt, input_timestamp
                elif 'data_available_engine' in daily_data and len(daily_data['data_available_engine'])>0 and daily_data['engine_data_available']==True:
                    print("hereeeee")
                    if self.override==True:
                        historical=False
                        eng_data_available=True
                        previous_data=daily_data['data']
                        data=self.getdata(eng,data_available_engine,identifier_mapping,previous_data, i)
                        new_common_data=self.common_data
                        data['rep_dt']=self.dates
                        print(data['rep_dt'])
                        daily_data_collection.update_one(daily_data_collection.find({"ship_imo":self.imo,"historical":False,"data.rep_dt":self.dates,"timestamp":input_timestamp})[0],{"$set":{"historical":False,"engine_data_available":True,"data_available_engine":data_available_engine,"data":data,"common_data":new_common_data}})
                        return input_rep_dt, input_timestamp
                    # else:
                    #     return None, None
                
            else:
                data=self.getdata(eng,data_available_engine,identifier_mapping,{}, i)
                data['rep_dt']=rep_dt
                print(data['rep_dt'])
                daily_nav={
                    "ship_imo":self.imo,
                    "ship_name":ship_name,
                    "timestamp":input_timestamp,
                    "historical":False,
                    "Noon":self.Noon,
                    "Logs":self.logs,
                    "nav_data_available":False,
                    "engine_data_available":True,
                    "nav_data_details":None,
                    "engine_data_details":{"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"},},
                    "data_available_nav":[],
                    "data_available_engine":data_available_engine,
                    "data":data,
                    "common_data":self.common_data
                }
                try:
                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],daily_nav['data']['timestamp'])
                except:
                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],None)
                daily_data_collection.insert_one(daily_nav).inserted_id
                return input_rep_dt, input_timestamp


    def dailydata_insert(self):
        database=self.db.get_database("aranti")
        ship_configs_collection=database.get_collection("ship")
        self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
        daily_data_collection =database.get_collection("daily_data")
        if self.fuel!=None:
            if self.logs==False:
                try:
                    del self.fuel['undefined']
                except:
                    pass
                fuel = pd.DataFrame(self.fuel)
                csv_buffer = StringIO()
                fuel.to_csv(csv_buffer)
                fuel_object_key = self.ship_configs['ship_name'] + ' - ' + str(self.imo) + '/' + 'Noon' + '/' + "Fuel".capitalize() + '/' + str(self.imo)+"_noonfuel_"+datetime.today().date().strftime("%Y-%m-%d")+'.csv'
                s3 = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
                s3.put_object(Body=csv_buffer.getvalue(), Bucket="input-templates", Key=fuel_object_key)
            elif self.logs==True:
                fuel=self.create_data(self.fuel)
                csv_buffer = StringIO()
                fuel.to_csv(csv_buffer)
                fuel_object_key = self.ship_configs['ship_name'] + ' - ' + str(self.imo) + '/' + 'Logs' + '/' + "Fuel".capitalize() + '/' + str(self.imo)+"_logsfuel_"+datetime.today().date().strftime("%Y-%m-%d")+'.csv'
                s3 = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
                s3.put_object(Body=csv_buffer.getvalue(), Bucket="input-templates", Key=fuel_object_key)            

            ship_name=self.ship_configs['ship_name']
            data_available_nav=self.ship_configs['data_available_nav']
            data_available_engine=self.ship_configs['data_available_engine']
            identifier_mapping=self.ship_configs['identifier_mapping']
            self.common_col=self.ship_configs['common_col']
            print("COMMON COL", self.common_col)
            for com_col in self.common_col:
                if com_col!=identifier_mapping['rep_dt'] and com_col!=identifier_mapping['timestamp'] and com_col!=identifier_mapping["ship_imo"] and com_col in fuel.columns:
                    fuel[str(com_col)+"_fuel_file"]=fuel[com_col]
                    print("COMMON CL", com_col)
                    print("FUEL COMMON COL", fuel[com_col])
            
            for i in range(len(fuel)):
                fuel_insert_date, fuel_insert_timestamp=self.fuel_insert(fuel,identifier_mapping,daily_data_collection,data_available_nav,ship_name,i)
                # if fuel_insert_date != None:
                #     runner = MainDbRunner(fuel_insert_date, self.imo, fuel_insert_timestamp)
                #     message = runner.check_fuel_and_engine_availability_and_run()

        elif self.eng!=None:
            if self.logs==False:
                try:
                    del self.eng['undefined']
                except:
                    pass
                eng = pd.DataFrame(self.eng)
                csv_buffer = StringIO()
                eng.to_csv(csv_buffer)
                eng_object_key = self.ship_configs['ship_name'] + ' - ' + str(self.imo) + '/' + 'Noon' + '/' + "Engine".capitalize() + '/' + str(self.imo)+"_noonengine_"+datetime.today().date().strftime("%Y-%m-%d")+'.csv'
                s3 = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
                s3.put_object(Body=csv_buffer.getvalue(), Bucket="input-templates", Key=eng_object_key)
            elif self.logs==True:
                eng=self.create_data(self.eng)
                csv_buffer = StringIO()
                eng.to_csv(csv_buffer)
                eng_object_key = self.ship_configs['ship_name'] + ' - ' + str(self.imo) + '/' + 'Logs' + '/' + "Engine".capitalize() + '/' + str(self.imo)+"_logsengine_"+datetime.today().date().strftime("%Y-%m-%d")+'.csv'
                s3 = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
                s3.put_object(Body=csv_buffer.getvalue(), Bucket="input-templates", Key=eng_object_key)
            
            # database=self.db.get_database("aranti")
            # ship_configs_collection=database.get_collection("ship")
            # self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
            # daily_data_collection =database.get_collection("daily_data")
            ship_name=self.ship_configs['ship_name']
            data_available_nav=self.ship_configs['data_available_nav']
            data_available_engine=self.ship_configs['data_available_engine']
            identifier_mapping=self.ship_configs['identifier_mapping']
            self.common_col=self.ship_configs['common_col']
            for com_col in self.common_col:
                if com_col!=identifier_mapping['rep_dt'] and com_col!=identifier_mapping['timestamp'] and com_col!=identifier_mapping["ship_imo"] and com_col in eng.columns:
                    eng[str(com_col)+"_eng_file"]=eng[com_col]

            for i in range(len(eng)):
                eng_insert_date, engine_insert_timestamp=self.eng_insert(eng,identifier_mapping,daily_data_collection,data_available_engine,ship_name,i)
                # if eng_insert_date != None:
                #     runner = MainDbRunner(eng_insert_date, self.imo, engine_insert_timestamp)
                #     message = runner.check_fuel_and_engine_availability_and_run()
            


    def getdata(self,row,data_available_nav,identifier_mapping,dest, index_number):
        for w in data_available_nav:
            try:
                if w in row:
                    dest[w]=self.is_float(str(row[w].iloc[index_number]))
                elif identifier_mapping[w].strip() in row:
                    dest[w]=self.is_float(str(row[identifier_mapping[w]].iloc[index_number]))
                if dest[w] == "None" or dest[w] == "none":
                    dest[w] = None
            except KeyError:
                continue

        self.common_data={}    
        for i in row.columns:
            print("ROW COLUMNS", i)
            try:
                if i in self.common_col and i!=identifier_mapping['rep_dt'] and i!=identifier_mapping['timestamp']:
                    if self.fuel!=None:
                        print(row[i].iloc[index_number])
                        if row[i].iloc[index_number]== None or pd.isnull(row[i].iloc[index_number]) == True:
                            self.common_data[str(i)+"_fuel_file"] = None
                        else:
                            self.common_data[str(i)+"_fuel_file"]=self.is_float(str(row[i].iloc[index_number]))
                    elif self.eng!=None:
                        if row[i].iloc[index_number] == None or pd.isnull(row[i].iloc[index_number]) == True:
                            self.common_data[str(i)+"_eng_file"] = None
                        else:
                            self.common_data[str(i)+"_eng_file"]=self.is_float(str(row[i].iloc[index_number]))
            except:
                continue
        print(self.common_data)
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

obj=DailyDataExtractor(None,None,9205926,True,False)
obj.connect()
msg=obj.dailydata_insert()
print(msg)
