# not react application backend, historical daily data/rows processing and inserting raw values 


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
# from mongoengine import *
from datetime import datetime
import pandas as pd
import numpy as np
import ntpath
import boto3

log = CommonLogger(__name__, debug=True).setup_logger()
# connect("aranti")




class DailyInsert:
    def __init__(self,fuelfile,engfile,imo,logs,override):
        self.fuel=fuelfile
        self.eng=engfile
        self.imo=imo
        self.error = False
        self.logs=logs
        self.override=override


    def do_steps(self):
        self.connect()
        inserted_id = self.dailydata_insert()
        # s3_upload=self.upload_to_s3()
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


    def logs_data_generate(self,file_loc):
        tabs = pd.ExcelFile(file_loc).sheet_names 
        dataframe_dicts={}
        maindata=pd.DataFrame({"rep_dt":[],"timestamp":[]})

        j=0
        for i in tabs:
            tempdata = pd.read_excel(file_loc,sheet_name=i,skiprows = [1, 2, 3],engine='openpyxl').fillna("  ")
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
        return maindata


    def dailydata_insert(self):
        if self.logs==True:
            self.historical_noon=False
            self.type_of_data="Logs"
        if self.logs==False:
            self.historical_noon=True
            self.type_of_data="Noon"



        if self.fuel!=None and self.eng!=None:
            if self.logs==True:
                fuel=self.logs_data_generate(self.fuel)
            elif self.logs==False:
                fuel = pd.read_excel(self.fuel,skiprows = [1, 2, 3],engine='openpyxl').fillna("  ")
                fuel = fuel.drop_duplicates(subset=["rep_dt"])
                fuel=fuel.reset_index(drop=True)
            fuel=fuel[fuel.rep_dt != np.NaN]
            for column in fuel.columns:
                if (fuel[column] == "  ").all():
                    fuel=fuel.drop(columns=column)

            if self.logs==True:
                eng=self.logs_data_generate(self.eng)
            elif self.logs==False:
                eng = pd.read_excel(self.eng,skiprows = [1, 2, 3],engine='openpyxl').fillna("  ")
                eng = eng.drop_duplicates(subset=["rep_dt"])
                eng=eng.reset_index(drop=True)
            eng=eng[eng.rep_dt != np.NaN]
            for column in eng.columns:
                if (eng[column] == "  ").all():
                    eng=eng.drop(columns=column)
         
            
            database=self.db.get_database("aranti")
            ship_configs_collection=database.get_collection("ship")
            self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
            daily_data_collection =database.get_collection("daily_data")
            maindb = database.get_collection("Main_db")
            ship_stats=database.get_collection("Ship_stats")
            try:
                # maindb.delete_many({"ship_imo":self.imo})
                ship_stats.delete_one({"ship_imo": self.imo})
                daily_data_collection.delete_many({"ship_imo":self.imo})
                print("deleted maindb")
            except:
                print("no value for maindb yet")
            # exit()
            shipstats={"ship_imo":int(self.imo),"updated":0,"stage":"to be started"}
            ship_stats.insert_one(shipstats).inserted_id
            try:
                ship_imo=self.ship_configs['ship_imo']
                if ship_imo==self.imo:
                    ship_name=self.ship_configs['ship_name']
                    data_available_nav=self.ship_configs['data_available_nav']
                    data_available_engine= self.ship_configs['data_available_engine']
                    identifier_mapping=self.ship_configs['identifier_mapping']
                    # mer=pd.merge(fuel,eng,on=[identifier_mapping["rep_dt"],identifier_mapping["timestamp"]])
                    # data={}
                    common_col = np.intersect1d(fuel.columns, eng.columns)

                    ship_configs_collection.update_one(ship_configs_collection.find({"ship_imo": int(self.imo)})[0],{"$set":{"common_col":list(common_col)}})

                    # exit()
                    for com_col in common_col:
                        if com_col!=identifier_mapping['rep_dt'] and com_col!=identifier_mapping['timestamp'] and com_col!=identifier_mapping["ship_imo"]:
                            fuel[str(com_col)+"_fuel_file"]=fuel[com_col]
                            eng[str(com_col)+"_eng_file"]=eng[com_col]

                    

                    temp_eng1=eng
                    temp_fuel1=fuel
                    if self.logs==False:
                        for col in common_col:
                            if col!=identifier_mapping["ship_imo"] and col!=identifier_mapping["rep_dt"]:
                                temp_fuel1=temp_fuel1.drop(columns=col)
                                temp_eng1=temp_eng1.drop(columns=col)
                    elif self.logs==True:
                        for col in common_col:
                            if col!=identifier_mapping["timestamp"] and col!=identifier_mapping["rep_dt"]:
                                temp_fuel1=temp_fuel1.drop(columns=col)
                                temp_eng1=temp_eng1.drop(columns=col)
                    
                    # print(common_fuel_data)
                    # print(common_eng_data)
                    # exit()
                            
                    
                    # temp_fuel1=fuel.drop([identifier_mapping["rpm"]],axis=1)
                
                
                    # temp_fuel1=temp_fuel1.drop([identifier_mapping["rep_per"]],axis=1)
                
                
                    # temp_fuel1=temp_fuel1.drop([identifier_mapping["utc_gmt"]],axis=1)
                
                        
                    

                    # temp_fuel1=temp_fuel1.drop([identifier_mapping["vsl_load_bal"]],axis=1)
                    
                    # temp_eng1=eng.drop([identifier_mapping["rpm"]],axis=1)
                    # temp_eng1=temp_eng1.drop([identifier_mapping["rep_per"]],axis=1)
                    # temp_eng1=temp_eng1.drop([identifier_mapping["utc_gmt"]],axis=1)
                    # temp_eng1=temp_eng1.drop([identifier_mapping["vsl_load_bal"]],axis=1)
                    
                    
                    if identifier_mapping["ship_imo"] in fuel.columns and identifier_mapping["ship_imo"] in eng.columns:
                        temp_fuel=fuel[fuel[identifier_mapping["ship_imo"]]==ship_imo]
                        temp_eng=eng[eng[identifier_mapping["ship_imo"]]==ship_imo]
                    else:
                        temp_fuel=fuel
                        temp_eng=eng
                    if self.logs==False:
                        try:
                            mer=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"],identifier_mapping["ship_imo"]])
                            merg1=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"],identifier_mapping["ship_imo"]],how="left",indicator="indicator")
                            merg2=pd.merge(temp_fuel1,temp_eng,on=[identifier_mapping["rep_dt"],identifier_mapping["ship_imo"]],how="right",indicator="indicator")
                            merg11=merg1[merg1["indicator"]!="both"]    #only fuel data is indicated
                            merg22=merg2[merg2["indicator"]!="both"]    #only eng data is indicated
                        except:
                            mer=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"]])
                            # mer.to_csv("rtm_merged.csv")
                            # exit()
                            merg1=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"]],how="left",indicator="indicator")
                            merg2=pd.merge(temp_fuel1,temp_eng,on=[identifier_mapping["rep_dt"]],how="right",indicator="indicator")
                            merg11=merg1[merg1["indicator"]!="both"]    #only fuel data is indicated
                            merg22=merg2[merg2["indicator"]!="both"]    #only eng data is indicated

                    elif self.logs==True:
                        mer=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"],identifier_mapping["timestamp"]])
                        merg1=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"],identifier_mapping["timestamp"]],how="left",indicator="indicator")
                        merg2=pd.merge(temp_fuel1,temp_eng,on=[identifier_mapping["rep_dt"],identifier_mapping["timestamp"]],how="right",indicator="indicator")
                        merg11=merg1[merg1["indicator"]!="both"]    #only fuel data is indicated
                        merg22=merg2[merg2["indicator"]!="both"]
                    temp_alldata=data_available_nav[:]
                    temp_alldata.extend(data_available_engine)
                    
                    # daily_data_db=self.db.get_database("aranti")
                    # daily_data_collection=daily_data_db.get_collection("daily_data")
                    ship_imos=daily_data_collection.distinct("ship_imo")
                    rep_dt_col=identifier_mapping['rep_dt']

                    # print("hi")
                    # print(mer['rpm'])
                    # mer.to_csv("atm_both_data.csv")
                    # exit()
                    # print(mer['estLat'])
                    # print(len(merg11))
                    # print(len(merg22))
                    # exit()
                    if len(mer)>0:
                        for j,row in mer.iterrows():  
                            if type(row[identifier_mapping['rep_dt']])!=str:                    #for both cases where rept date and imo are common in both files
                                print(j,"noooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
                                daily_nav={
                                    "ship_imo":ship_imo,
                                    "ship_name":ship_name,
                                    "historical":True,
                                    "Noon":self.historical_noon,
                                    "Logs":self.logs,
                                    "nav_data_available":True,
                                    "engine_data_available":True,
                                    "nav_data_details":{"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                                    "engine_data_details":{"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"},},
                                    "data_available_nav":data_available_nav,
                                    "data_available_engine":data_available_engine,
                                    "data":self.getdata(row,temp_alldata,identifier_mapping),
                                    "common_data":self.common_data

                                }
                                try:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],daily_nav['data']['timestamp'])
                                except:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],None)


                                if self.imo in ship_imos:
                                    daily_data=daily_data_collection.find({"ship_imo": self.imo})
                                    dates_unique=daily_data.distinct("data.rep_dt")
                                    if mer[rep_dt_col][j] in dates_unique:
                                        if self.override==True:
                                            print("override true")
                                            # daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":mer[rep_dt_col][j]})
                                            # print("deleted")
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                            print("inserted")
                                        else:
                                            print("record already exist")
                                            
                                    else:
                                        print("dates not matchingg")
                                        try:
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                        except:
                                            print(self.error)
                                            continue
                                        print("inserted")
                                else:
                                    print("new")
                                    daily_data_collection.insert_one(daily_nav).inserted_id
                    # exit()
                    # merg11.to_csv("natcsv.csv")
                    if len(merg11)>0:
                        print("koooo")
                        for j,row in merg11.iterrows():                      #for case where rept date and imo are not common in both files and fuel data gets inserted
                            if type(row[identifier_mapping['rep_dt']])!=str:
                                daily_nav={
                                    "ship_imo":ship_imo,
                                    "ship_name":ship_name,
                                    "historical":True,
                                    "Noon":self.historical_noon,
                                    "Logs":self.logs,
                                    "nav_data_available":True,
                                    "engine_data_available":False,
                                    "nav_data_details":{"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                                    "engine_data_details":{},
                                    "data_available_nav":data_available_nav,
                                    "data_available_engine":[],
                                    "data":self.getdata(row,data_available_nav,identifier_mapping),
                                    "common_data":self.common_data
                                    
                                }
                                try:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],daily_nav['data']['timestamp'])
                                except:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],None)
                                if self.imo in ship_imos:
                                    daily_data=daily_data_collection.find({"ship_imo": self.imo})
                                    dates_unique=daily_data.distinct("data.rep_dt")
                                    if merg11[rep_dt_col][j] in dates_unique:
                                        if self.override==True:
                                            print("override true")
                                            # daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":merg11[rep_dt_col][j]})
                                            # print("deleted")
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                            print("inserted")
                                        else:
                                            print("record already exist")
                                            
                                    else:
                                        print("dates not matched merg11")
                                        try:
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                        except:
                                            continue
                                else:
                                    print("new")
                                    daily_data_collection.insert_one(daily_nav).inserted_id
                    

                    if len(merg22)>0:
                        print("poooooo")
                        for j,row in merg22.iterrows():                      #for case where rept date and imo are not common in both files and engine data gets inserted
                            if type(row[identifier_mapping['rep_dt']])!=str:
                                daily_nav={
                                    "ship_imo":ship_imo,
                                    "ship_name":ship_name,
                                    "historical":True,
                                    "Noon":self.historical_noon,
                                    "Logs":self.logs,
                                    "nav_data_available":False,
                                    "engine_data_available":True,
                                    "nav_data_details":{},
                                    "engine_data_details":{"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                                    "data_available_nav":[],
                                    "data_available_engine":data_available_engine,
                                    "data":self.getdata(row,data_available_engine,identifier_mapping),
                                    "common_data":self.common_data
                                    
                                }
                                try:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],daily_nav['data']['timestamp'])
                                except:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],None)
                                if self.imo in ship_imos:
                                    daily_data=daily_data_collection.find({"ship_imo": self.imo})
                                    dates_unique=daily_data.distinct("data.rep_dt")
                                    if merg22[rep_dt_col][j] in dates_unique:
                                        if self.override==True:
                                            print("override true")
                                            # daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":merg22[rep_dt_col][j]})
                                            # print("deleted")
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                            print("inserted")
                                        else:
                                            print("record already exist")
                                            
                                    else:
                                        print("dates not matched merg22")
                                        try:
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                        except:
                                            continue
                                else:
                                    print("new")
                                    daily_data_collection.insert_one(daily_nav).inserted_id
                        
                        

            except:
                return "no data in ship config"



        elif self.fuel!=None and self.eng==None:
            
            fuel = pd.read_excel(self.fuel,skiprows = [1, 2, 3],engine='openpyxl').fillna("")
        
            database=self.db.get_database("aranti")
            ship_configs_collection=database.get_collection("ship")
            self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
            daily_data_collection =database.get_collection("daily_data")

            try :
                
                ship_imo=self.ship_configs['ship_imo']
                if ship_imo==self.imo:
                    ship_name=self.ship_configs['ship_name']
                    data_available_nav=self.ship_configs['data_available_nav']
                    data_available_engine= self.ship_configs['data_available_engine']
                    identifier_mapping=self.ship_configs['identifier_mapping']
                    merg11=fuel[fuel[identifier_mapping["ship_imo"]]==ship_imo]
                    merg11=merg11.reset_index(drop=True)
            
                    ship_imos=daily_data_collection.distinct("ship_imo")
                    rep_dt_col=identifier_mapping['rep_dt']
                    if len(merg11)>0:
                        for j,row in merg11.iterrows():                      #for case where rept date and imo are not common in both files and fuel data gets inserted
                            if type(row[identifier_mapping['rep_dt']])!=str:
                                daily_nav={
                                    "ship_imo":ship_imo,
                                    "ship_name":ship_name,
                                    "historical":True,
                                    "Noon":self.historical_noon,
                                    "Logs":self.logs,
                                    "nav_data_available":True,
                                    "engine_data_available":False,
                                    "nav_data_details":{"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                                    "engine_data_details":{},
                                    "data_available_nav":data_available_nav,
                                    "data_available_engine":[],
                                    "data":self.getdata(row,data_available_nav,identifier_mapping),
                                    "common_data":self.common_data
                                    
                                }
                                try:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],daily_nav['data']['timestamp'])
                                except:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],None)
                                if self.imo in ship_imos:
                                    daily_data=daily_data_collection.find({"ship_imo": self.imo})
                                    dates_unique=daily_data.distinct("data.rep_dt")
                                    if merg11[rep_dt_col][j] in dates_unique:
                                        if self.override==True:
                                            print("override true")
                                            daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":merg11[rep_dt_col][j]})
                                            print("deleted")
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                            print("inserted")
                                        else:
                                            print("record already exist")
                                    else:
                                        print("dates not matched")
                                        try:
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                        except:
                                            continue
                                else:
                                    print("new")
                                    daily_data_collection.insert_one(daily_nav).inserted_id
            except:
                return "no data in ship config"
        

        elif self.fuel==None and self.eng!=None:
            eng = pd.read_excel(self.eng,skiprows = [1, 2, 3],engine='openpyxl').fillna("")
            database=self.db.get_database("aranti")
            ship_configs_collection=database.get_collection("ship")
            self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
            daily_data_collection =database.get_collection("daily_data")
            try :
                ship_imo=self.ship_configs['ship_imo']
                if ship_imo==self.imo:
                    ship_name=self.ship_configs['ship_name']
                    data_available_nav=self.ship_configs['data_available_nav']
                    data_available_engine= self.ship_configs['data_available_engine']
                    identifier_mapping=self.ship_configs['identifier_mapping']
                    merg22=eng[eng[identifier_mapping["ship_imo"]]==ship_imo]
                    merg22=merg22.reset_index(drop=True)
            
                    ship_imos=daily_data_collection.distinct("ship_imo")
                    rep_dt_col=identifier_mapping['rep_dt']
                    if len(merg22)>0:
                        for j,row in merg22.iterrows():                      #for case where rept date and imo are not common in both files and engine data gets inserted
                            if type(row[identifier_mapping['rep_dt']])!=str:
                                daily_nav={
                                    "ship_imo":ship_imo,
                                    "ship_name":ship_name,
                                    "historical":True,
                                    "Noon":self.historical_noon,
                                    "Logs":self.logs,
                                    "nav_data_available":False,
                                    "engine_data_available":True,
                                    "nav_data_details":{},
                                    "engine_data_details":{"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                                    "data_available_nav":[],
                                    "data_available_engine":data_available_engine,
                                    "data":self.getdata(row,data_available_engine,identifier_mapping),
                                    "common_data":self.common_data
                                    
                                }
                                try:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],daily_nav['data']['timestamp'])
                                except:
                                    daily_nav['final_rep_dt']=self.final_rep_dt(daily_nav['data']['rep_dt'],None)
                                if self.imo in ship_imos:
                                    daily_data=daily_data_collection.find({"ship_imo": self.imo})
                                    dates_unique=daily_data.distinct("data.rep_dt")
                                    if merg22[rep_dt_col][j] in dates_unique:
                                        if self.override==True:
                                            print("override true")
                                            # daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":merg22[rep_dt_col][j]})
                                            # print("deleted")
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                            print("inserted")
                                        else:
                                            print("record already exist")
                                            
                                    else:
                                        print("dates not matched")
                                        try:
                                            daily_data_collection.insert_one(daily_nav).inserted_id
                                        except:
                                            continue
                                else:
                                    print("new")
                                    daily_data_collection.insert_one(daily_nav).inserted_id

            except:
                return "no data in ship config"
        
        # daily_data_collection =database.get_collection("daily_data")
        # daily_data=daily_data_collection.find({"ship_imo": self.imo})
        # dates_unique=daily_data.distinct("data.rep_dt")
        # for i in dates_unique:
        #     if daily_data_collection.find({"ship_imo": self.imo,'data.rep_dt':i}).count()>1:
        #         daily_data_collection.delete_one({"ship_imo": self.imo,'data.rep_dt':i})
        #         print("deletedeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
            




    def getdata(self,row,data_available_nav,identifier_mapping):
        dest={}
        for w in data_available_nav:
            try:
                if w in row:
                    if row[w]=="  " or row[w]=="":
                        dest[w]=None
                    elif type(row[w])==type(datetime.now().time()):
                        dest[w]=str(row[w])
                    elif type(row[w])==type(pd.NaT):
                        self.common_data[w]=None
                    else:
                        dest[w]=row[w]
                elif identifier_mapping[w].strip() in row:
                    if row[identifier_mapping[w]]=="  " or row[identifier_mapping[w]]=="":
                        dest[w]=None
                    elif type(row[identifier_mapping[w]])==type(datetime.now().time()):
                        dest[w]=str(row[identifier_mapping[w]])
                    elif type(row[identifier_mapping[w]])==type(pd.NaT):
                        self.common_data[identifier_mapping[w]]=None
                    else:
                        dest[w]=row[identifier_mapping[w]]
                
                
            except KeyError:
                continue

        self.common_data={}    
        for i in row.keys():
            try:
                if pd.isnull(i)==False:
                    if i.endswith("_fuel_file") or i.endswith("_eng_file"):
                        if row[i]=="  " or row[i]=="":
                            self.common_data[i]=None
                        elif type(row[i])==type(datetime.now().time()):
                            self.common_data[i]=str(row[i])
                        elif type(row[i])==type(pd.NaT):
                            self.common_data[i]=None
                        else:
                            self.common_data[i]=row[i]
            except AttributeError:
                continue
        for i in dest:
            if identifier_mapping[i]+"_fuel_file" in self.common_data or identifier_mapping[i]+"_eng_file" in self.common_data:
                if pd.isnull(dest[i])==True:
                    if pd.isnull(self.common_data[identifier_mapping[i]+"_fuel_file"])==False:
                        dest[i]=self.common_data[identifier_mapping[i]+"_fuel_file"]
                    elif pd.isnull(self.common_data[identifier_mapping[i]+"_eng_file"])==False:
                        dest[i]=self.common_data[identifier_mapping[i]+"_eng_file"]
        # for i in self.common_data:
        #     print(i," ",type(self.common_data[i]),"   ",self.common_data[i])
        return dest
    

    def upload_to_s3(self):
        self.fuel_object_key = self.ship_configs['ship_name'] + ' - ' + str(self.imo) + '/' + self.type_of_data.capitalize() + '/' + "Fuel".capitalize() + '/' + ntpath.basename(self.fuel)
        self.eng_object_key = self.ship_configs['ship_name'] + ' - ' + str(self.imo) + '/' + self.type_of_data.capitalize() + '/' + "Engine".capitalize() + '/' + ntpath.basename(self.eng)
        s3 = boto3.client('s3', aws_access_key_id=os.getenv("ak_aws_id"), aws_secret_access_key=os.getenv("ak_aws_key"))
        s3.upload_file(self.fuel, Bucket="input-templates", Key=self.fuel_object_key)
        s3.upload_file(self.eng, Bucket="input-templates", Key=self.eng_object_key)


# obj=DailyInsert('F:\Afzal_cs\Internship\Arvind data files\9205926noonfuel.xlsx','F:\Afzal_cs\Internship\Arvind data files\9205926noonengine.xlsx',9205926,False,True)
# # obj=DailyInsert('F:\Afzal_cs\Internship\Arvind data files\9250505noonfuel.xlsx',None,9250505,False,True)
# # obj=DailyInsert(None,'F:\Afzal_cs\Internship\Arvind data files\RTM ENGINE.xlsx',9591301,False,True)
# obj.do_steps()