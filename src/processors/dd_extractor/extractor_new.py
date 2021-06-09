from math import nan
import sys 
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

log = CommonLogger(__name__, debug=True).setup_logger()
connect("aranti")




class DailyInsert:
    def __init__(self,fuelfile,engfile):
        self.fuel=fuelfile
        self.eng=engfile

    def dailydata_insert(self):
        fuel = pd.read_excel(self.fuel).fillna("")
        eng = pd.read_excel(self.eng).fillna("")
    
        ship_imo_o=Ship.objects()
        for i in ship_imo_o:
            ship_imo=i.ship_imo
            ship_name=i.ship_name
            data_available_nav=i.data_available_nav
            data_available_engine= i.data_available_engine
            identifier_mapping=i.identifier_mapping
            data={}
            temp_fuel1=fuel.drop([identifier_mapping["rpm"]],axis=1)
            temp_fuel1=temp_fuel1.drop([identifier_mapping["rep_per"]],axis=1)
            temp_fuel1=temp_fuel1.drop([identifier_mapping["utc_gmt"]],axis=1)
            temp_eng1=eng.drop([identifier_mapping["rpm"]],axis=1)
            temp_eng1=temp_eng1.drop([identifier_mapping["rep_per"]],axis=1)
            temp_eng1=temp_eng1.drop([identifier_mapping["utc_gmt"]],axis=1)


            temp_fuel=fuel[fuel[identifier_mapping["ship_imo"]]==ship_imo]
            temp_eng=eng[eng[identifier_mapping["ship_imo"]]==ship_imo]
            mer=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"],identifier_mapping["ship_imo"]])
            merg1=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"],identifier_mapping["ship_imo"]],how="left",indicator="indicator")
            merg2=pd.merge(temp_fuel1,temp_eng,on=[identifier_mapping["rep_dt"],identifier_mapping["ship_imo"]],how="right",indicator="indicator")
            merg11=merg1[merg1["indicator"]!="both"]    #only fuel data is indicated
            merg22=merg2[merg2["indicator"]!="both"]    #only eng data is indicated
            temp_alldata=data_available_nav[:]
            temp_alldata.extend(data_available_engine)
            
      
            
            
            
            for j,row in mer.iterrows():                      #for both cases where rept date and imo are common in both files
                
                daily_nav=DailyData(
                    ship_imo=ship_imo,
                    ship_name=ship_name,
                    historical=True,
                    nav_data_available=True,
                    engine_data_available=True,
                    nav_data_details={"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                    engine_data_details={"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"},},
                    data_available_nav=data_available_nav,
                    data_available_engine=data_available_engine,
                    data=self.getdata(row,temp_alldata,identifier_mapping)
                    
                )
                daily_nav.save()
                
            for j,row in merg11.iterrows():                      #for case where rept date and imo are not common in both files and fuel data gets inserted
                
                daily_nav=DailyData(
                    ship_imo=ship_imo,
                    ship_name=ship_name,
                    historical=True,
                    nav_data_available=True,
                    engine_data_available=False,
                    nav_data_details={"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                    engine_data_details={},
                    data_available_nav=data_available_nav,
                    data_available_engine=[],
                    data=self.getdata(row,data_available_nav,identifier_mapping)
                    
                )
                daily_nav.save()

            for j,row in merg22.iterrows():                      #for case where rept date and imo are not common in both files and engine data gets inserted
                
                daily_nav=DailyData(
                    ship_imo=ship_imo,
                    ship_name=ship_name,
                    historical=True,
                    nav_data_available=False,
                    engine_data_available=True,
                    nav_data_details={},
                    engine_data_details={"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                    data_available_nav=[],
                    data_available_engine=data_available_engine,
                    data=self.getdata(row,data_available_engine,identifier_mapping)
                    
                )
                daily_nav.save()

    def getdata(self,row,data_available_nav,identifier_mapping):
        
        dest={}
        for w in data_available_nav:
            try:
                if w in row:
                    dest[w]=row[w]
                elif identifier_mapping[w].strip() in row:
                    dest[w]=row[identifier_mapping[w]]
            except KeyError:
                continue    
            
        return dest


obj=DailyInsert('F:\Afzal_cs\Internship\Arvind data files\RTM FUEL.xlsx','F:\Afzal_cs\Internship\Arvind data files\RTM ENGINE.xlsx')
obj.dailydata_insert()