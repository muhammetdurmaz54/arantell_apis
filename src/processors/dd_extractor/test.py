
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

connect("aranti")
types="fuel"


"""class Extractor:

    def __init__(self,
                 ship_imo,
                 date,
                 type,
                 file,
                 override):
        self.ship_imo = ship_imo
        self.df = file
        self.type = str(type)
        self.date = datetime(date)
        self.override = override
        pass"""
def getdata(row,data_available_nav,identifier_mapping,data):
        for w in data_available_nav:
            if w in row:
                data[w]=row[w]
            elif identifier_mapping[w] in row:
                data[w]=row[identifier_mapping[w]]
        return data


f=pd.read_excel('F:\Afzal_cs\Internship\RTM FUEL _1ROW.xlsx').fillna("")
i=input("dum")
ship=Ship.objects(ship_imo=i)
for config in ship:
    
    imo=config.ship_imo
    ship_name = config.ship_name
    identifier_mapping = config.identifier_mapping
    nav=config.data_available_nav
    eng=config.data_available_engine
    ddd=DailyData.objects(ship_imo=i,date='2021-04-19T10:51:31.360+00:00')
    if(len(ddd)>0):
        
        if types=="fuel":
            for j in ddd:
                
                data=j.data

                        
        for row in f.iterrows():

            daily_nav=DailyData.objects.update(
                
            
                historical=True,
                nav_data_available=True,
                
                nav_data_details={"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                
                data_available_nav=nav,
                
                data=getdata(row,nav,identifier_mapping,data)

            )
            daily_nav.save()


            


    
        
        
            




  





