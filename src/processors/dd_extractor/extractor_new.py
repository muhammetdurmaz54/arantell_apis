from math import nan
from os import error
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




# class DailyInsert:
#     def __init__(self,fuelfile,engfile,imo,override):
#         self.fuel=fuelfile
#         self.eng=engfile
#         self.imo=imo
#         self.error = False
#         self.override=override


#     def do_steps(self):
#         self.connect()
#         inserted_id = self.dailydata_insert()
#         # if self.error:
#         #     return False, str(self.traceback_msg)
#         # else:
#         #     return True, str(inserted_id)

#     def connect(self):
#         self.db = connect_db()

#     def dailydata_insert(self):
#         if self.fuel!=None and self.eng!=None:
#             fuel = pd.read_excel(self.fuel).fillna("  ")
#             eng = pd.read_excel(self.eng).fillna("  ")
            
#             database=self.db.get_database("aranti")
#             ship_configs_collection=database.get_collection("ship")
#             self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
#             daily_data_collection =database.get_collection("daily_data")
#             try :
#                 ship_imo=self.ship_configs['ship_imo']
#                 if ship_imo==self.imo:
#                     ship_name=self.ship_configs['ship_name']
#                     data_available_nav=self.ship_configs['data_available_nav']
#                     data_available_engine= self.ship_configs['data_available_engine']
#                     identifier_mapping=self.ship_configs['identifier_mapping']
#                     data={}
#                     common_col = np.intersect1d(fuel.columns, eng.columns)
                   
#                     temp_eng1=eng
#                     temp_fuel1=fuel
#                     for col in common_col:
#                         if col!=identifier_mapping["ship_imo"] and col!=identifier_mapping["rep_dt"]:
#                             temp_fuel1=temp_fuel1.drop(columns=col)   
#                             temp_eng1=temp_eng1.drop(columns=col)
                            
                            
                   
#                     # temp_fuel1=fuel.drop([identifier_mapping["rpm"]],axis=1)
                
                
#                     # temp_fuel1=temp_fuel1.drop([identifier_mapping["rep_per"]],axis=1)
                
                
#                     # temp_fuel1=temp_fuel1.drop([identifier_mapping["utc_gmt"]],axis=1)
                
                      
                    

#                     # temp_fuel1=temp_fuel1.drop([identifier_mapping["vsl_load_bal"]],axis=1)
                    
#                     # temp_eng1=eng.drop([identifier_mapping["rpm"]],axis=1)
#                     # temp_eng1=temp_eng1.drop([identifier_mapping["rep_per"]],axis=1)
#                     # temp_eng1=temp_eng1.drop([identifier_mapping["utc_gmt"]],axis=1)
#                     # temp_eng1=temp_eng1.drop([identifier_mapping["vsl_load_bal"]],axis=1)
                    
                   

#                     temp_fuel=fuel[fuel[identifier_mapping["ship_imo"]]==ship_imo]
#                     temp_eng=eng[eng[identifier_mapping["ship_imo"]]==ship_imo]
#                     mer=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"],identifier_mapping["ship_imo"]])
#                     merg1=pd.merge(temp_fuel,temp_eng1,on=[identifier_mapping["rep_dt"],identifier_mapping["ship_imo"]],how="left",indicator="indicator")
#                     merg2=pd.merge(temp_fuel1,temp_eng,on=[identifier_mapping["rep_dt"],identifier_mapping["ship_imo"]],how="right",indicator="indicator")
#                     merg11=merg1[merg1["indicator"]!="both"]    #only fuel data is indicated
#                     merg22=merg2[merg2["indicator"]!="both"]    #only eng data is indicated
#                     temp_alldata=data_available_nav[:]
#                     temp_alldata.extend(data_available_engine)
                    
#                     # daily_data_db=self.db.get_database("aranti")
#                     # daily_data_collection=daily_data_db.get_collection("daily_data")
#                     ship_imos=daily_data_collection.distinct("ship_imo")
#                     rep_dt_col=identifier_mapping['rep_dt']
#                     # print("hi")
#                     # print(len(mer))
#                     # mer.to_csv("atm_both_data.csv")
#                     # exit()
#                     if len(mer)>0:
#                         for j,row in mer.iterrows():                      #for both cases where rept date and imo are common in both files
#                             daily_nav={
#                                 "ship_imo":ship_imo,
#                                 "ship_name":ship_name,
#                                 "historical":True,
#                                 "nav_data_available":True,
#                                 "engine_data_available":True,
#                                 "nav_data_details":{"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
#                                 "engine_data_details":{"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"},},
#                                 "data_available_nav":data_available_nav,
#                                 "data_available_engine":data_available_engine,
#                                 "data":self.getdata(row,temp_alldata,identifier_mapping)
#                             }
                        
                          
#                             if self.imo in ship_imos:
#                                 daily_data=daily_data_collection.find({"ship_imo": self.imo})
#                                 dates_unique=daily_data.distinct("data.rep_dt")
#                                 if mer[rep_dt_col][j] in dates_unique:
#                                     if self.override==True:
#                                         print("override true")
#                                         daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":mer[rep_dt_col][j]})
#                                         print("deleted")
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                         print("inserted")
#                                     else:
#                                         print("record already exist")
                                        
#                                 else:
#                                     print("dates not matchingg")
#                                     try:
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                     except:
#                                         print(self.error)
#                                         continue
#                                     print("inserted")
#                             else:
#                                 print("new")
#                                 daily_data_collection.insert_one(daily_nav).inserted_id
                
#                     if len(merg11)>0:
#                         print("koooo")
#                         for j,row in merg11.iterrows():                      #for case where rept date and imo are not common in both files and fuel data gets inserted
                            
#                             daily_nav={
#                                 "ship_imo":ship_imo,
#                                 "ship_name":ship_name,
#                                 "historical":True,
#                                 "nav_data_available":True,
#                                 "engine_data_available":False,
#                                 "nav_data_details":{"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
#                                 "engine_data_details":{},
#                                 "data_available_nav":data_available_nav,
#                                 "data_available_engine":[],
#                                 "data":self.getdata(row,data_available_nav,identifier_mapping)
                                
#                             }
#                             if self.imo in ship_imos:
#                                 daily_data=daily_data_collection.find({"ship_imo": self.imo})
#                                 dates_unique=daily_data.distinct("data.rep_dt")
#                                 if merg11[rep_dt_col][j] in dates_unique:
#                                     if self.override==True:
#                                         print("override true")
#                                         daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":merg11[rep_dt_col][j]})
#                                         print("deleted")
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                         print("inserted")
#                                     else:
#                                         print("record already exist")
                                        
#                                 else:
#                                     print("dates not matched merg11")
#                                     try:
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                     except:
#                                         continue
#                             else:
#                                 print("new")
#                                 daily_data_collection.insert_one(daily_nav).inserted_id
                   

#                     if len(merg22)>0:
#                         print("poooooo")
#                         for j,row in merg22.iterrows():                      #for case where rept date and imo are not common in both files and engine data gets inserted
                            
#                             daily_nav={
#                                 "ship_imo":ship_imo,
#                                 "ship_name":ship_name,
#                                 "historical":True,
#                                 "nav_data_available":False,
#                                 "engine_data_available":True,
#                                 "nav_data_details":{},
#                                 "engine_data_details":{"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
#                                 "data_available_nav":[],
#                                 "data_available_engine":data_available_engine,
#                                 "data":self.getdata(row,data_available_engine,identifier_mapping)
                                
#                             }
#                             if self.imo in ship_imos:
#                                 daily_data=daily_data_collection.find({"ship_imo": self.imo})
#                                 dates_unique=daily_data.distinct("data.rep_dt")
#                                 if merg22[rep_dt_col][j] in dates_unique:
#                                     if self.override==True:
#                                         print("override true")
#                                         daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":merg22[rep_dt_col][j]})
#                                         print("deleted")
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                         print("inserted")
#                                     else:
#                                         print("record already exist")
                                        
#                                 else:
#                                     print("dates not matched merg22")
#                                     try:
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                     except:
#                                         continue
#                             else:
#                                 print("new")
#                                 daily_data_collection.insert_one(daily_nav).inserted_id
                
                      

#             except:
#                 return "no data in ship config"



#         elif self.fuel!=None and self.eng==None:
#             fuel = pd.read_excel(self.fuel).fillna("")
        
#             database=self.db.get_database("aranti")
#             ship_configs_collection=database.get_collection("ship")
#             self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
#             daily_data_collection =database.get_collection("daily_data")

#             try :
#                 ship_imo=self.ship_configs['ship_imo']
#                 if ship_imo==self.imo:
#                     ship_name=self.ship_configs['ship_name']
#                     data_available_nav=self.ship_configs['data_available_nav']
#                     data_available_engine= self.ship_configs['data_available_engine']
#                     identifier_mapping=self.ship_configs['identifier_mapping']
#                     merg11=fuel[fuel[identifier_mapping["ship_imo"]]==ship_imo]
#                     merg11=merg11.reset_index(drop=True)
            
#                     ship_imos=daily_data_collection.distinct("ship_imo")
#                     rep_dt_col=identifier_mapping['rep_dt']
#                     if len(merg11)>0:
#                         for j,row in merg11.iterrows():                      #for case where rept date and imo are not common in both files and fuel data gets inserted
                            
#                             daily_nav={
#                                 "ship_imo":ship_imo,
#                                 "ship_name":ship_name,
#                                 "historical":True,
#                                 "nav_data_available":True,
#                                 "engine_data_available":False,
#                                 "nav_data_details":{"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
#                                 "engine_data_details":{},
#                                 "data_available_nav":data_available_nav,
#                                 "data_available_engine":[],
#                                 "data":self.getdata(row,data_available_nav,identifier_mapping)
                                
#                             }
#                             if self.imo in ship_imos:
#                                 daily_data=daily_data_collection.find({"ship_imo": self.imo})
#                                 dates_unique=daily_data.distinct("data.rep_dt")
#                                 if merg11[rep_dt_col][j] in dates_unique:
#                                     if self.override==True:
#                                         print("override true")
#                                         daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":merg11[rep_dt_col][j]})
#                                         print("deleted")
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                         print("inserted")
#                                     else:
#                                         print("record already exist")
                                        
#                                 else:
#                                     print("dates not matched")
#                                     try:
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                     except:
#                                         continue
#                             else:
#                                 print("new")
#                                 daily_data_collection.insert_one(daily_nav).inserted_id
#             except:
#                 return "no data in ship config"
        

#         elif self.fuel==None and self.eng!=None:
#             eng = pd.read_excel(self.eng).fillna("")
        
#             database=self.db.get_database("aranti")
#             ship_configs_collection=database.get_collection("ship")
#             self.ship_configs = ship_configs_collection.find({"ship_imo": self.imo})[0]
#             daily_data_collection =database.get_collection("daily_data")

#             try :
#                 ship_imo=self.ship_configs['ship_imo']
#                 if ship_imo==self.imo:
#                     ship_name=self.ship_configs['ship_name']
#                     data_available_nav=self.ship_configs['data_available_nav']
#                     data_available_engine= self.ship_configs['data_available_engine']
#                     identifier_mapping=self.ship_configs['identifier_mapping']
#                     merg22=eng[eng[identifier_mapping["ship_imo"]]==ship_imo]
#                     merg22=merg22.reset_index(drop=True)
            
#                     ship_imos=daily_data_collection.distinct("ship_imo")
#                     rep_dt_col=identifier_mapping['rep_dt']
#                     if len(merg22)>0:
#                         for j,row in merg22.iterrows():                      #for case where rept date and imo are not common in both files and engine data gets inserted
                            
#                             daily_nav={
#                                 "ship_imo":ship_imo,
#                                 "ship_name":ship_name,
#                                 "historical":True,
#                                 "nav_data_available":False,
#                                 "engine_data_available":True,
#                                 "nav_data_details":{},
#                                 "engine_data_details":{"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
#                                 "data_available_nav":[],
#                                 "data_available_engine":data_available_engine,
#                                 "data":self.getdata(row,data_available_engine,identifier_mapping)
                                
#                             }
#                             if self.imo in ship_imos:
#                                 daily_data=daily_data_collection.find({"ship_imo": self.imo})
#                                 dates_unique=daily_data.distinct("data.rep_dt")
#                                 if merg22[rep_dt_col][j] in dates_unique:
#                                     if self.override==True:
#                                         print("override true")
#                                         daily_data_collection.delete_one({"ship_imo": self.imo,"data.rep_dt":merg22[rep_dt_col][j]})
#                                         print("deleted")
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                         print("inserted")
#                                     else:
#                                         print("record already exist")
                                        
#                                 else:
#                                     print("dates not matched")
#                                     try:
#                                         daily_data_collection.insert_one(daily_nav).inserted_id
#                                     except:
#                                         continue
#                             else:
#                                 print("new")
#                                 daily_data_collection.insert_one(daily_nav).inserted_id

#             except:
#                 return "no data in ship config"



#     def getdata(self,row,data_available_nav,identifier_mapping):
        
#         dest={}
#         for w in data_available_nav:
#             try:
#                 if w in row:
#                     dest[w]=row[w]
#                 elif identifier_mapping[w].strip() in row:
#                     dest[w]=row[identifier_mapping[w]]
              
#             except KeyError:
#                 continue    
     
#         return dest


# obj=DailyInsert('F:\Afzal_cs\Internship\Arvind data files\RTM FUEL.xlsx','F:\Afzal_cs\Internship\Arvind data files\RTM ENGINE.xlsx',9591301,True)
# # obj=DailyInsert('F:\Afzal_cs\Internship\Arvind data files\RTM FUEL.xlsx',None,9591301,True)
# # obj=DailyInsert(None,'F:\Afzal_cs\Internship\Arvind data files\RTM ENGINE.xlsx',9591301,True)
# obj.do_steps()


# below code is for rtm cook which has only one data file combined
class DailyInsert:
    def __init__(self,fuelfile,imo):
        self.fuel=fuelfile
        self.imo=imo
        self.error=False

    def do_steps(self):
        self.connect()
        inserted_id = self.dailydata_insert()
        if self.error:
            return False, str(self.traceback_msg)
        else:
            return True, str(inserted_id)

    def connect(self):
        self.db = connect_db()


    def dailydata_insert(self):
        # fuel = pd.read_excel(self.fuel,sheet_name='OriginalData').fillna("")
        fuel = pd.read_excel(self.fuel).fillna("")

        ship_imo_o=Ship.objects()
        for i in ship_imo_o:

            ship_imo=i.ship_imo
            if ship_imo==self.imo:
                ship_name=i.ship_name
                data_available_nav=i.data_available_nav
                data_available_engine= i.data_available_engine
                identifier_mapping=i.identifier_mapping
                temp_alldata=data_available_nav[:]
                temp_alldata.extend(data_available_engine)
                
        
                
                
                
                for j,row in fuel.iterrows():                      #for both cases where rept date and imo are common in both files
                    
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
            # for j,row in fuel.iterrows():                      #for both cases where rept date and imo are common in both files
                
            #     daily_nav={
            #         "ship_imo":ship_imo,
            #         "ship_name":ship_name,
            #         "historical":True,
            #         "nav_data_available":True,
            #         "engine_data_available":True,
            #         "nav_data_details":{"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
            #         "engine_data_details":{"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"},},
            #         "data_available_nav":data_available_nav,
            #         "data_available_engine":data_available_engine,
            #         "data":self.getdata(row,temp_alldata,identifier_mapping)
                    
            #     }
            #     daily_data_collection.insert_one(daily_nav).inserted_id
                    
            
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


obj=DailyInsert('F:\Afzal_cs\Internship\Atm_both_data.xlsx',9591301)
obj.connect()
obj.dailydata_insert()





































# for j,row in mer.iterrows():                      #for both cases where rept date and imo are common in both files
                        
                    #     daily_nav=DailyData(
                    #         ship_imo=ship_imo,
                    #         ship_name=ship_name,
                    #         historical=True,
                    #         nav_data_available=True,
                    #         engine_data_available=True,
                    #         nav_data_details={"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                    #         engine_data_details={"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"},},
                    #         data_available_nav=data_available_nav,
                    #         data_available_engine=data_available_engine,
                    #         data=self.getdata(row,temp_alldata,identifier_mapping)
                            
                    #     )
                    #     daily_nav.save()
                        
                    # for j,row in merg11.iterrows():                      #for case where rept date and imo are not common in both files and fuel data gets inserted
                        
                    #     daily_nav=DailyData(
                    #         ship_imo=ship_imo,
                    #         ship_name=ship_name,
                    #         historical=True,
                    #         nav_data_available=True,
                    #         engine_data_available=False,
                    #         nav_data_details={"file_name":"daily_data19June20.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                    #         engine_data_details={},
                    #         data_available_nav=data_available_nav,
                    #         data_available_engine=[],
                    #         data=self.getdata(row,data_available_nav,identifier_mapping)
                            
                    #     )
                    #     daily_nav.save()

                    # for j,row in merg22.iterrows():                      #for case where rept date and imo are not common in both files and engine data gets inserted
                        
                    #     daily_nav=DailyData(
                    #         ship_imo=ship_imo,
                    #         ship_name=ship_name,
                    #         historical=True,
                    #         nav_data_available=False,
                    #         engine_data_available=True,
                    #         nav_data_details={},
                    #         engine_data_details={"file_name":"daily_data19June20engine.xlsx","file_url":"aws.s3.xyz.com","uploader_details":{"userid":"xyz","company":"sdf"}},
                    #         data_available_nav=[],
                    #         data_available_engine=data_available_engine,
                    #         data=self.getdata(row,data_available_engine,identifier_mapping)
                            
                    #     )
                    #     daily_nav.save()