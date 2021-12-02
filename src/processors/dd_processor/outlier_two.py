from __future__ import division
from re import escape
from typing import List
from numpy.core.arrayprint import format_float_positional

from numpy.core.fromnumeric import reshape, var
from numpy.core.function_base import linspace
from numpy.lib.function_base import diff
from numpy.lib.shape_base import dstack
from sklearn import linear_model
import scipy.stats as st
from datetime import date
import sys
from numpy.core.defchararray import add, index, upper
import pandas
from pandas.core import base
from pandas.core.frame import DataFrame
from pandas.core.indexes.datetimes import date_range
from bson import json_util
from pandas.core.dtypes.missing import isnull 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
#from mongoengine import *
from src.db.schema.ship import Ship 
#from src.processors.dd_processor.regress import regress
import numpy as np
import pandas as pd
import math
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import datetime
from datetime import date, timedelta
#from pysimplelog import Logger
import string
from dateutil.relativedelta import relativedelta
from src.processors.config_extractor.outlier import CheckOutlier

client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
database = db


np.seterr(divide='ignore', invalid='ignore')



class OutlierTwo():
    def __init__(self,configs,md,imo):
        self.ship_configs = configs
        self.main_data= md
        self.ship_imo=imo
       

    def time_dataframe_generator(self,identifier,main_data_dict,no_months,last_year_months,list_date):
        list=[]
        list.append(identifier)
        list.append("rep_dt")
        maindb = database.get_collection("Main_db")
        temp_list=[]
        for i in list:
            self.main =maindb.find({"processed_daily_data.rep_dt.processed": {"$lte":list_date[-1], "$gte": list_date[0]},"ship_imo":self.ship_imo},{"processed_daily_data."+i+".processed":1,"_id":0})
            mainobject=json_util.dumps(self.main)
            load=json_util.loads(mainobject)
            temp_list.append(load)
        # print(temp_list)
        temp_dict={}
        for i in range(len(temp_list)):
            temp_list_2=[]
            for j in range(0,len(temp_list[i])):    
                try:
                    temp_list_2.append(temp_list[i][j]['processed_daily_data'][list[i]]['processed'])
                except KeyError:
                    temp_list_2.append(None)
            
            temp_dict[list[i]]=temp_list_2    
        dataframe=pd.DataFrame(temp_dict)
        
        return dataframe

    def dataframe_generator(self,identifier,main_data_dict,processed_daily_data,no_months,last_year_months):
        if type(main_data_dict['processed'])!=str:
            current_date=processed_daily_data['rep_dt']['processed'].date()
            # print(current_date)
            # current_date=datetime.datetime(2016, 7, 16)#to be changed later only temporary use
            if no_months!=0:
                current_date=current_date+relativedelta(days=1)
                old_month=current_date-relativedelta(months=no_months)
                list_date=pd.date_range(old_month,current_date,freq='d')
                dataframe_final=self.time_dataframe_generator(identifier,main_data_dict,no_months,last_year_months,list_date)
            elif last_year_months!=0:
                last_year_current_month=current_date-relativedelta(months=12)
                last_year_prev_months=last_year_current_month-relativedelta(months=last_year_months)
                list_date=pd.date_range(last_year_prev_months,last_year_current_month,freq='d')
                dataframe=self.time_dataframe_generator(identifier,main_data_dict,no_months,last_year_months,list_date)
                list_date_2=pd.date_range(current_date,current_date+relativedelta(days=1),freq='d')
                dataframe_2=self.time_dataframe_generator(identifier,main_data_dict,no_months,last_year_months,list_date_2)
                dataframe_final=dataframe_2.append(dataframe)
        return dataframe_final


    def outlier_processor(self,identifier,dataframe,main_data_dict,processed_daily_data):
        limit_val=self.ship_configs['data'][identifier]['limits']['type']
        outlier_val=main_data_dict['within_outlier_limits']
        operational_val=main_data_dict['within_operational_limits']
        current_date=processed_daily_data['rep_dt']['processed'].date()
        dataframe=dataframe.dropna()
        dataframe=dataframe.reset_index(drop=True)
        dataframe=dataframe[dataframe[identifier]!='']
        dataframe=dataframe[dataframe[identifier]!=' ']
        dataframe=dataframe[dataframe[identifier]!='  ']
        dataframe=dataframe.reset_index(drop=True)
        # z_score_limit=st.norm.ppf(1-i)
        # print(dataframe)
        # exit()
        if len(dataframe)>=2:
            dataframe["z_score"]=st.zscore(dataframe[identifier])
            for i in range(0,len(dataframe['rep_dt'])):
                if dataframe['rep_dt'][i]==current_date:
                    try:
                        z_score=dataframe['z_score'][i]
                        return z_score,True
                    except:
                        return None,False  
        else:
            return None,False
                 

    def outlier_operational_z_score(self,z_score,z_score_limit):
        outlier_z_score={}
        if pd.isnull(z_score['m3']):
            outlier_z_score['m3']=None
        else:
            if z_score['m3']>=-z_score_limit and z_score['m3']<=z_score_limit:
                outlier_z_score['m3']=True
            else:
                outlier_z_score['m3']=False

        if pd.isnull(z_score['m6']):
            outlier_z_score['m6']=None
        else:
            if z_score['m6']>=-z_score_limit and z_score['m6']<=z_score_limit:
                outlier_z_score['m6']=True
            else:
                outlier_z_score['m6']=False

        if pd.isnull(z_score['m12']):
            outlier_z_score['m12']=None
        else:
            if z_score['m12']>=-z_score_limit and z_score['m12']<=z_score_limit:
                outlier_z_score['m12']=True
            else:
                outlier_z_score['m12']=False

        if pd.isnull(z_score['ly_m3']):
            outlier_z_score['ly_m3']=None
        else:
            if z_score['ly_m3']>=-z_score_limit and z_score['ly_m3']<=z_score_limit:
                outlier_z_score['ly_m3']=True
            else:
                outlier_z_score['ly_m3']=False

        if pd.isnull(z_score['ly_m6']):
            outlier_z_score['ly_m6']=None
        else:
            if z_score['ly_m6']>=-z_score_limit and z_score['ly_m6']<=z_score_limit:
                outlier_z_score['ly_m6']=True
            else:
                outlier_z_score['ly_m6']=False

        if pd.isnull(z_score['ly_m12']):
            outlier_z_score['ly_m12']=None
        else:
            if z_score['ly_m12']>=-z_score_limit and z_score['ly_m12']<=z_score_limit:
                outlier_z_score['ly_m12']=True
            else:
                outlier_z_score['ly_m12']=False
        return outlier_z_score

    
    
        # else:
        #     outlier=None
        #     operational=None
        #     z_score=None
        #     return outlier,operational,z_score

            
        # if pd.isnull(limit_val) or (limit_val!=1 and limit_val!=2 and limit_val!=3):
        #     limit_val=2
       
        # if limit_val==1:
        #     if outlier_val==True:
        #         outlier=True

        #     elif outlier_val==False:
        #         outlier=False
                
        #     elif outlier_val!=True and outlier_val!=False:
        #         outlier="not checked"
                
        #     if operational_val==True:
        #         operational=True
                
        #     elif operational_val==False:
        #         operational=False
                
        #     elif operational_val!=True and operational_val!=False:
        #         operational="not checked"

        #     if pd.isnull(z_score)==True:
        #         z_score=None
                
        #     return outlier,operational,z_score

        # elif limit_val==2:
        #     if outlier_val==True:
        #         outlier=True
                
        #     elif outlier_val==False:
        #         outlier=False
                
        #     elif outlier_val!=True and outlier_val!=False:
        #         if pd.isnull(z_score)==False or z_score!=None:
        #             if z_score<=3 and z_score>=-3:
        #                 outlier=True
        #             else:
        #                 outlier=False
                   
        #     if operational_val==True:
        #         operational=True
                
        #     elif operational_val==False:
        #         operational=False
                
        #     elif operational_val!=True and operational_val!=False:
        #         if pd.isnull(z_score)==False or z_score!=None:
        #             if z_score<=2 and z_score>=-2:
        #                 operational=True
        #             else:
        #                 operational=False

        #     if pd.isnull(z_score)==True:
        #         z_score=None

        #     return outlier,operational,z_score 

        # elif limit_val==3:
        #     if outlier_val==True:
        #         outlier=True
                
        #     elif outlier_val==False:
        #         outlier=False
                
        #     if pd.isnull(z_score)==False:
        #         if z_score<=3 or z_score>=-3:
        #             z_outlier=True
        #         else:
        #             z_outlier=False
                   
        #     if operational_val==True:
        #         operational=True
                
        #     elif operational_val==False:
        #         operational=False
           
        #     if pd.isnull(z_score)==False:
        #         if z_score<=2 and z_score>=-2:
        #             z_operational=True
        #         else:
        #             z_operational=False
            
        #     if pd.isnull(z_score)==True:
        #         z_outlier=None
        #         z_operational=None

        #     if z_outlier!=None:
        #         if z_outlier==outlier:
        #             outlier=True
        #         else:
        #             outlier=False
            
        #         if z_operational==operational:
        #             operational=True
        #         else:
        #             operational=False
            
        #     return outlier,operational,z_score 





    def base_prediction_processor(self,identifier,main_data_dict,processed_daily_data):
        main_data_dict=main_data_dict
        data_availabe={}
        z_score={}
        m3_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,3,0)
        z_score_m3,data_availabe_m3=self.outlier_processor(identifier,m3_dataframe,main_data_dict,processed_daily_data)
        z_score['m3']=z_score_m3
        data_availabe['m3']=data_availabe_m3
        
        m6_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,6,0)
        z_score_m6,data_availabe_m6=self.outlier_processor(identifier,m6_dataframe,main_data_dict,processed_daily_data)
        z_score['m6']=z_score_m6
        data_availabe['m6']=data_availabe_m6

        
        m12_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,12,0)
        z_score_m12,data_availabe_m12=self.outlier_processor(identifier,m12_dataframe,main_data_dict,processed_daily_data)
        z_score['m12']=z_score_m12
        data_availabe['m12']=data_availabe_m12
          
        ly_m3_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,0,3)
        z_score_ly_m3,data_availabe_ly_m3=self.outlier_processor(identifier,ly_m3_dataframe,main_data_dict,processed_daily_data)
        z_score['ly_m3']=z_score_ly_m3
        data_availabe['ly_m3']=data_availabe_ly_m3
        
        ly_m6_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,0,6)
        z_score_ly_m6,data_availabe_ly_m6=self.outlier_processor(identifier,ly_m6_dataframe,main_data_dict,processed_daily_data)
        z_score['ly_m6']=z_score_ly_m6
        data_availabe['ly_m6']=data_availabe_ly_m6
        
        ly_m12_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,0,12)
        z_score_ly_m12,data_availabe_ly_m12=self.outlier_processor(identifier,ly_m12_dataframe,main_data_dict,processed_daily_data)        
        z_score['ly_m12']=z_score_ly_m12
        data_availabe['ly_m12']=data_availabe_ly_m12

        return z_score,data_availabe