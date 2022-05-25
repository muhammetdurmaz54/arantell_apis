from __future__ import division
from asyncio import new_event_loop
from hashlib import new
from re import escape
from statistics import stdev
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
from src.db.setup_mongo import connect_db




np.seterr(divide='ignore', invalid='ignore')



class OutlierTwo():
    def __init__(self,configs,md,imo):
        self.ship_configs = configs
        self.main_data= md
        self.ship_imo=imo
    


    def dataframe_generator(self,identifier,main_data_dict,processed_daily_data,no_months,last_year_months,dataframe):
        current_date=processed_daily_data['rep_dt']['processed'].date()
        if no_months is not None:
            new_date=current_date-relativedelta(months=no_months)
            new_data=dataframe.loc[(dataframe['rep_dt'] >= new_date) & (dataframe['rep_dt'] < current_date)]
            curr_data=dataframe.loc[(dataframe['rep_dt'] == current_date)]
        elif last_year_months is not None:
            newyeardate=current_date-relativedelta(months=12)
            new_date=newyeardate-relativedelta(months=last_year_months)
            new_data=dataframe.loc[(dataframe['rep_dt'] >= new_date) & (dataframe['rep_dt'] < newyeardate)]
            curr_data=dataframe.loc[(dataframe['rep_dt'] == current_date)]
        new_data=new_data.append(curr_data)
        new_data=new_data.reset_index(drop=True)

        return new_data



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
            mean_val=np.mean(dataframe[identifier])
            standdev=np.std(dataframe[identifier])
            newlist=[]
            try:
                dataframe["z_score"]=st.zscore(dataframe[identifier])
            except:
                for i in dataframe[identifier]:
                    zsc=(i-mean_val)/standdev
                    newlist.append(zsc)
                dataframe["z_score"]=newlist
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



    def base_prediction_processor(self,identifier,main_data_dict,processed_daily_data,dataframe):
        main_data_dict=main_data_dict
        data_availabe={}
        z_score={}
        m3_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,3,0,dataframe)
        z_score_m3,data_availabe_m3=self.outlier_processor(identifier,m3_dataframe,main_data_dict,processed_daily_data)
        z_score['m3']=z_score_m3
        data_availabe['m3']=data_availabe_m3
        
        m6_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,6,0,dataframe)
        z_score_m6,data_availabe_m6=self.outlier_processor(identifier,m6_dataframe,main_data_dict,processed_daily_data)
        z_score['m6']=z_score_m6
        data_availabe['m6']=data_availabe_m6

        
        m12_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,12,0,dataframe)
        z_score_m12,data_availabe_m12=self.outlier_processor(identifier,m12_dataframe,main_data_dict,processed_daily_data)
        z_score['m12']=z_score_m12
        data_availabe['m12']=data_availabe_m12
          
        ly_m3_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,0,3,dataframe)
        z_score_ly_m3,data_availabe_ly_m3=self.outlier_processor(identifier,ly_m3_dataframe,main_data_dict,processed_daily_data)
        z_score['ly_m3']=z_score_ly_m3
        data_availabe['ly_m3']=data_availabe_ly_m3
        
        ly_m6_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,0,6,dataframe)
        z_score_ly_m6,data_availabe_ly_m6=self.outlier_processor(identifier,ly_m6_dataframe,main_data_dict,processed_daily_data)
        z_score['ly_m6']=z_score_ly_m6
        data_availabe['ly_m6']=data_availabe_ly_m6
        
        ly_m12_dataframe=self.dataframe_generator(identifier,main_data_dict,processed_daily_data,0,12,dataframe)
        z_score_ly_m12,data_availabe_ly_m12=self.outlier_processor(identifier,ly_m12_dataframe,main_data_dict,processed_daily_data)        
        z_score['ly_m12']=z_score_ly_m12
        data_availabe['ly_m12']=data_availabe_ly_m12

        return z_score,data_availabe