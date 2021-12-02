from __future__ import division


from numpy.core.fromnumeric import reshape, var
from numpy.core.function_base import linspace
from numpy.lib.function_base import diff
from numpy.lib.shape_base import dstack
from sklearn import linear_model

from datetime import date
import sys
import pprint
from numpy.core.defchararray import add, index, upper
import pandas
from pandas.core import base
from pandas.core.frame import DataFrame
from pandas.core.indexes.datetimes import date_range
from bson import json_util
from pandas.core.dtypes.missing import isnull 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
#from mongoengine import *

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
from plotly.subplots import make_subplots
import scipy.stats as st
import plotly.graph_objects as go
client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
database = db
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import Normalizer
from sklearn.preprocessing import Binarizer
from sklearn.preprocessing import scale
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import Lasso
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from sklearn.cross_decomposition import PLSRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import Ridge
from mpl_toolkits import mplot3d
import seaborn as sns
from sklearn.metrics import r2_score
import matplotlib.pyplot as mp
import seaborn as sb
import functools
from sklearn.cross_decomposition import PLSRegression



from scipy.stats import f


class Indice_Processing():


    def __init__(self,configs,md,ss,imo):
        self.ship_configs = configs
        self.main_data= md
        self.ship_stats= ss
        self.ship_imo=imo
        pass

    def time_dataframe_generator(self,identifier,dependent_variables,no_months,last_year_months,list_date,vsl_load):
        for i in identifier:
            dependent_variables.append(i)
        maindb = database.get_collection("Main_db")
        temp_list=[]
        for i in dependent_variables:
            self.main =maindb.find({"processed_daily_data.rep_dt.processed": {"$lte":list_date[-1], "$gte": list_date[0]},"ship_imo":self.ship_imo,"within_good_voyage_limit":True,'processed_daily_data.vsl_load_bal.processed':vsl_load},{"processed_daily_data."+i+".processed":1,"_id":0})
            # self.main =maindb.find({"processed_daily_data.rep_dt.processed": {"$lte":list_date[-1], "$gte": list_date[0]},"ship_imo":self.ship_imo},{"processed_daily_data."+i+".processed":1,"_id":0})
            mainobject=json_util.dumps(self.main)
            load=json_util.loads(mainobject)
            temp_list.append(load)
        # print(temp_list)
        temp_dict={}
        for i in range(len(temp_list)):
            temp_list_2=[]
            for j in range(0,len(temp_list[i])):    
                try:
                    temp_list_2.append(temp_list[i][j]['processed_daily_data'][dependent_variables[i]]['processed'])
                except KeyError:
                    temp_list_2.append(None)
            
            temp_dict[dependent_variables[i]]=temp_list_2    
        dataframe=pd.DataFrame(temp_dict)
        for i in identifier:
            dependent_variables.remove(i)
        return dataframe

    def dataframe_generator(self,identifier,dependent_variables,processed_daily_data,no_months,last_year_months):
        # if type(main_data_dict['processed'])!=str:
        current_date=processed_daily_data['rep_dt']['processed'].date()
        vsl_load=processed_daily_data['vsl_load_bal']['processed']
        # print(current_date)
        # current_date=datetime.datetime(2016, 7, 16)#to be changed later only temporary use
        # if no_months!=0:
        #     old_month=current_date-relativedelta(months=no_months)
        #     list_date=pd.date_range(old_month,current_date,freq='d')
        #     dataframe=self.time_dataframe_generator(identifier,dependent_variables,no_months,last_year_months,list_date,vsl_load)
        # elif last_year_months!=0:
        #     last_year_current_month=current_date-relativedelta(months=12)
        #     last_year_prev_months=last_year_current_month-relativedelta(months=last_year_months)
        #     list_date=pd.date_range(last_year_prev_months,last_year_current_month,freq='d')
        #     dataframe=self.time_dataframe_generator(identifier,dependent_variables,no_months,last_year_months,list_date,vsl_load)

        if no_months!=0:
            old_month=current_date-relativedelta(months=no_months)
            list_date=pd.date_range(old_month,current_date,freq='d')
            dataframe=self.time_dataframe_generator(identifier,dependent_variables,no_months,last_year_months,list_date,vsl_load)
            if len(dataframe)<=25:
                old_month=current_date-relativedelta(months=12)
                list_date=pd.date_range(old_month,current_date,freq='d')
                temp_data=self.time_dataframe_generator(identifier,dependent_variables,no_months,last_year_months,list_date,vsl_load)
                try:
                    temp_dataframe=temp_data.head(30)
                    return temp_dataframe
                except:
                    return dataframe
            else:
                return dataframe
        elif last_year_months!=0:
            last_year_current_month=current_date-relativedelta(months=12)
            last_year_prev_months=last_year_current_month-relativedelta(months=last_year_months)
            list_date=pd.date_range(last_year_prev_months,last_year_current_month,freq='d')
            dataframe=self.time_dataframe_generator(identifier,dependent_variables,no_months,last_year_months,list_date,vsl_load)
            if len(dataframe)<=25:
                last_year_current_month=current_date-relativedelta(months=12)
                last_year_prev_months=last_year_current_month-relativedelta(months=12)
                list_date=pd.date_range(last_year_prev_months,last_year_current_month,freq='d')
                temp_data=self.time_dataframe_generator(identifier,dependent_variables,no_months,last_year_months,list_date,vsl_load)
                try:
                    temp_dataframe=temp_data.head(30)
                    return temp_dataframe
                except:
                    return dataframe
            else:
                return dataframe

    # dataframe after the given date
    def dataframe_generator_new(self,identifier,dependent_variables,no_months,last_year_months):
        # if type(main_data_dict['processed'])!=str:
        # current_date=self.main_data['processed_daily_data']['rep_dt']['processed'].date()
        current_date=datetime.datetime(2016, 7, 16)#to be changed later only temporary use
        if no_months!=0:
            old_month=current_date+relativedelta(months=no_months)
            list_date=pd.date_range(current_date,old_month,freq='d')
            dataframe=self.time_dataframe_generator(identifier,dependent_variables,no_months,last_year_months,list_date)
        elif last_year_months!=0:
            last_year_current_month=current_date-relativedelta(months=12)
            last_year_prev_months=last_year_current_month-relativedelta(months=last_year_months)
            list_date=pd.date_range(last_year_prev_months,last_year_current_month,freq='d')
            dataframe=self.time_dataframe_generator(identifier,dependent_variables,no_months,last_year_months,list_date)
        return dataframe

    def prediction(self,identifier,dataframe,processed_daily_data,dependent_variables):
        length_dataframe=len(dataframe)
        
        if length_dataframe>25:
            if 'rep_dt' in dataframe.columns:
                dataframe=dataframe.drop(columns='rep_dt')
                
            if 'rep_dt' in dependent_variables:
                dependent_variables.remove('rep_dt')
            for column in dataframe:
                dataframe=dataframe[dataframe[column]!='']
                dataframe=dataframe[dataframe[column]!=' ']
                dataframe=dataframe[dataframe[column]!='  ']
                dataframe=dataframe[dataframe[column]!='r[a-zA-Z]']
            # dataframe=dataframe.drop(columns='current_dir_rel')
            dataframe=dataframe.reset_index(drop=True)
            dataframe=dataframe.dropna()
            dataframe=dataframe.reset_index(drop=True)

            
            for col in dataframe.columns:
                for i in range(0,len(dataframe)):
                    if i in dataframe.index and type(dataframe[col][i])==str:
                        
                        dataframe=dataframe[dataframe[col]!=dataframe[col][i]]
                    
                    if i in dataframe.index and type(dataframe[col][i])==datetime.datetime:
                        
                        dataframe=dataframe[dataframe[col]!=dataframe[col][i]]
                        
                # dataframe = dataframe[~dataframe[col].contains("[a-zA-Z]").fillna(False)]

                if pd.isnull(dataframe[col]).all():
                    dataframe=dataframe.drop(columns=col)
                
            
            dataframe=dataframe.dropna()
                    
            dataframe=dataframe.reset_index(drop=True)
        
            if len(dataframe)>=5 and len(dataframe.columns)>2:
                x=[]
                y=identifier
                for col in dataframe.columns:
                    if col not in identifier:
                        x.append(col)

                for i in identifier:
                    z_score=st.zscore(dataframe[i])
                    dataframe['z_score']=z_score
                    dataframe= dataframe.drop(index=dataframe[dataframe['z_score'] > 2].index)
                    dataframe= dataframe.drop(index=dataframe[dataframe['z_score'] < -2].index)
                    dataframe=dataframe.reset_index(drop=True)
                    dataframe=dataframe.drop(columns='z_score')

                
                try:
                    tempdict={}
                    col_list=x
                    for i in y:
                        col_list.append(i)
                    for i in col_list:
                        if i in processed_daily_data :
                            # if pd.isnull(processed_daily_data[i]['processed'])==True or type(processed_daily_data[i]['processed'])==str or type(processed_daily_data[i]['processed'])==datetime.datetime:
                            #     tempdict[i]=np.mean(dataframe[i])
                            # else:
                            tempdict[i]=processed_daily_data[i]['processed']
                        else:
                            tempdict[i]=None
                    for i in y:
                        col_list.remove(i)
                    # print(tempdict)
                    temp_dict_2={}
                    tempdataframe=pd.DataFrame(temp_dict_2,columns=tempdict.keys())
                    # print(tempdataframe)
                    tempdataframe=tempdataframe.append(tempdict,ignore_index=True)
                    new_data=dataframe
                    new_data=new_data.append(tempdataframe)
                    new_data=new_data.reset_index(drop=True)
                    # print(tempdataframe)
                    # dataframe=dataframe.append(tempdataframe)
                    # dataframe=dataframe.reset_index(drop=True)
                    

                    X_train=dataframe[x]
                    training_dict={}
                    testing_dict={}
                    training_dataframe=pd.DataFrame(training_dict)
                    testing_dataframe=pd.DataFrame(testing_dict)
                    sc=StandardScaler()
                    X_train=sc.fit_transform(dataframe[x])
                    X_test=sc.fit_transform(new_data[x])
                    # Y_train=sc.fit_transform(dataframe["pwr"])
                    # print(Y_train)
                    pred_list_upper=[]
                    spe_list_upper=[]
                    spe_dataframe=pd.DataFrame()
                    
                    for id in identifier:
                        std_y=np.std(dataframe[id])
                        mean_y=np.mean(dataframe[id])
                        y_list=[]
                        test_y_list=[]
                        for i in dataframe[id]:
                            val=(i-mean_y)/std_y
                            y_list.append(val)
                        for i in new_data[id]:
                            val=(i-mean_y)/std_y
                            test_y_list.append(val)
                        training_dataframe[id]=y_list
                        Y_train=training_dataframe[id]
                        testing_dataframe[id]=test_y_list
                        Y_test=testing_dataframe    
                        
                    #     # pls_reg=PLSRegression(n_components=2)
                    #     # pls_reg.fit(X_train, dataframe[y])
                        if len(x)>5:
                            pls_reg=LRPI()
                            pls_reg.fit(X_train,Y_train)
                            pls_t2_list=['plsscore1','plsscore2','plsscore3','plsscore4']
                        else:
                            pls_reg=LRPI_low_x()
                            pls_reg.fit(X_train,Y_train)
                            pls_t2_list=['plsscore1','plsscore2']
                        
                        pred=pls_reg.predict(X_test)
                        
                        spe_dataframe[id+'_spe']=(pred['Pred']-Y_test[id])**2
                        pred_list=[]
                        predcol=["lower","Pred","upper"]
                        
                        for col in predcol:
                            val=(pred[col].iloc[-1]*std_y)+mean_y
                            pred_list.append(round(val,2))
                        pred_list_upper.append(pred_list[1])
                        spe=round((pred['Pred'].iloc[-1]-Y_test[id].iloc[-1])**2,4)
                        spe_list_upper.append(spe)
                    
                    spe_value=sum(spe_list_upper)
                    
                    spe_sum=spe_dataframe.sum(axis=1)
                    var_sped=np.var(spe_sum)
                    mean_var=np.mean(spe_sum)
                    # h_val=(2*(mean_var**2))/(var_sped)
                    # g_val=(var_sped)/(2*(mean_var))
                    
                    param_alpha=[]
                    param_alpha.append(self.ship_configs['parameter_anamoly']['SPE_alpha1']['alpha'])
                    param_alpha.append(self.ship_configs['parameter_anamoly']['SPE_alpha2']['alpha'])
                    param_alpha.append(self.ship_configs['parameter_anamoly']['SPE_alpha3']['alpha'])

                    spe_y_limit_array=[]
                    spe_anamoly=[]
                    # for i in param_alpha:
                    #     chi_val=st.chi2.ppf(1-i, h_val)
                    #     spe_limit_g_val=g_val*chi_val
                    #     spe_y_limit_array.append(spe_limit_g_val)
                    #     if spe_limit_g_val<spe:
                    #         spe_anamoly.append(False)
                    #     else:
                    #         spe_anamoly.append(True)
                    
                
                    pls_reg_2=PLSRegression(n_components=len(x))
                    pls_reg_2.fit(X_test,Y_test)
                    pls_reg_3=PLSRegression(n_components=len(identifier))
                    pls_reg_3.fit(X_test,Y_test)
                    pls_col=[]

                    for i in range(1,len(x)+1):
                        pls_col.append("plsscore"+str(i))
                    
                    pls_dataframe = pd.DataFrame(data = pls_reg_2.x_scores_, columns = pls_col)
                    pls_y_dataframe = pd.DataFrame(data = pls_reg_3.y_scores_, columns = identifier)
                    # pls_dataframe['pwr']=Y_train
                    for i in identifier:
                        pls_dataframe[i]=pls_y_dataframe[i]
                    dis_listnew=[]

                    if len(x)>4:
                        var_list=[]
                        for i in range(4,len(pls_col)):
                            var_list.append(np.var(pls_dataframe[pls_col[i]]))
                        cube_list=[]
                        for i in var_list:
                            cube_list.append(i**3)
                        theta_1=round(sum(var_list),3)
                        theta_2 = round(functools.reduce(lambda i, j: i + j * j, [var_list[:1][0]**2]+var_list[1:]),3)
                        g_val=theta_2/theta_1
                        h_val=(theta_1**2)/theta_2
                        for i in param_alpha:
                            chi_val=st.chi2.ppf(1-i, h_val)
                            spe_limit_g_val=g_val*chi_val
                            spe_y_limit_array.append(spe_limit_g_val)
                            if spe_limit_g_val < spe:
                                spe_anamoly.append(False)
                            elif spe_limit_g_val > spe:
                                spe_anamoly.append(True)
                    else:
                        spe_anamoly=None
                        spe_y_limit_array=None



                    
                    
                    for i in range(0,len(pls_dataframe[pls_t2_list])):
                        data=np.array(pls_dataframe[pls_t2_list])
                        X_feat=np.array(pls_dataframe[pls_t2_list].iloc[[i]])
                        mean=np.mean(data,axis=0)
                        X_feat_mean=X_feat-mean
                        data=np.transpose(data)
                        data=data.astype(float)
                        cov=np.cov(data,bias=False)
                        inv_cov=np.linalg.pinv(cov)
                        tem1=np.dot(X_feat_mean,inv_cov)
                        temp2=np.dot(tem1,np.transpose(X_feat_mean))
                        m_dis=np.sqrt(temp2[0][0])
                        dis_listnew.append(m_dis)

                    pls_dataframe['t_2']=dis_listnew
                    t2_initial=pls_dataframe['t_2'].iloc[-1].round(3)

                    
                    m=length_dataframe
                    p=4
                    cl=((m-1)**2)/m
                    numerator=p*(m -1)
                    denominator=m-p
                    multiplier=numerator/denominator
                    # lcl=cl*(st.beta.ppf(0.01, p/2, (m - p - 1) / 2))
                    # center=cl*(st.beta.ppf(0.5,p/2,(m-p-1)/2))
                    # ucl=cl*(st.beta.ppf(0.75,p/2,(m-p-1)/2))
                    t_2limit=st.f.ppf(1-0.1,p,m-p)*multiplier
                    # t_2limit=ucl-center
                    crit_data=t_2limit
                    if t2_initial>crit_data:
                        t2_anamoly=False
                    else:
                        t2_anamoly=True
                    
                    crit_val_dynamic=None
                    t2_final=None
                    print("donweeeeeeeeeeeeeeeeeee")
                    return spe_value,spe_y_limit_array,crit_data,crit_val_dynamic,t2_initial,t2_final,length_dataframe,spe_anamoly,t2_anamoly
                    # pls_dataframe=pls_dataframe.drop(index=pls_dataframe[pls_dataframe['t_2'] > t_2limit].index)
                    # if len(pls_dataframe)>=2:
                    #     if dataframe.index[-1]==pls_dataframe.index[-1]:
                    #         pls_dataframe=pls_dataframe.reset_index(drop=True)
                    #         del pls_dataframe['t_2']
                    #         dis_listnew=[]
                            
                    #         for i in range(0,len(pls_dataframe[pls_t2_list])):
                    #             data=np.array(pls_dataframe[pls_t2_list])
                    #             X_feat=np.array(pls_dataframe[pls_t2_list].iloc[[i]])
                    #             mean=np.mean(data,axis=0)
                    #             X_feat_mean=X_feat-mean
                    #             data=np.transpose(data)
                    #             data=data.astype(float)
                    #             cov=np.cov(data,bias=False)
                    #             inv_cov=np.linalg.pinv(cov)
                    #             tem1=np.dot(X_feat_mean,inv_cov)
                    #             temp2=np.dot(tem1,np.transpose(X_feat_mean))
                    #             m_dis=np.sqrt(temp2[0][0])
                    #             dis_listnew.append(m_dis)

                    #         pls_dataframe['t_2']=dis_listnew
                    #         t2_final=pls_dataframe['t_2'].iloc[-1].round(3)
                    #         m=len(pls_dataframe)
                    #         p=2
                    #         if m>p:
                    #             dfn=m-p-1
                    #             dfd=p*(m-2)
                    #             cl=(p*(m-1))/(m-p)
                    #             f_val_left=st.f.ppf(0.10,dfn,dfd)*cl
                    #             f_val_right=st.f.ppf(1-0.10,dfn,dfd)*cl
                    #             f_val_two=st.f.ppf((1-0.10)/2,dfn,dfd)*cl
                    #             crit_val_dynamic=f_val_right
                            
                    #         else:
                    #             crit_val_dynamic=None
                    #         # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                    #         return spe_value,spe_y_limit_array,crit_data,crit_val_dynamic,t2_initial,t2_final
                            
                    #     else:
                    #         new_dataframe=pd.DataFrame(columns=dataframe.columns)
                    #         for i in pls_dataframe.index:
                    #             new_dataframe=new_dataframe.append(dataframe.iloc[i])
                    #         new_dataframe=new_dataframe.append(tempdataframe)
                    #         new_dataframe=new_dataframe.reset_index(drop=True)
                    #         pls_reg_new=PLSRegression(n_components=len(x))
                    #         pls_reg_new.fit(new_dataframe[x],new_dataframe[y])
                    #         pls_col_list=[]
                            
                    #         for i in range(1,len(x)+1):
                    #             pls_col_list.append("plsscore"+str(i))
                    #         new_pls_dataframe=pd.DataFrame(data=pls_reg_new.x_scores_,columns=pls_col_list)
                    #         new_pls_dataframe[y]=new_dataframe[y]
                    #         dis_listnew=[]
                    #         for i in range(0,len(new_pls_dataframe[pls_t2_list])):
                    #             data=np.array(new_pls_dataframe[pls_t2_list])
                    #             X_feat=np.array(new_pls_dataframe[pls_t2_list].iloc[[i]])
                    #             mean=np.mean(data,axis=0)
                    #             X_feat_mean=X_feat-mean
                    #             data=np.transpose(data)
                    #             data=data.astype(float)
                    #             cov=np.cov(data,bias=False)
                    #             inv_cov=np.linalg.pinv(cov)
                    #             tem1=np.dot(X_feat_mean,inv_cov)
                    #             temp2=np.dot(tem1,np.transpose(X_feat_mean))
                    #             m_dis=np.sqrt(temp2[0][0])
                    #             dis_listnew.append(m_dis)

                    #         new_pls_dataframe['t_2']=dis_listnew


                    #         m=len(new_pls_dataframe)
                    #         p=2
                    #         if m>p:
                    #             dfn=m-p-1
                    #             dfd=p*(m-2)
                    #             cl=(p*(m-1))/(m-p)
                    #             f_val_left=st.f.ppf(0.10,dfn,dfd)*cl
                    #             f_val_right=st.f.ppf(1-0.10,dfn,dfd)*cl
                    #             f_val_two=st.f.ppf((1-0.10)/2,dfn,dfd)*cl
                    #             crit_val_dynamic=f_val_right
                    #         else:
                    #             crit_val_dynamic=None
                    #         t2_final=new_pls_dataframe['t_2'].iloc[-1].round(3)
                    #     # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                    #     return spe_value,spe_y_limit_array,crit_data,crit_val_dynamic,t2_initial,t2_final
                    # else:
                    #     crit_val_dynamic=None
                    #     t2_final=None
                    #     # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                    #     return spe_value,spe_y_limit_array,crit_data,crit_val_dynamic,t2_initial,t2_final
                except:
                    spe_value=None
                    spe_y_limit_array=None
                    crit_data=None
                    crit_val_dynamic=None
                    t2_initial=None
                    t2_final=None
                    spe_anamoly=None
                    t2_anamoly=None
                    # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                    return spe_value,spe_y_limit_array,crit_data,crit_val_dynamic,t2_initial,t2_final,length_dataframe,spe_anamoly,t2_anamoly

            else:
                spe_value=None
                spe_y_limit_array=None
                crit_data=None
                crit_val_dynamic=None
                t2_initial=None
                t2_final=None
                spe_anamoly=None
                t2_anamoly=None
                # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                return spe_value,spe_y_limit_array,crit_data,crit_val_dynamic,t2_initial,t2_final,length_dataframe,spe_anamoly,t2_anamoly
        else:
            spe_value=None
            spe_y_limit_array=None
            crit_data=None
            crit_val_dynamic=None
            t2_initial=None
            t2_final=None
            spe_anamoly=None
            t2_anamoly=None
            # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
            return spe_value,spe_y_limit_array,crit_data,crit_val_dynamic,t2_initial,t2_final,length_dataframe,spe_anamoly,t2_anamoly
            


    def base_prediction_processor(self,identifier,dependent_variables,processed_daily_data):
        spe={}
        spe_chi_square={}
        crit_data={}
        crit_val_dynamic={}
        t2_initial={}
        t2_final={}
        length_dataframe={}
        spe_anamoly={}
        t2_anamoly={}

        m3_dataframe=self.dataframe_generator(identifier,dependent_variables,processed_daily_data,3,0)
        m3_spe,m3_spe_chisquare,crit_data_m3,crit_val_dynamic_m3,t2_initial_m3,t2_final_m3,ld_m3,spe_anamoly_m3,t2_anamoly_m3=self.prediction(identifier,m3_dataframe,processed_daily_data,dependent_variables)
        spe['m3']=m3_spe
        spe_chi_square['m3']=m3_spe_chisquare
        crit_data['m3']=crit_data_m3
        crit_val_dynamic['m3']=crit_val_dynamic_m3
        t2_initial['m3']=t2_initial_m3
        t2_final['m3']=t2_final_m3
        length_dataframe['m3']=ld_m3
        spe_anamoly['m3']=spe_anamoly_m3
        t2_anamoly['m3']=t2_anamoly_m3

        # exit()
        m6_dataframe=self.dataframe_generator(identifier,dependent_variables,processed_daily_data,6,0)
        m6_spe,m6_spe_chisquare,crit_data_m6,crit_val_dynamic_m6,t2_initial_m6,t2_final_m6,ld_m6,spe_anamoly_m6,t2_anamoly_m6=self.prediction(identifier,m6_dataframe,processed_daily_data,dependent_variables)
        spe['m6']=m6_spe
        spe_chi_square['m6']=m6_spe_chisquare
        crit_data['m6']=crit_data_m6
        crit_val_dynamic['m6']=crit_val_dynamic_m6
        t2_initial['m6']=t2_initial_m6
        t2_final['m6']=t2_final_m6
        length_dataframe['m6']=ld_m6
        spe_anamoly['m6']=spe_anamoly_m6
        t2_anamoly['m6']=t2_anamoly_m6

        m12_dataframe=self.dataframe_generator(identifier,dependent_variables,processed_daily_data,12,0)
        m12_spe,m12_spe_chisquare,crit_data_m12,crit_val_dynamic_m12,t2_initial_m12,t2_final_m12,ld_m12,spe_anamoly_m12,t2_anamoly_m12=self.prediction(identifier,m12_dataframe,processed_daily_data,dependent_variables)
        spe['m12']=m12_spe
        spe_chi_square['m12']=m12_spe_chisquare
        crit_data['m12']=crit_data_m12
        crit_val_dynamic['m12']=crit_val_dynamic_m12
        t2_initial['m12']=t2_initial_m12
        t2_final['m12']=t2_final_m12
        length_dataframe['m12']=ld_m12
        spe_anamoly['m12']=spe_anamoly_m12
        t2_anamoly['m12']=t2_anamoly_m12
        
        ly_m3_dataframe=self.dataframe_generator(identifier,dependent_variables,processed_daily_data,0,3)
        ly_m3_spe,ly_m3_spe_chisquare,crit_data_ly_m3,crit_val_dynamic_ly_m3,t2_initial_ly_m3,t2_final_ly_m3,ld_ly_m3,spe_anamoly_ly_m3,t2_anamoly_ly_m3=self.prediction(identifier,ly_m3_dataframe,processed_daily_data,dependent_variables)
        spe['ly_m3']=ly_m3_spe
        spe_chi_square['ly_m3']=ly_m3_spe_chisquare
        crit_data['ly_m3']=crit_data_ly_m3
        crit_val_dynamic['ly_m3']=crit_val_dynamic_ly_m3
        t2_initial['ly_m3']=t2_initial_ly_m3
        t2_final['ly_m3']=t2_final_ly_m3
        length_dataframe['ly_m3']=ld_ly_m3
        spe_anamoly['ly_m3']=spe_anamoly_ly_m3
        t2_anamoly['ly_m3']=t2_anamoly_ly_m3

        ly_m6_dataframe=self.dataframe_generator(identifier,dependent_variables,processed_daily_data,0,6)
        ly_m6_spe,ly_m6_spe_chisquare,crit_data_ly_m6,crit_val_dynamic_ly_m6,t2_initial_ly_m6,t2_final_ly_m6,ld_ly_m6,spe_anamoly_ly_m6,t2_anamoly_ly_m6=self.prediction(identifier,ly_m6_dataframe,processed_daily_data,dependent_variables)
        spe['ly_m6']=ly_m6_spe
        spe_chi_square['ly_m6']=ly_m6_spe_chisquare
        crit_data['ly_m6']=crit_data_ly_m6
        crit_val_dynamic['ly_m6']=crit_val_dynamic_ly_m6
        t2_initial['ly_m6']=t2_initial_ly_m6
        t2_final['ly_m6']=t2_final_ly_m6
        length_dataframe['ly_m6']=ld_ly_m6
        spe_anamoly['ly_m6']=spe_anamoly_ly_m6
        t2_anamoly['ly_m6']=t2_anamoly_ly_m6

        ly_m12_dataframe=self.dataframe_generator(identifier,dependent_variables,processed_daily_data,0,12)
        ly_m12_spe,ly_m12_spe_chisquare,crit_data_ly_m12,crit_val_dynamic_ly_m12,t2_initial_ly_m12,t2_final_ly_m12,ld_ly_m12,spe_anamoly_ly_m12,t2_anamoly_ly_m12=self.prediction(identifier,ly_m12_dataframe,processed_daily_data,dependent_variables)
        spe['ly_m12']=ly_m12_spe
        spe_chi_square['ly_m12']=ly_m12_spe_chisquare
        crit_data['ly_m12']=crit_data_ly_m12
        crit_val_dynamic['ly_m12']=crit_val_dynamic_ly_m12
        t2_initial['ly_m12']=t2_initial_ly_m12
        t2_final['ly_m12']=t2_final_ly_m12
        length_dataframe['ly_m12']=ld_ly_m12
        spe_anamoly['ly_m12']=spe_anamoly_ly_m12
        t2_anamoly['ly_m12']=t2_anamoly_ly_m12
        
        # print(spe_limit_array)
        # print(pred)
        # exit()
        
        return spe,spe_chi_square,crit_data,crit_val_dynamic,t2_initial,t2_final,length_dataframe,spe_anamoly,t2_anamoly

    # def rpm_processor(self,main_data_dict,processed_daily_data):
        
    #     return self.base_prediction_processor('pwr',main_data_dict,['trim','w_force','rep_dt','sea_st','speed_stw_calc','rpm','draft_mean','dft_aft','dft_fwd'],processed_daily_data)





class LRPI:
    def __init__(self, normalize=False, n_jobs=1, t_value = 2.13144955):
        self.normalize = normalize
        self.n_jobs = n_jobs
        self.LR = PLSRegression(n_components=4)
        self.t_value = t_value
        
    def fit(self, X_train, y_train):
        self.X_train = pd.DataFrame(X_train)
        self.y_train = pd.DataFrame(y_train)
        self.LR.fit(self.X_train, self.y_train)
        X_train_fit = self.LR.predict(self.X_train)
        self.MSE = np.power(self.y_train.subtract(X_train_fit), 2).sum(axis=0) / (self.X_train.shape[0] - self.X_train.shape[1] - 1)
        self.X_train.loc[:, 'const_one'] = 1
        self.XTX_inv = np.linalg.pinv(np.dot(np.transpose(self.X_train.values.astype(float)) , self.X_train.values.astype(float)))
        
    def predict(self, X_test):
        self.X_test = pd.DataFrame(X_test)
        self.pred = self.LR.predict(self.X_test)
        self.X_test.loc[: , 'const_one'] =1
        SE = [np.dot(np.transpose(self.X_test.values[i]) , np.dot(self.XTX_inv, self.X_test.values[i]) ) for i in range(len(self.X_test)) ]
        quant=np.quantile(SE,0.9)
        if SE[-1]>quant:
            print("greaaaaaaaaaaaaat")
            SE=[0.3]*len(self.X_test)
        results = pd.DataFrame(self.pred , columns=['Pred'])
        # print(self.MSE.values + np.multiply(SE,self.MSE.values))
        results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        return results

class LRPI_low_x:
    def __init__(self, normalize=False, n_jobs=1, t_value = 2.13144955):
        self.normalize = normalize
        self.n_jobs = n_jobs
        self.LR = PLSRegression()
        self.t_value = t_value
        
    def fit(self, X_train, y_train):
        self.X_train = pd.DataFrame(X_train.values)
        self.y_train = pd.DataFrame(y_train.values)
        self.LR.fit(self.X_train, self.y_train)
        X_train_fit = self.LR.predict(self.X_train)
        self.MSE = np.power(self.y_train.subtract(X_train_fit), 2).sum(axis=0) / (self.X_train.shape[0] - self.X_train.shape[1] - 1)
        self.X_train.loc[:, 'const_one'] = 1
        self.XTX_inv = np.linalg.pinv(np.dot(np.transpose(self.X_train.values) , self.X_train.values))
        
    def predict(self, X_test):
        self.X_test = pd.DataFrame(X_test.values)
        self.pred = self.LR.predict(self.X_test)
        self.X_test.loc[: , 'const_one'] =1
        SE = [np.dot(np.transpose(self.X_test.values[i]) , np.dot(self.XTX_inv, self.X_test.values[i]) ) for i in range(len(self.X_test)) ]
        quant=np.quantile(SE,0.9)
        if SE[-1]>quant:
            print("greaaaaaaaaaaaaat")
            SE=[0.3]*len(self.X_test)
        results = pd.DataFrame(self.pred , columns=['Pred'])
        results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        return results