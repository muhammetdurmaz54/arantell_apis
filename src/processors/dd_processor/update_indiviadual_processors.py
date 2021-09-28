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
from src.db.schema.ship import Ship 
from src.processors.config_extractor.outlier import CheckOutlier
from src.processors.dd_processor.regress import regress
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


class UpdateIndividualProcessors():


    def __init__(self,configs,md,ss,imo):
        self.ship_configs = configs
        self.main_data= md
        self.ship_stats= ss
        self.ship_imo=imo
        pass

    def time_dataframe_generator(self,identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date):
        dependent_variables.append(identifier)
        maindb = database.get_collection("Main_db")
        temp_list=[]
        for i in dependent_variables:
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
                    temp_list_2.append(temp_list[i][j]['processed_daily_data'][dependent_variables[i]]['processed'])
                except KeyError:
                    temp_list_2.append(None)
            
            temp_dict[dependent_variables[i]]=temp_list_2    
        dataframe=pd.DataFrame(temp_dict)
        dependent_variables.remove(identifier)
        return dataframe

    def dataframe_generator(self,identifier,main_data_dict,dependent_variables,processed_daily_data,no_months,last_year_months):
        if type(main_data_dict['processed'])!=str:
            current_date=processed_daily_data['rep_dt']['processed'].date()
            # print(current_date)
            # current_date=datetime.datetime(2016, 7, 16)#to be changed later only temporary use
            if no_months!=0:
                old_month=current_date-relativedelta(months=no_months)
                list_date=pd.date_range(old_month,current_date,freq='d')
                dataframe=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date)
            elif last_year_months!=0:
                last_year_current_month=current_date-relativedelta(months=12)
                last_year_prev_months=last_year_current_month-relativedelta(months=last_year_months)
                list_date=pd.date_range(last_year_prev_months,last_year_current_month,freq='d')
                dataframe=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date)
        return dataframe

    # dataframe after the given date
    def dataframe_generator_new(self,identifier,main_data_dict,dependent_variables,no_months,last_year_months):
        if type(main_data_dict['processed'])!=str:
            # current_date=self.main_data['processed_daily_data']['rep_dt']['processed'].date()
            current_date=datetime.datetime(2016, 7, 16)#to be changed later only temporary use
            if no_months!=0:
                old_month=current_date+relativedelta(months=no_months)
                list_date=pd.date_range(current_date,old_month,freq='d')
                dataframe=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date)
            elif last_year_months!=0:
                last_year_current_month=current_date-relativedelta(months=12)
                last_year_prev_months=last_year_current_month-relativedelta(months=last_year_months)
                list_date=pd.date_range(last_year_prev_months,last_year_current_month,freq='d')
                dataframe=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date)
        return dataframe

    def prediction(self,identifier,dataframe,processed_daily_data,dependent_variables):
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
        
        for col in dataframe.columns:
            # print(col)
            for i in range(0,len(dataframe)):
                # print(dataframe[col][i])
                if i in dataframe.index and type(dataframe[col][i])==str:
                    
                    dataframe=dataframe[dataframe[col]!=dataframe[col][i]]
                
                if i in dataframe.index and type(dataframe[col][i])==datetime.datetime:
                    
                    dataframe=dataframe[dataframe[col]!=dataframe[col][i]]
                    
            # dataframe = dataframe[~dataframe[col].contains("[a-zA-Z]").fillna(False)]

            if pd.isnull(dataframe[col]).all():
                dataframe=dataframe.drop(columns=col)
            

        dataframe=dataframe.dropna()
        # print(dataframe)
        
        # dataframe=dataframe.reset_index(drop=True)
        

        # ax=sb.heatmap(dataframe.corr(), cmap="YlGnBu", annot=True)
        # mp.show()




        if len(dataframe)>=2 and identifier in dataframe.columns:
            x=[]
            y=identifier
            for col in dataframe.columns:
                x.append(col)
            x.remove(y)
            
            # x=['cpress','w_force','draft_mean','rpm','speed_sog']
            # y=identifier
        
            # sc=StandardScaler()
            # X_train=sc.fit_transform(dataframe[x])
            # print(X_train)
            # Y_train=sc.fit(tempdataframe[y])

            # minmax=MinMaxScaler()
            # X_train=minmax.fit_transform(dataframe[x])
            

            # norm=Normalizer()
            # X_train=norm.fit_transform(dataframe[x])

            z_score=st.zscore(dataframe[identifier])
            dataframe['z_score']=z_score
            dataframe= dataframe.drop(index=dataframe[dataframe['z_score'] > 2].index)
            dataframe= dataframe.drop(index=dataframe[dataframe['z_score'] < -2].index)
            dataframe=dataframe.reset_index(drop=True)
            dataframe=dataframe.drop(columns='z_score')
            # print(dataframe)
            X_train=dataframe[x]
            
            # pls_reg=PLSRegression(n_components=2)
            # pls_reg.fit(X_train, dataframe[y])
            if len(x)>5:
                pls_reg=LRPI()
                pls_reg.fit(X_train,dataframe[y])
                pls_t2_list=['plsscore1','plsscore2','plsscore3','plsscore4']
            else:
                pls_reg=LRPI_low_x()
                pls_reg.fit(X_train,dataframe[y])
                pls_t2_list=['plsscore1','plsscore2']
            tempdict={}
            col_list=x
            col_list.append(y)
            for i in col_list:
                if i in processed_daily_data :
                    tempdict[i]=[processed_daily_data[i]['processed']]
                else:
                    tempdict[i]=None
            col_list.remove(y)
            tempdataframe=pd.DataFrame(tempdict)
            for col in tempdataframe.columns:
                if tempdataframe[col].iloc[0]==None or pd.isnull(tempdataframe[col].iloc[0])==True or type(tempdataframe[col].iloc[0])!=int or type(tempdataframe[col].iloc[0])!=float:
                    tempdataframe[col].iloc[0]=np.mean(dataframe[col])
            # print(tempdataframe)
            pred=pls_reg.predict(tempdataframe[col_list])
            # print(pred)
            pred_list=[]
            pred_list.append(pred['lower'].iloc[0])
            pred_list.append(pred['Pred'].iloc[0])
            pred_list.append(pred['upper'].iloc[0])
            
            spe=(pred['Pred'].iloc[0]-tempdataframe[y].iloc[0])**2
            
            dataframe=dataframe.append(tempdataframe)
            dataframe=dataframe.reset_index(drop=True)
            
            pls_reg_2=PLSRegression(n_components=len(x))
            pls_reg_2.fit(dataframe[x],dataframe[y])
            pls_col=[]

            for i in range(1,len(x)+1):
                pls_col.append("plsscore"+str(i))
            pls_dataframe = pd.DataFrame(data = pls_reg_2.x_scores_, columns = pls_col)
            pls_dataframe[y]=dataframe[y]
            dis_listnew=[]
            


            # new spelimit arrays code
            if len(x)>4:
                param_alpha=[]
                param_alpha.append(self.ship_configs['parameter_anamoly']['SPE_alpha1']['alpha'])
                param_alpha.append(self.ship_configs['parameter_anamoly']['SPE_alpha2']['alpha'])
                param_alpha.append(self.ship_configs['parameter_anamoly']['SPE_alpha3']['alpha'])
                
                var_list=[]
                for i in range(4,len(pls_col)):
                    var_list.append(np.var(pls_dataframe[pls_col[i]]))
                cube_list=[]
                for i in var_list:
                    cube_list.append(i**3)
                theta_1=round(sum(var_list),3)
                theta_2 = round(functools.reduce(lambda i, j: i + j * j, [var_list[:1][0]**2]+var_list[1:]),3)
                theta_3=round(sum(cube_list),3)
                
                upper=2*theta_1*theta_3
                theta_2_square=theta_2**2
                lower=3*theta_2_square
                
                if lower!=0:
                    h_zero=round(1-(upper/lower),3)
                    if h_zero!=0:
                        power=1/h_zero
                        two_theta_2_h_zero_square=2*theta_2*(h_zero**2)
                        theta_2_h_zero_minus1=theta_2*h_zero*(h_zero-1)
                        spe_limit_array=[]
                        for i in param_alpha:
                            c_alpha=st.norm.ppf(1-i)
                            first_formula=c_alpha*(np.sqrt(two_theta_2_h_zero_square))/theta_1
                            second_formula=theta_2_h_zero_minus1/(theta_1**2)
                            formula=first_formula+1+second_formula
                            spe_limit_array.append(round(theta_1*(formula**power),3))
                    else:
                        spe_limit_array=[]
                else:
                    spe_limit_array=[]
                # print(spe_limit_array)
            else:
                spe_limit_array=None
            
            # end of limit array code

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
            
            m=len(pls_dataframe)
            p=2
            cl=((m-1)**2)/m
            lcl=cl*(st.beta.ppf(0.01, p/2, (m - p - 1) / 2))
            center=cl*(st.beta.ppf(0.5,p/2,(m-p-1)/2))
            ucl=cl*(st.beta.ppf(0.75,p/2,(m-p-1)/2))
            t_2limit=ucl-center
            crit_data=t_2limit
            
            pls_dataframe=pls_dataframe.drop(index=pls_dataframe[pls_dataframe['t_2'] > t_2limit].index)
            if len(pls_dataframe)>=2:
                if dataframe.index[-1]==pls_dataframe.index[-1]:
                    pls_dataframe=pls_dataframe.reset_index(drop=True)
                    del pls_dataframe['t_2']
                    dis_listnew=[]
                    
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
                    t2_final=pls_dataframe['t_2'].iloc[-1].round(3)
                    m=len(pls_dataframe)
                    p=2
                    if m>p:
                        dfn=m-p-1
                        dfd=p*(m-2)
                        cl=(p*(m-1))/(m-p)
                        f_val_left=st.f.ppf(0.10,dfn,dfd)*cl
                        f_val_right=st.f.ppf(1-0.10,dfn,dfd)*cl
                        f_val_two=st.f.ppf((1-0.10)/2,dfn,dfd)*cl
                        crit_val_dynamic=f_val_right
                    
                    else:
                        crit_val_dynamic=None
                    # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                    return pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array
                    
                else:
                    new_dataframe=pd.DataFrame(columns=dataframe.columns)
                    for i in pls_dataframe.index:
                        new_dataframe=new_dataframe.append(dataframe.iloc[i])
                    new_dataframe=new_dataframe.append(tempdataframe)
                    new_dataframe=new_dataframe.reset_index(drop=True)
                    pls_reg_new=PLSRegression(n_components=len(x))
                    pls_reg_new.fit(new_dataframe[x],new_dataframe[y])
                    pls_col_list=[]
                    
                    for i in range(1,len(x)+1):
                        pls_col_list.append("plsscore"+str(i))
                    new_pls_dataframe=pd.DataFrame(data=pls_reg_new.x_scores_,columns=pls_col_list)
                    new_pls_dataframe[y]=new_dataframe[y]
                    dis_listnew=[]
                    for i in range(0,len(new_pls_dataframe[pls_t2_list])):
                        data=np.array(new_pls_dataframe[pls_t2_list])
                        X_feat=np.array(new_pls_dataframe[pls_t2_list].iloc[[i]])
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

                    new_pls_dataframe['t_2']=dis_listnew


                    m=len(new_pls_dataframe)
                    p=2
                    if m>p:
                        dfn=m-p-1
                        dfd=p*(m-2)
                        cl=(p*(m-1))/(m-p)
                        f_val_left=st.f.ppf(0.10,dfn,dfd)*cl
                        f_val_right=st.f.ppf(1-0.10,dfn,dfd)*cl
                        f_val_two=st.f.ppf((1-0.10)/2,dfn,dfd)*cl
                        crit_val_dynamic=f_val_right
                    else:
                        crit_val_dynamic=None
                    t2_final=new_pls_dataframe['t_2'].iloc[-1].round(3)
                # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                return pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array
            else:
                crit_val_dynamic=None
                t2_final=None

                # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                return pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array
        else:
            pred_list=None
            spe=None
            crit_data=None
            crit_val_dynamic=None
            t2_initial=None
            t2_final=None
            spe_limit_array=None
            # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
            return pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array

    def base_prediction_processor(self,identifier,main_data_dict,dependent_variables,processed_daily_data):
        main_data_dict=main_data_dict
        pred={}
        spe={}
        crit_data={}
        crit_val_dynamic={}
        t2_initial={}
        t2_final={}
        spe_limit_array={}
        m3_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,processed_daily_data,3,0)
        m3_pred,m3_spe,m3_crit_data,m3_crit_val_dynamic,m3_t2_initial,m3_t2_final,m3_spe_limit=self.prediction(identifier,m3_dataframe,processed_daily_data,dependent_variables)
        pred['m3']=m3_pred
        spe['m3']=m3_spe
        crit_data['m3']=m3_crit_data
        crit_val_dynamic['m3']=m3_crit_val_dynamic
        t2_initial['m3']=m3_t2_initial
        t2_final['m3']=m3_t2_final
        spe_limit_array['m3']=m3_spe_limit
        # print(pred)
        # print(spe_limit_array)
        # exit()
        m6_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,processed_daily_data,6,0)
        m6_pred,m6_spe,m6_crit_data,m6_crit_val_dynamic,m6_t2_initial,m6_t2_final,m6_spe_limit=self.prediction(identifier,m6_dataframe,processed_daily_data,dependent_variables)
        pred['m6']=m6_pred
        spe['m6']=m6_spe
        crit_data['m6']=m6_crit_data
        crit_val_dynamic['m6']=m6_crit_val_dynamic
        t2_initial['m6']=m6_t2_initial
        t2_final['m6']=m6_t2_final
        spe_limit_array['m6']=m6_spe_limit

        m12_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,processed_daily_data,12,0)
        m12_pred,m12_spe,m12_crit_data,m12_crit_val_dynamic,m12_t2_initial,m12_t2_final,m12_spe_limit=self.prediction(identifier,m12_dataframe,processed_daily_data,dependent_variables)
        pred['m12']=m12_pred
        spe['m12']=m12_spe
        crit_data['m12']=m12_crit_data
        crit_val_dynamic['m12']=m12_crit_val_dynamic
        t2_initial['m12']=m12_t2_initial
        t2_final['m12']=m12_t2_final
        spe_limit_array['m12']=m12_spe_limit

        ly_m3_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,processed_daily_data,0,3)
        ly_m3_pred,ly_m3_spe,ly_m3_crit_data,ly_m3_crit_val_dynamic,ly_m3_t2_initial,ly_m3_t2_final,ly_m3_spe_limit=self.prediction(identifier,ly_m3_dataframe,processed_daily_data,dependent_variables)
        pred['ly_m3']=ly_m3_pred
        spe['ly_m3']=ly_m3_spe
        crit_data['ly_m3']=ly_m3_crit_data
        crit_val_dynamic['ly_m3']=ly_m3_crit_val_dynamic
        t2_initial['ly_m3']=ly_m3_t2_initial
        t2_final['ly_m3']=ly_m3_t2_final
        spe_limit_array['ly_m3']=ly_m3_spe_limit

        ly_m6_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,processed_daily_data,0,6)
        ly_m6_pred,ly_m6_spe,ly_m6_crit_data,ly_m6_crit_val_dynamic,ly_m6_t2_initial,ly_m6_t2_final,ly_m6_spe_limit=self.prediction(identifier,ly_m6_dataframe,processed_daily_data,dependent_variables)
        pred['ly_m6']=ly_m6_pred
        spe['ly_m6']=ly_m6_spe
        crit_data['ly_m6']=ly_m6_crit_data
        crit_val_dynamic['ly_m6']=ly_m6_crit_val_dynamic
        t2_initial['ly_m6']=ly_m6_t2_initial
        t2_final['ly_m6']=ly_m6_t2_final
        spe_limit_array['ly_m6']=ly_m6_spe_limit

        ly_m12_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,processed_daily_data,0,12)
        ly_m12_pred,ly_m12_spe,ly_m12_crit_data,ly_m12_crit_val_dynamic,ly_m12_t2_initial,ly_m12_t2_final,ly_m12_spe_limit=self.prediction(identifier,ly_m12_dataframe,processed_daily_data,dependent_variables)
        pred['ly_m12']=ly_m12_pred
        spe['ly_m12']=ly_m12_spe
        crit_data['ly_m12']=ly_m12_crit_data
        crit_val_dynamic['ly_m12']=ly_m12_crit_val_dynamic
        t2_initial['ly_m12']=ly_m12_t2_initial
        t2_final['ly_m12']=ly_m12_t2_final
        spe_limit_array['ly_m12']=ly_m12_spe_limit
        # print(spe_limit_array)
        # print(pred)
        # exit()
        return pred,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array

    def rpm_processor(self,main_data_dict,processed_daily_data):
        return self.base_prediction_processor('main_fuel_per_dst',main_data_dict,['cpress','w_force','rep_dt','draft_mean','speed_ship_sog','rpm','w_rel_0'],processed_daily_data)
    
    # def rpm_processor(self,Y,main_data_dict,X,processed_daily_data):
        
    #     return self.base_prediction_processor(Y,main_data_dict,X,processed_daily_data)




class LRPI:
    def __init__(self, normalize=False, n_jobs=1, t_value = 2.13144955):
        self.normalize = normalize
        self.n_jobs = n_jobs
        self.LR = PLSRegression(n_components=4)
        self.t_value = t_value
        
    def fit(self, X_train, y_train):
        self.X_train = pd.DataFrame(X_train.values)
        self.y_train = pd.DataFrame(y_train.values)
        self.LR.fit(self.X_train, self.y_train)
        X_train_fit = self.LR.predict(self.X_train)
        self.MSE = np.power(self.y_train.subtract(X_train_fit), 2).sum(axis=0) / (self.X_train.shape[0] - self.X_train.shape[1] - 1)
        self.X_train.loc[:, 'const_one'] = 1
        self.XTX_inv = np.linalg.pinv(np.dot(np.transpose(self.X_train.values.astype(float)) , self.X_train.values.astype(float)))
        
    def predict(self, X_test):
        self.X_test = pd.DataFrame(X_test.values)
        self.pred = self.LR.predict(self.X_test)
        self.X_test.loc[: , 'const_one'] =1
        SE = [np.dot(np.transpose(self.X_test.values[i]) , np.dot(self.XTX_inv, self.X_test.values[i]) ) for i in range(len(self.X_test)) ]
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
        results = pd.DataFrame(self.pred , columns=['Pred'])
        results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        return results