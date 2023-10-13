# not react application backend, part of maindb process this code is for processing indices

from __future__ import division
import os
from dotenv import load_dotenv
load_dotenv()
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
sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
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
# from plotly.subplots import make_subplots
import scipy.stats as st
# import plotly.graph_objects as go
client = MongoClient(os.getenv("MONGODB_ATLAS"))
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
# import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import Lasso
# from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from sklearn.cross_decomposition import PLSRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import Ridge
# from mpl_toolkits import mplot3d
# import seaborn as sns
from sklearn.metrics import r2_score
# import matplotlib.pyplot as mp
# import seaborn as sb
import functools
from sklearn.cross_decomposition import PLSRegression



from scipy.stats import f


class Indice_Processing():


    def __init__(self,configs,md,imo):
        self.ship_configs = configs
        self.main_data= md
        self.ship_imo=imo
        pass

    def time_dataframe_generator(self,identifier,dependent_variables,no_months,last_year_months,list_date,vsl_load):
        for i in identifier:
            dependent_variables.append(i)
        maindb = database.get_collection("Main_db")
        temp_list=[]
        for i in dependent_variables:
            # try:
            self.main =maindb.find({"processed_daily_data.rep_dt.processed": {"$lte":list_date[-1], "$gte": list_date[0]},"ship_imo":self.ship_imo,"within_good_voyage_limit":True,'processed_daily_data.vsl_load_bal.processed':vsl_load},{"processed_daily_data."+i+".processed":1,"_id":0})
            # except:
            #     self.main =maindb.find({"processed_daily_data.rep_dt.processed": {"$lte":list_date[-1], "$gte": list_date[0]},"ship_imo":self.ship_imo,"within_good_voyage_limit":True,'processed_daily_data.vsl_load_bal.processed':vsl_load},{"independent_indices."+i+".processed":1,"_id":0})
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

    def prediction(self,identifier,dataframe,currdate,processed_daily_data,dependent_variables,month,lastyear):
        if dataframe is not None:
            if month is not None:
                new_date=currdate-relativedelta(months=month)
                new_data=dataframe.loc[(dataframe['rep_dt'] >= new_date) & (dataframe['rep_dt'] < currdate)]
                curr_data=dataframe.loc[(dataframe['rep_dt'] == currdate)]
            if lastyear is not None:
                newyeardate=currdate-relativedelta(months=12)
                new_date=newyeardate-relativedelta(months=lastyear)
                new_data=dataframe.loc[(dataframe['rep_dt'] >= new_date) & (dataframe['rep_dt'] < newyeardate)]
                curr_data=dataframe.loc[(dataframe['rep_dt'] == currdate)]
            new_data=new_data.reset_index(drop=True)
            length_dataframe=len(new_data)
            if length_dataframe>25:
                if 'rep_dt' in new_data.columns:
                    new_data=new_data.drop(columns='rep_dt')
                if 'rep_dt' in dependent_variables:
                    dependent_variables.remove('rep_dt')
                for column in new_data:
                    new_data=new_data[new_data[column]!='']
                    new_data=new_data[new_data[column]!=' ']
                    new_data=new_data[new_data[column]!='  ']
                    new_data=new_data[new_data[column]!='r[a-zA-Z]']
                new_data=new_data.reset_index(drop=True)
                
                for col in new_data.columns:
                    for i in range(0,len(new_data)):
                        if i in new_data.index and type(new_data[col][i])==str:
                            
                            new_data=new_data[new_data[col]!=new_data[col][i]]
                        
                        if i in new_data.index and type(new_data[col][i])==datetime:
                            
                            new_data=new_data[new_data[col]!=new_data[col][i]]
                            
                            

                    if pandas.isnull(new_data[col]).all():
                        new_data=new_data.drop(columns=col)

                new_data=new_data.dropna()
                new_data=new_data.reset_index(drop=True)

                if len(new_data)>=5 and len(new_data.columns)>2:
                    x=[]
                    y=[]
                    col_list=list(new_data.columns)
                    for col in col_list:
                        if col in dependent_variables:
                            x.append(col)
                        if col in identifier:
                            y.append(col)

                    for i in identifier:
                        z_score=st.zscore(new_data[i])
                        new_data['z_score']=z_score
                        new_data= new_data.drop(index=new_data[new_data['z_score'] > 2].index)
                        new_data= new_data.drop(index=new_data[new_data['z_score'] < -2].index)
                        new_data=new_data.reset_index(drop=True)
                        new_data=new_data.drop(columns='z_score')
                        new_data=new_data.reset_index(drop=True)

                    data_today=new_data
                    if len(curr_data) == 1:
                        data_today=data_today.append(curr_data[new_data.columns])
                        data_today=data_today.reset_index(drop=True)
                    else:
                        tempdict={}
                        for i in new_data.columns:
                            if i in processed_daily_data :
                                tempdict[i]=[processed_daily_data[i]['processed']]
                            else:
                                tempdict[i]=[None]
                        data_today=data_today.append(pandas.DataFrame(tempdict))
                        data_today=data_today.reset_index(drop=True)
                    try:

                        X_train=new_data[x]
                        training_dict={}
                        testing_dict={}
                        training_dataframe=pd.DataFrame(training_dict)
                        testing_dataframe=pd.DataFrame(testing_dict)
                        sc=StandardScaler()
                        X_train=sc.fit_transform(new_data[x])
                        X_test=sc.fit_transform(data_today[x])
                        # Y_train=sc.fit_transform(dataframe["pwr"])
                        # print(Y_train)
                        pred_list_upper=[]
                        spe_list_upper=[]
                        spe_dataframe=pd.DataFrame()
                        
                        for id in identifier:
                            std_y=np.std(new_data[id])
                            mean_y=np.mean(new_data[id])
                            y_list=[]
                            test_y_list=[]
                            for i in new_data[id]:
                                val=(i-mean_y)/std_y
                                y_list.append(val)
                            for i in data_today[id]:
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
                                pred_list.append(val)
                            pred_list_upper.append(pred_list[1])
                            spe=(pred['Pred'].iloc[-1]-Y_test[id].iloc[-1])**2
                            spe_list_upper.append(spe)
                        
                        spe_value=sum(spe_list_upper)
                        
                        spe_sum=spe_dataframe.sum(axis=1)
                        # print(spe_dataframe)
                        spe_matrix=spe_dataframe.values
                        # print(spe_matrix)
                        mewma_obj=mewma(lambd=0.2)
                        mewma_val,mewma_ucl=mewma_obj.plot(spe_matrix)
                        
                    
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
                        t2_initial=pls_dataframe['t_2'].iloc[-1]

                        
                        m=length_dataframe
                        p=4
                        cl=((m-1)**2)/m
                        numerator=p*(m -1)
                        denominator=m-p
                        multiplier=numerator/denominator
                        return spe_value,t2_initial,length_dataframe,mewma_val,mewma_ucl
                        
                    except:
                        spe_value=None
                        t2_initial=None
                        mewma_val=None
                        mewma_ucl=None
                        # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                        return spe_value,t2_initial,length_dataframe,mewma_val,mewma_ucl

                else:
                    spe_value=None
                    t2_initial=None
                    mewma_val=None
                    mewma_ucl=None
                    # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                    return spe_value,t2_initial,length_dataframe,mewma_val,mewma_ucl
            else:
                spe_value=None
                t2_initial=None
                mewma_val=None
                mewma_ucl=None
                # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
                return spe_value,t2_initial,length_dataframe,mewma_val,mewma_ucl
        else:
            spe_value=None
            t2_initial=None
            mewma_val=None
            mewma_ucl=None
            length_dataframe=None
            # print(pred_list,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array)
            return spe_value,t2_initial,length_dataframe,mewma_val,mewma_ucl
            


    def base_prediction_processor(self,identifier,dependent_variables,dataframe,curr_date,processed_daily_data):
        spe={}
        spe_chi_square={}
        crit_data={}
        crit_val_dynamic={}
        t2_initial={}
        t2_final={}
        length_dataframe={}
        spe_anamoly={}
        t2_anamoly={}
        mewma_val={}
        mewma_ucl={}
        m3_spe,t2_initial_m3,ld_m3,mewma_val_m3,mewma_ucl_m3=self.prediction(identifier,dataframe,curr_date,processed_daily_data,dependent_variables,3,None)
        spe['m3']=m3_spe
        # spe_chi_square['m3']=m3_spe_chisquare
        # crit_data['m3']=crit_data_m3
        # crit_val_dynamic['m3']=crit_val_dynamic_m3
        t2_initial['m3']=t2_initial_m3
        # t2_final['m3']=t2_final_m3
        length_dataframe['m3']=ld_m3
        # spe_anamoly['m3']=spe_anamoly_m3
        # t2_anamoly['m3']=t2_anamoly_m3
        mewma_val['m3']=mewma_val_m3
        mewma_ucl['m3']=mewma_ucl_m3

        m6_spe,t2_initial_m6,ld_m6,mewma_val_m6,mewma_ucl_m6=self.prediction(identifier,dataframe,curr_date,processed_daily_data,dependent_variables,6,None)
        spe['m6']=m6_spe
        # spe_chi_square['m6']=m6_spe_chisquare
        # crit_data['m6']=crit_data_m6
        # crit_val_dynamic['m6']=crit_val_dynamic_m6
        t2_initial['m6']=t2_initial_m6
        # t2_final['m6']=t2_final_m6
        length_dataframe['m6']=ld_m6
        # spe_anamoly['m6']=spe_anamoly_m6
        # t2_anamoly['m6']=t2_anamoly_m6
        mewma_val['m6']=mewma_val_m6
        mewma_ucl['m6']=mewma_ucl_m6

        m12_spe,t2_initial_m12,ld_m12,mewma_val_m12,mewma_ucl_m12=self.prediction(identifier,dataframe,curr_date,processed_daily_data,dependent_variables,12,None)
        spe['m12']=m12_spe
        # spe_chi_square['m12']=m12_spe_chisquare
        # crit_data['m12']=crit_data_m12
        # crit_val_dynamic['m12']=crit_val_dynamic_m12
        t2_initial['m12']=t2_initial_m12
        # t2_final['m12']=t2_final_m12
        length_dataframe['m12']=ld_m12
        # spe_anamoly['m12']=spe_anamoly_m12
        # t2_anamoly['m12']=t2_anamoly_m12
        mewma_val['m12']=mewma_val_m12
        mewma_ucl['m12']=mewma_ucl_m12
        
        ly_m3_spe,t2_initial_ly_m3,ld_ly_m3,mewma_val_ly_m3,mewma_ucl_ly_m3=self.prediction(identifier,dataframe,curr_date,processed_daily_data,dependent_variables,None,3)
        spe['ly_m3']=ly_m3_spe
        # spe_chi_square['ly_m3']=ly_m3_spe_chisquare
        # crit_data['ly_m3']=crit_data_ly_m3
        # crit_val_dynamic['ly_m3']=crit_val_dynamic_ly_m3
        t2_initial['ly_m3']=t2_initial_ly_m3
        # t2_final['ly_m3']=t2_final_ly_m3
        length_dataframe['ly_m3']=ld_ly_m3
        # spe_anamoly['ly_m3']=spe_anamoly_ly_m3
        # t2_anamoly['ly_m3']=t2_anamoly_ly_m3
        mewma_val['ly_m3']=mewma_val_ly_m3
        mewma_ucl['ly_m3']=mewma_ucl_ly_m3

        ly_m6_spe,t2_initial_ly_m6,ld_ly_m6,mewma_val_ly_m6,mewma_ucl_ly_m6=self.prediction(identifier,dataframe,curr_date,processed_daily_data,dependent_variables,None,6)
        spe['ly_m6']=ly_m6_spe
        # spe_chi_square['ly_m6']=ly_m6_spe_chisquare
        # crit_data['ly_m6']=crit_data_ly_m6
        # crit_val_dynamic['ly_m6']=crit_val_dynamic_ly_m6
        t2_initial['ly_m6']=t2_initial_ly_m6
        # t2_final['ly_m6']=t2_final_ly_m6
        length_dataframe['ly_m6']=ld_ly_m6
        # spe_anamoly['ly_m6']=spe_anamoly_ly_m6
        # t2_anamoly['ly_m6']=t2_anamoly_ly_m6
        mewma_val['ly_m6']=mewma_val_ly_m6
        mewma_ucl['ly_m6']=mewma_ucl_ly_m6

        ly_m12_spe,t2_initial_ly_m12,ld_ly_m12,mewma_val_ly_m12,mewma_ucl_ly_m12=self.prediction(identifier,dataframe,curr_date,processed_daily_data,dependent_variables,None,12)
        spe['ly_m12']=ly_m12_spe
        # spe_chi_square['ly_m12']=ly_m12_spe_chisquare
        # crit_data['ly_m12']=crit_data_ly_m12
        # crit_val_dynamic['ly_m12']=crit_val_dynamic_ly_m12
        t2_initial['ly_m12']=t2_initial_ly_m12
        # t2_final['ly_m12']=t2_final_ly_m12
        length_dataframe['ly_m12']=ld_ly_m12
        # spe_anamoly['ly_m12']=spe_anamoly_ly_m12
        # t2_anamoly['ly_m12']=t2_anamoly_ly_m12
        mewma_val['ly_m12']=mewma_val_ly_m12
        mewma_ucl['ly_m12']=mewma_ucl_ly_m12
        
        # print(spe_limit_array)
        # print(pred)
        # exit()
        
        return spe,t2_initial,length_dataframe,mewma_val,mewma_ucl

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
            # print("greaaaaaaaaaaaaat")
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
            # print("greaaaaaaaaaaaaat")
            SE=[0.3]*len(self.X_test)
        results = pd.DataFrame(self.pred , columns=['Pred'])
        results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        return results


class mewma():

    _title = "MEWMA Chart"

    def __init__(self, lambd=0.2):
        super(mewma, self).__init__()
        self.lambd = lambd

    def plot(self, data):
        nrow, ncol = data.shape
        mean = data.mean(axis=0)
        # print(ncol)
        v = np.zeros(shape=(nrow - 1, ncol))
        
        for i in range(nrow - 1):
            v[i] = data[i + 1] - data[i]

        vv = v.T @ v
        
        s = np.zeros(shape=(ncol, ncol))
        for i in range(ncol):
            s[i] = (1 / (2 * (nrow - 1))) * (vv[i])

        mx = data - mean
        
        z = np.zeros(shape=(nrow + 1, ncol))
        for i in range(nrow):
            z[i + 1] = self.lambd * mx[i] + (1 - self.lambd) * z[i]
        z = z[1:, :]
    
        t2 = [] # values
        for i in range(nrow):
            w = (self.lambd / (2 - self.lambd)) * (1 - (1 - self.lambd)**(2 * (i + 1)))
            inv = np.linalg.inv(w * s)
            t2.append((z[i].T @ inv) @ z[i])
        
        # ucl = ((self.lambd * 10) - 1)*(ncol - 1)
        ucl = np.zeros(len(data))
        
        # lcl = np.zeros(len(self.X))
        I   = np.arange(1,len(data)+1)
       
        sigma=np.std(t2)
        
        L=st.norm.ppf(1-0.2)
        for i in range(len(data)):
            # print(i)
            ucl[i] = np.mean(t2) + L*sigma*np.sqrt((self.lambd / (2 - self.lambd))*(1-(1-self.lambd)**(I[i])))
        
        mewma_val=t2[-1]
        ucl_val=ucl[-1]
        
        return mewma_val,ucl_val 