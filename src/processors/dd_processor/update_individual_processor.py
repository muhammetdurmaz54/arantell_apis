from __future__ import division
from numpy import outer, testing

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
from src.db.schema.ship import Ship 
# from src.processors.dd_processor.regress import regress
#from src.processors.dd_processor.regress import regress
import numpy as np
import pandas as pd
import math
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from dotenv import load_dotenv
load_dotenv()
import datetime
from datetime import date, timedelta
#from pysimplelog import Logger
import string
from dateutil.relativedelta import relativedelta
# from plotly.subplots import make_subplots
import scipy.stats as st
# import plotly.graph_objects as go
# client = MongoClient("mongodb://localhost:27017/aranti")
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
import matplotlib.pyplot as plt
# import seaborn as sb
import functools
from sklearn.cross_decomposition import PLSRegression
from pymongo import ASCENDING,DESCENDING

# from pyearth import Earth



from scipy.stats import f


class UpdateIndividualProcessors():


    def __init__(self,configs,imo):
        self.ship_configs = configs
        # self.main_data= md
        self.ship_imo=imo
        

    def time_dataframe_generator(self,identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date,vsl_load,processed_daily_data):
        dependent_variables.append(identifier)
        maindb = database.get_collection("Main_db")
        temp_list=[]
        # variable_list=['ext_temp1','ext_temp2','ext_temp3','ext_temp4','ext_temp5','ext_temp6','ext_temp7','ext_temp8','ext_temp9','ext_temp10','ext_temp11','ext_temp12','ext_tempavg','tc1_extin_temp','tc1_extout_temp']
        for i in dependent_variables:
            self.main =maindb.find({"processed_daily_data.rep_dt.processed": {"$lte":list_date[-1], "$gte": list_date[0]},"ship_imo":self.ship_imo,"within_good_voyage_limit":True,'processed_daily_data.vsl_load_bal.processed':vsl_load},{"processed_daily_data."+i+".processed":1,"_id":0})
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
                except:
                    try:
                        temp_list_2.append(temp_list[i][j]['independent_indices'][dependent_variables[i]]['processed'])
                    except:
                        temp_list_2.append(None)
            
            temp_dict[dependent_variables[i]]=temp_list_2    
        dataframe=pd.DataFrame(temp_dict)
        dependent_variables.remove(identifier)
        return dataframe

    def dataframe_generator(self,identifier,main_data_dict,dependent_variables,processed_daily_data,no_months,last_year_months):
        if type(main_data_dict['processed'])!=str:
            current_date=processed_daily_data['rep_dt']['processed'].date()
            vsl_load=processed_daily_data['vsl_load_bal']['processed']
        # if no_months!=0:
        #     old_month=current_date-relativedelta(months=no_months)
        #     list_date=pd.date_range(old_month,current_date,freq='d')
        #     dataframe=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date,vsl_load,processed_daily_data)
        # elif last_year_months!=0:
        #     last_year_current_month=current_date-relativedelta(months=12)
        #     last_year_prev_months=last_year_current_month-relativedelta(months=last_year_months)
        #     list_date=pd.date_range(last_year_prev_months,last_year_current_month,freq='d')
        #     dataframe=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date,vsl_load,processed_daily_data)
        # return dataframe
            # print(current_date)
            # current_date=datetime.datetime(2016, 7, 16)#to be changed later only temporary use
            if no_months!=0:
                old_month=current_date-relativedelta(months=no_months)
                list_date=pd.date_range(old_month,current_date,freq='d')
                dataframe=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date,vsl_load,processed_daily_data)
                if len(dataframe)<=25:
                    old_month=current_date-relativedelta(months=12)
                    list_date=pd.date_range(old_month,current_date,freq='d')
                    temp_data=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date,vsl_load,processed_daily_data)
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
                dataframe=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date,vsl_load,processed_daily_data)
                if len(dataframe)<=25:
                    last_year_current_month=current_date-relativedelta(months=12)
                    last_year_prev_months=last_year_current_month-relativedelta(months=12)
                    list_date=pd.date_range(last_year_prev_months,last_year_current_month,freq='d')
                    temp_data=self.time_dataframe_generator(identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date,vsl_load,processed_daily_data)
                    try:
                        temp_dataframe=temp_data.head(30)
                        return temp_dataframe
                    except:
                        return dataframe
                else:
                    return dataframe
       


    def z_score_data(self,new_data,identifier):
        mean_val=np.mean(new_data[identifier])
        standdev=np.std(new_data[identifier])
        zsc_list=[]
        try:
            z_score=st.zscore(new_data[identifier])
            new_data['z_score']=z_score
        except:
            for rowval in new_data[identifier]:
                zsc=(rowval-mean_val)/standdev
                zsc_list.append(zsc)
            new_data['z_score']=zsc_list
        new_data= new_data.drop(index=new_data[new_data['z_score'] > 2].index)
        new_data= new_data.drop(index=new_data[new_data['z_score'] < -2].index)
        new_data=new_data.reset_index(drop=True)
        new_data=new_data.drop(columns='z_score')
        new_data=new_data.reset_index(drop=True)
        return new_data

    def checkspe_limit(self,spe_data):
        mean_val=np.mean(spe_data['spe'])
        var_sped=np.var(spe_data['spe'])
        spe_h_val=2*(mean_val**2)/(var_sped)
        spe_g_val=(var_sped)/(2*(mean_val))
        chi_val=st.chi2.ppf(1-0.05, spe_h_val)
        spe_limit_g_val=spe_g_val*chi_val
        spe_data.drop(spe_data.tail(1).index,inplace = True)
        new_spe_data= spe_data.drop(index=spe_data[spe_data['spe'] > spe_limit_g_val].index)
        return new_spe_data.index

    def prediction(self,dataframe,currdate,month,lastyear,identifier,main_data_dict):
        if month is not None:
            new_date=currdate-relativedelta(months=month)
            new_data=dataframe.loc[(dataframe['rep_dt'] >= new_date) & (dataframe['rep_dt'] < currdate)]
            # print(new_data)
            # print(currdate)
            length_dataframe=len(new_data)
            # print(length_dataframe)
            curr_data=dataframe.loc[(dataframe['rep_dt'] == currdate)]
            # print(curr_data)
            if length_dataframe!=50 and length_dataframe<50:
                try:
                    index_number=curr_data.index
                    print("inside index part",index_number)
                    if index_number>50:
                        new_data=dataframe.loc[(dataframe.index<index_number[0])&(dataframe.index>=index_number[0]-51)]
                        # print("newwww_dataaggin",new_data)
                    else:
                        new_data=new_data
                except:
                    new_data=new_data
                
            # curr_data=dataframe.loc[(dataframe['rep_dt'] == currdate)]
        if lastyear is not None:
            newyeardate=currdate-relativedelta(months=12)
            new_date=newyeardate-relativedelta(months=lastyear)
            new_data=dataframe.loc[(dataframe['rep_dt'] >= new_date) & (dataframe['rep_dt'] < newyeardate)]
            # print(new_data)
            # print(currdate)
            length_dataframe=len(new_data)
            # print(length_dataframe)
            
            if length_dataframe!=50 and length_dataframe<50:
                try:
                    previousyear_curr_data=dataframe.loc[(dataframe['rep_dt'] == newyeardate)]
                    index_number=previousyear_curr_data.index
                    print(index_number)
                    if index_number>50:
                        new_data=dataframe.loc[(dataframe.index<index_number[0])&(dataframe.index>=index_number[0]-51)]
                    else:
                        new_data=new_data
                except:
                    new_data=new_data

            curr_data=dataframe.loc[(dataframe['rep_dt'] == currdate)]
        new_data=new_data.reset_index(drop=True)
        length_dataframe=len(new_data)
        print(length_dataframe)

        if length_dataframe>=50:
            if 'rep_dt' in new_data.columns:
                new_data=new_data.drop(columns='rep_dt')
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
            # outer_new_data=new_data.copy(deep=True)
            # print(new_data)
            # print(curr_data)
            pred_list,spe,t2_initial,ewma,cumsum_val,new_spe_data,clean_new_data=self.prediction_data(new_data,curr_data,identifier,main_data_dict,"first")
            # print(clean_new_data)
            # print(pred_list)
            if new_spe_data is not None and clean_new_data is not None:
                if len(new_spe_data)==len(clean_new_data):
                    # print(clean_new_data)
                    return pred_list,spe,t2_initial,length_dataframe,ewma,cumsum_val
                else:
                    # print(new_spe_data)
                    clean_new_data=clean_new_data.iloc[new_spe_data]
                    clean_new_data=clean_new_data.reset_index(drop=True)
                    pred_list,spe,t2_initial,ewma,cumsum_val,new_spe_data,clean_new_data=self.prediction_data(clean_new_data,curr_data,identifier,main_data_dict,"second")
                    # print(pred_list)
                    return pred_list,spe,t2_initial,length_dataframe,ewma,cumsum_val
            # exit()
            else:
                pred_list=None
                spe=None
                t2_initial=None
                ewma=None
                cumsum_val=None
                return pred_list,spe,t2_initial,length_dataframe,ewma,cumsum_val
        else:
            pred_list=None
            spe=None
            t2_initial=None
            ewma=None
            cumsum_val=None
            return pred_list,spe,t2_initial,length_dataframe,ewma,cumsum_val

    def prediction_data(self,new_data,curr_data,identifier,main_data_dict,first_iter):
            if len(new_data)>=45 and len(new_data.columns)>=2 and identifier in new_data.columns and (new_data[identifier] == 0).all()==False:
                x=[]
                y=identifier
                for col in new_data.columns:
                    x.append(col)
                x.remove(y)
                # print(new_data[identifier])
                # print(new_data)
                if first_iter=="first":
                    new_data=self.z_score_data(new_data,identifier)
                    # print(new_data)
                    new_data=self.z_score_data(new_data,identifier)
                    # print("z_scoreeeeeeeeeee",new_data)
                    # exit()

                # try:
                #     earth=Earth(max_terms=500,max_degree=1)
                #     fitted=earth.fit(new_data[x],new_data[y])
                #     earth_var=True
                # except:
                #     earth_var=False
                # print("Training data:",new_data[x])
                # print("Training_data:",new_data[y])




                data_today_temp=new_data.copy(deep=True)
                data_today=new_data
                # print("data Todayyyyyyy",data_today)
                if len(curr_data) == 1:
                    data_today=data_today.append(curr_data[new_data.columns])
                    data_today=data_today.reset_index(drop=True)
                else:
                    tempdict={}
                    for i in new_data.columns:
                        if i in main_data_dict :
                            tempdict[i]=[main_data_dict[i]['processed']]
                        else:
                            tempdict[i]=[None]
                    data_today=data_today.append(pandas.DataFrame(tempdict))
                    data_today=data_today.reset_index(drop=True)

                
                # print(new_data)
                # print(data_today)

                # new_data=new_data.append(data_today)
                # new_data=new_data.reset_index(drop=True)
                
            
                # print(dataframe)
                try:
                    # try:
                    #     if earth_var==True:
                    #         # print("Testing:",data_today[x])
                    #         # print("testing:",data_today[y])
                    #         earth_pred=fitted.predict(data_today[x])
                    #         # print(earth_pred)
                    #         final_earth_pred=earth_pred[-1]
                    #     else:
                    #         final_earth_pred=None
                        

                    # except:
                    #     final_earth_pred=None

                    training_dict={}
                    testing_dict={}
                    training_dataframe=pd.DataFrame(training_dict)
                    testing_dataframe=pd.DataFrame(testing_dict)
                    # sc=StandardScaler()
                    # X_train=sc.fit_transform(dataframe[x])
                    # X_test=sc.fit_transform(new_data[x])
                    # Y_train=sc.fit_transform(dataframe["pwr"])
                    # print(Y_train)
                    std_y=np.std(new_data[y])
                    mean_y=np.mean(new_data[y])

                    y_list=[]
                    test_y_list=[]
                    X_train=pd.DataFrame({})
                    X_test=pd.DataFrame({})
                    
                    for i in x:
                        train_x_list=[]
                        test_x_list=[]
                        mean_x=np.mean(new_data[i])
                        std_x=np.std(new_data[i])
                        mean_x_test=np.mean(data_today[i])
                        std_x_test=np.std(data_today[i])
                        try:
                            for j in new_data[i]:
                                val=(j-mean_x)/std_x
                                if pd.isnull(val)==False:
                                    train_x_list.append(val)
                                else:
                                    train_x_list.append(j)
                            X_train[i]=train_x_list
                            
                        except:
                            X_train[i]=new_data[i]
                        try:
                            for j in data_today[i]:
                                test_val=(j-mean_x)/std_x
                                if pd.isnull(test_val)==False:
                                    test_x_list.append(test_val)
                                else:
                                    test_x_list.append(j)
                            X_test[i]=test_x_list
                        except:
                            X_test[i]=data_today[i]
                    
                    for i in new_data[y]:
                        y_val=(i-mean_y)/std_y
                        y_list.append(y_val)
                    for i in data_today[y]:
                        test_y_val=(i-mean_y)/std_y
                        test_y_list.append(test_y_val)
                    training_dataframe[y]=y_list
                    Y_train=training_dataframe[y]
                    testing_dataframe[y]=test_y_list
                    Y_test=testing_dataframe

                    # print(X_train)
                    # print(Y_train)
                    # print(X_test)
                    # print(Y_test)

                    if len(x)>5:
                        pls_reg=LRPI()
                        pls_reg.fit(X_train,Y_train)
                        pls_t2_list=['plsscore1','plsscore2','plsscore3','plsscore4']
                    else:
                        pls_reg=LRPI_low_x()
                        pls_reg.fit(X_train,Y_train)
                        pls_t2_list=['plsscore1','plsscore2']
                    
                    pred=pls_reg.predict(X_test)
                    pred_list=[]
                    
                    predcol=["lower","Pred","upper"]
                    for col in predcol:
                        val=(pred[col].iloc[-1]*std_y)+mean_y
                        pred_list.append(val)
                    
                    if pred_list[1]<0:
                        pred_list[1]=0
                        
                    # print(pred_list)
                    pred_temp=[]
                    for i in range(0,len(pred['Pred'])):
                        pred_temp.append((pred['Pred'].iloc[i]*std_y)+mean_y)
                    # print(new_data)
                    # print(Y_test)
                    # print(pred)
                    # print(pred_list)
                    if pred_list[1]<0:
                        spe=(0-Y_test[y].iloc[-1])**2
                    else:
                        spe=(pred['Pred'].iloc[-1]-Y_test[y].iloc[-1])**2
                    # print("speeeee",spe)
                    spe_dataframe=pd.DataFrame({})
                    spe_dataframe['spe']=(pred['Pred']-Y_test[y])**2
                    # print(spe_dataframe)
                    spe_dataframe = spe_dataframe.head(spe_dataframe.shape[0] - 1)
                    spe_dataframe.loc[len(spe_dataframe)]=spe
                    new_spe_data=self.checkspe_limit(spe_dataframe)
                    new_spe_data=new_spe_data.tolist()
                    mewma_alpha=[]
                    mewma_alpha.append(self.ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha1']['alpha'])
                    mewma_alpha.append(self.ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha2']['alpha'])
                    mewma_alpha.append(self.ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha3']['alpha'])
                    ewma=[]
                    std_val=np.std(spe_dataframe['spe'])
                    ewma_obj=EWMA()
                    ewma_obj.fit(spe_dataframe['spe'],0.2,0)
                    for i in mewma_alpha:
                        L=st.norm.ppf(1-i)
                        ewma_val_cal,ewma_ucl_cal,ewma_lcl_cal=ewma_obj.ControlChart(L=L,sigma=std_val)
                        ewma_val=ewma_val_cal[-1]
                        ewma.append(ewma_val)
                    
                    cum_sum=spe_dataframe['spe'].cumsum()
                    cumsum_val=cum_sum.iloc[-1]
                    
                
                    pls_reg_2=PLSRegression(n_components=len(x))
                    pls_reg_2.fit(X_test,Y_test)
                    pls_col=[]
                    # print(pls_reg_2.y_scores_)
                    for i in range(1,len(x)+1):
                        pls_col.append("plsscore"+str(i))
                    pls_dataframe = pd.DataFrame(data = pls_reg_2.x_scores_, columns =pls_col)
                    # pls_dataframe[y]=Y_test[y]
                    dis_listnew=[]
                    pls_spe=pls_dataframe.copy()
                    pls_spe = pls_spe.head(pls_spe.shape[0] - 1)


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
                    return pred_list,spe,t2_initial,ewma,cumsum_val,new_spe_data,data_today_temp

                except:
                    pred_list=None
                    spe=None
                    t2_initial=None
                    ewma=None
                    cumsum_val=None
                    new_spe_data=None
                    data_today_temp=None
                    return pred_list,spe,t2_initial,ewma,cumsum_val,new_spe_data,data_today_temp

            else:
                pred_list=None
                spe=None
                t2_initial=None
                ewma=None
                cumsum_val=None
                new_spe_data=None
                data_today_temp=None
                return pred_list,spe,t2_initial,ewma,cumsum_val,new_spe_data,data_today_temp


    def base_prediction_processor(self,dataframe,currdate,identifier,main_data_dict):
        pred={}
        spe={}
        crit_data={}
        crit_val_dynamic={}
        t2_initial={}
        t2_final={}
        spe_limit_array={}
        spe_anamoly={}
        spe_y_limit_array={}
        t2_anamoly={}
        length_dataframe={}
        ewma={}
        cumsum={}
        ewma_ucl={}


        m3_pred,m3_spe,m3_t2_initial,length_dataframe_m3,ewma_m3,cumsum_m3=self.prediction(dataframe,currdate,3,None,identifier,main_data_dict)
        pred['m3']=m3_pred
        spe['m3']=m3_spe
        t2_initial['m3']=m3_t2_initial
        length_dataframe['m3']=length_dataframe_m3
        ewma['m3']=ewma_m3
        cumsum['m3']=cumsum_m3

        m6_pred,m6_spe,m6_t2_initial,length_dataframe_m6,ewma_m6,cumsum_m6=self.prediction(dataframe,currdate,6,None,identifier,main_data_dict)
        pred['m6']=m6_pred
        spe['m6']=m6_spe
        t2_initial['m6']=m6_t2_initial
        length_dataframe['m6']=length_dataframe_m6
        ewma['m6']=ewma_m6
        cumsum['m6']=cumsum_m6

        m12_pred,m12_spe,m12_t2_initial,length_dataframe_m12,ewma_m12,cumsum_m12=self.prediction(dataframe,currdate,12,None,identifier,main_data_dict)
        pred['m12']=m12_pred
        spe['m12']=m12_spe
        t2_initial['m12']=m12_t2_initial
        length_dataframe['m12']=length_dataframe_m12
        ewma['m12']=ewma_m12
        cumsum['m12']=cumsum_m12

        ly_m3_pred,ly_m3_spe,ly_m3_t2_initial,length_dataframe_ly_m3,ewma_ly_m3,cumsum_ly_m3=self.prediction(dataframe,currdate,None,3,identifier,main_data_dict)
        pred['ly_m3']=ly_m3_pred
        spe['ly_m3']=ly_m3_spe
        t2_initial['ly_m3']=ly_m3_t2_initial
        length_dataframe['ly_m3']=length_dataframe_ly_m3
        ewma['ly_m3']=ewma_ly_m3
        cumsum['ly_m3']=cumsum_ly_m3

        ly_m6_pred,ly_m6_spe,ly_m6_t2_initial,length_dataframe_ly_m6,ewma_ly_m6,cumsum_ly_m6=self.prediction(dataframe,currdate,None,6,identifier,main_data_dict)
        pred['ly_m6']=ly_m6_pred
        spe['ly_m6']=ly_m6_spe
        t2_initial['ly_m6']=ly_m6_t2_initial
        length_dataframe['ly_m6']=length_dataframe_ly_m6
        ewma['ly_m6']=ewma_ly_m6
        cumsum['ly_m6']=cumsum_ly_m6

        ly_m12_pred,ly_m12_spe,ly_m12_t2_initial,length_dataframe_ly_m12,ewma_ly_m12,cumsum_ly_m12=self.prediction(dataframe,currdate,None,12,identifier,main_data_dict)
        pred['ly_m12']=ly_m12_pred
        spe['ly_m12']=ly_m12_spe
        t2_initial['ly_m12']=ly_m12_t2_initial
        length_dataframe['ly_m12']=length_dataframe_ly_m12
        ewma['ly_m12']=ewma_ly_m12
        cumsum['ly_m12']=cumsum_ly_m12
        # print("pred:",pred)
        return pred,spe,t2_initial,length_dataframe,ewma,cumsum




class LRPI:
    def __init__(self, normalize=False, n_jobs=1, t_value = 2.13144955):
        self.normalize = normalize
        self.n_jobs = n_jobs
        self.LR = PLSRegression(n_components=4)
        self.t_value = t_value
        
    def fit(self, X_train, y_train):
        self.X_train = pd.DataFrame(X_train)
        self.y_train = pd.DataFrame(y_train)
        # print(self.X_train)
        # print(self.y_train)
        self.LR.fit(self.X_train, self.y_train)
        X_train_fit = self.LR.predict(self.X_train)
        self.MSE = np.power(self.y_train.subtract(X_train_fit), 2).sum(axis=0) / (self.X_train.shape[0] - self.X_train.shape[1] - 1)
        self.X_train.loc[:, 'const_one'] = 1
        self.XTX_inv = np.linalg.pinv(np.dot(np.transpose(self.X_train.values.astype(float)) , self.X_train.values.astype(float)))
   

    def predict(self, X_test):
        self.X_test = pd.DataFrame(X_test)
        self.pred = self.LR.predict(self.X_test)
        self.X_test.loc[: , 'const_one'] =1
        SE = [np.dot(np.transpose(self.X_test.values[i]) , np.dot(self.XTX_inv, self.X_test.values[i]) ) for i in range(len(self.X_test))]
        quant=np.quantile(SE,0.9)
        if SE[-1]>quant:
            # print("greaaaaaaaaaaaaat")
            SE=[0.3]*len(self.X_test)
        # SE=[0.3]*len(self.X_test)
        results = pd.DataFrame(self.pred , columns=['Pred'])
        results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        # print(results)
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
        quant=np.quantile(SE,0.9)
        if SE[-1]>quant:
            # print("greaaaaaaaaaaaaat")
            SE=[0.3]*len(self.X_test)
       
        results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        return results


class EWMA:
    def fit(self,data,lamda,mean):
        
        self.X     = data
        self.z     = np.zeros(len(data))
        self.lamda = lamda
        self.mean  = mean
        self.z[0]  = self.mean
        for i in range(1,len(self.z)):
            self.z[i] = self.lamda*self.X[i] + (1-self.lamda)*self.z[i-1] 
 
    def  ControlChart(self,L,sigma):
        # L     : Kontrol limitlerinin genişliği
        ucl = np.zeros(len(self.X))
        lcl = np.zeros(len(self.X))
        I   = np.arange(1,len(self.X)+1)
        
        for i in range(len(self.X)):
            ucl[i] = self.mean + L*sigma*np.sqrt((self.lamda / (2 - self.lamda))*(1-(1-self.lamda)**(I[i])))
            lcl[i] = self.mean - L*sigma*np.sqrt((self.lamda / (2 - self.lamda))*(1-(1-self.lamda)**(I[i])))
        return self.z,ucl,lcl
