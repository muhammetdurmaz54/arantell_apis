from __future__ import division
from numpy import outer, testing

from numpy.core.fromnumeric import reshape, var
from numpy.core.function_base import linspace
from numpy.lib.function_base import diff
from numpy.lib.shape_base import dstack

from datetime import date
import sys
from numpy.core.defchararray import add, index, upper
import pandas
from pandas.core import base
from pandas.core.frame import DataFrame
from pandas.core.indexes.datetimes import date_range
from bson import json_util
from pandas.core.dtypes.missing import isnull 
sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
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
import string
from dateutil.relativedelta import relativedelta
import scipy.stats as st
client = MongoClient(os.getenv("MONGODB_ATLAS"))
db=client.get_database("aranti")
database = db
from sklearn.cross_decomposition import PLSRegression
import functools




from scipy.stats import f


class Sister_Vessel_pred():

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

    def prediction(self,dataframe,identifier,main_data_dict):
        new_data=dataframe
        length_dataframe=len(new_data)
        # print(length_dataframe)

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
            pred_list=self.prediction_data(new_data,identifier,main_data_dict)
            return pred_list
        else:
            pred_list=None
            return pred_list

    def prediction_data(self,new_data,identifier,main_data_dict):
            if len(new_data)>=45 and len(new_data.columns)>=2 and identifier in new_data.columns and (new_data[identifier] == 0).all()==False:
                x=[]
                y=identifier
                for col in new_data.columns:
                    x.append(col)
                x.remove(y)
                new_data=self.z_score_data(new_data,identifier)
                new_data=self.z_score_data(new_data,identifier)

                data_today_temp=new_data.copy(deep=True)
                data_today=new_data
                
                tempdict={}
                for i in new_data.columns:
                    if i in main_data_dict:
                        tempdict[i]=[main_data_dict[i]['processed']]
                    else:
                        tempdict[i]=[None]
                data_today=data_today.append(pandas.DataFrame(tempdict))
                data_today=data_today.reset_index(drop=True)
                # print(data_today)
                try:
                    
                    
                    training_dict={}
                    testing_dict={}
                    training_dataframe=pd.DataFrame(training_dict)
                    testing_dataframe=pd.DataFrame(testing_dict)
                
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
                        # print("neggativeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                        pred_list[1]=0
                    return pred_list[1]
                    
                except:
                    pred_list=None
                    return pred_list

            else:
                pred_list=None
                return pred_list


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
        # print(self.XTX_inv)
        # print("transpose of training:",np.transpose(self.X_train.values.astype(float)))
        # print("dot of transpose training and training values:",np.dot(np.transpose(self.X_train.values.astype(float)) , self.X_train.values.astype(float)))
        # print(self.XTX_inv)

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


