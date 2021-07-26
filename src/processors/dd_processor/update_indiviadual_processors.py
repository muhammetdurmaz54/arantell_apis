from __future__ import division

from numpy.core.fromnumeric import reshape
from numpy.lib.function_base import diff
from sklearn import linear_model

from datetime import date
import sys
import pprint
from numpy.core.defchararray import add, index
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




from scipy.stats import f


class UpdateIndividualProcessors():


    def __init__(self,configs,md,ss):
        self.ship_configs = configs
        self.main_data= md
        self.ship_stats= ss
        pass

    def time_dataframe_generator(self,identifier,main_data_dict,dependent_variables,no_months,last_year_months,list_date):
        dependent_variables.append(identifier)
        maindb = database.get_collection("Main_db")
        temp_list=[]
        for i in dependent_variables:
            self.main =maindb.find({"processed_daily_data.rep_dt.processed": {"$lte":list_date[-1], "$gte": list_date[0]}},{"processed_daily_data."+i+".processed":1,"_id":0})
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

    def dataframe_generator(self,identifier,main_data_dict,dependent_variables,no_months,last_year_months):
        if type(main_data_dict['processed'])!=str:
            current_date=self.main_data['processed_daily_data']['rep_dt']['processed']
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

    def prediction(self,identifier,dataframe,processed_daily_data,dependent_variables):
        if 'rep_dt' in dataframe.columns:
            dataframe=dataframe.drop(columns='rep_dt')
        if 'rep_dt' in dependent_variables:
            dependent_variables.remove('rep_dt')
        for column in dataframe:
            dataframe=dataframe[dataframe[column]!='']
        # for column in dataframe:
        #     mean=dataframe[column].mean()
        #     dataframe=dataframe.replace(np.nan,mean)
        
        # dataframe=dataframe.sort_values(by=['draft_mean'])
        
        # x=dependent_variables
        # y=identifier
        # X=dataframe[x]
        # Y=dataframe[y]
        # #standardization
        # scalar=StandardScaler()
        # scalar=scalar.fit(dataframe)
        # scaled_data=scalar.transform(dataframe)
       
        # #minmaxscaling
        # """min_max=MinMaxScaler(feature_range=(0,1))
        # min_max=min_max.fit(dataframe)
        # scaled_data=min_max.transform(dataframe)"""
        # #normalizing
        # """normalize=Normalizer()     
        # normalize=normalize.fit(dataframe)
        # scaled_data=normalize.transform(dataframe)"""
        

        # """scalar_x=StandardScaler()
        # scalar_x=scalar_x.fit(dataframe[x])
        # scaled_data_x=scalar_x.transform(dataframe[x])
        # scalar_y=StandardScaler()
        # scalar_y=scalar_y.fit(dataframe[y])
        # scalar_y_data=scalar_y.transform(dataframe[y])"""
        # #pca
        # pca=PCA(n_components=3)
        # """pca.fit(scale(dataframe))
        # x_pca=pca.transform(scale(dataframe))"""
        # pca.fit(scaled_data)
        # x_pca=pca.transform(scaled_data)
        # pca1_dict={}
        # pca1_list=[]
        # pca2_dict={}
        # pca2_list=[]
        # pca3_dict={}
        # pca3_list=[]
        # for i in range(0,len(x_pca)):
        #     pca1_list.append(x_pca[i][0])
        #     pca2_list.append(x_pca[i][1])
        #     pca3_list.append(x_pca[i][2])
        # pca1_dict['pc1']=pca1_list
        # pca2_dict['pc2']=pca2_list
        # pca3_dict['pc3']=pca3_list
        # pca1_dataframe=pd.DataFrame(pca1_dict)
        # pca2_dataframe=pd.DataFrame(pca2_dict)
        # pca3_dataframe=pd.DataFrame(pca3_dict)
        # reg_obj=LRPI()
        # reg_obj_new=LRPI()
        # pred_list=[]
        # if dataframe.empty==False:
        #     X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.7, random_state=42)
        #     # reg_obj=LRPI()
        #     reg_obj.fit(X_train,y_train)
        #     pred_x_data={}
        #     for i in dependent_variables:
        #         pred_x_val=[]
        #         pred_x_val.append(processed_daily_data[i]['processed'])
        #         pred_x_data[i]=pred_x_val
        #     pred_x_dataframe=pd.DataFrame(pred_x_data)
        #     """m3_pred=reg_obj.predict(pred_x_dataframe)     
        #     pred_list.append(m3_pred['lower'][0])
        #     pred_list.append(m3_pred['Pred'][0])
        #     pred_list.append(m3_pred['upper'][0])"""
        #     m3_pred=reg_obj.predict(X_test)
        #     dataframe['y_pred']=m3_pred['Pred']
        #     dataframe['y_lower']=m3_pred['lower']
        #     dataframe['y_upper']=m3_pred['upper']
        #     #getting  meansqaured error for each row
        #     len_y=len(dataframe[y])
        #     summation=0
        #     difference_dict={}
        #     difference_list=[]
        #     sqaured_diff_dict={}
        #     sqaured_diff_list=[]
        #     for i in range(0,len_y):
        #         difference=dataframe[y][i]-dataframe['y_pred'][i]
        #         difference_list.append(difference)
        #         sqaured_diff=difference**2
        #         sqaured_diff_list.append(sqaured_diff)
        #         summation=summation+sqaured_diff
        #     difference_dict['obs-pred']=difference_list
        #     sqaured_diff_dict['squared_diference']=sqaured_diff_list
        #     difference_dataframe=pd.DataFrame(difference_dict)
        #     sqaured_diff_dataframe=pd.DataFrame(sqaured_diff_dict)
        #     dataframe['obs-pred']=difference_dataframe['obs-pred']
        #     dataframe['squared_diference']=sqaured_diff_dataframe['squared_diference']
        #     mse=summation/len_y
        #     dataframe['pc1']=pca1_dataframe['pc1']
        #     dataframe['pc2']=pca2_dataframe['pc2']
        #     dataframe['pc3']=pca3_dataframe['pc3']
        #     #dataframe.to_csv('main_fuel_per_dst_pca_prediction.csv')
        # temp_dict={'pc1':[],'pc2':[],'pc3':[]}
        # dataframe_2=pd.DataFrame(temp_dict)
        # dataframe_2['pc1']=dataframe['pc1']
        # dataframe_2['pc2']=dataframe['pc2']
        # dataframe_2['pc3']=dataframe['pc3']
        # dataframe_2[y]=dataframe[y]
        
        # xx=['pc1','pc2','pc3']
        # yy=y
        # XX=dataframe_2[xx]
        # YY=dataframe_2[yy]
        
        # X_train, X_test_, y_train, y_test = train_test_split(XX, YY, test_size=0.7, random_state=42)
        
        # reg_obj_new.fit(X_train,y_train)
        
        # pred_new=reg_obj_new.predict(X_test_)
        # dataframe_2['ordinary_y_pred']=dataframe['y_pred']
        # dataframe_2['y_pred_pca']=pred_new['Pred']
        # dataframe_2['y_lower']=pred_new['lower']
        # dataframe_2['upper']=pred_new['upper']
        # dataframe_2_diff_dict={}
        # dataframe_2_diff_list=[]
        # dataframe_2_squared_diff_dict={}
        # dataframe_2_squared_diff_list=[]
        # summation_1=0
        # for i in range(0,len(dataframe_2[yy])):
        #     dataframe_2_diff=dataframe_2[yy][i]-dataframe_2['y_pred_pca'][i]
        #     dataframe_2_diff_list.append(dataframe_2_diff)
        #     dataframe_2_square_diff=dataframe_2_diff**2
        #     dataframe_2_squared_diff_list.append(dataframe_2_square_diff)
        #     summation_1=summation_1+dataframe_2_square_diff
        # dataframe_2_diff_dict['obs-pred']=dataframe_2_diff_list
        # dataframe_2_squared_diff_dict['squared_diff']=dataframe_2_squared_diff_list
        # new_dataframe_2=pd.DataFrame(dataframe_2_diff_dict)
        # new_dataframe_3=pd.DataFrame(dataframe_2_squared_diff_dict)
        # dataframe_2['ordinary_obs-pred']=dataframe['obs-pred']
        # dataframe_2['ordinary_squared_diff']=dataframe['squared_diference']
        # dataframe_2['obs-pred_pca']=new_dataframe_2['obs-pred']
        # dataframe_2['squared_diff_pca']=new_dataframe_3['squared_diff']
        # mse_1=summation_1/len(dataframe_2[yy])
        # dataframe=dataframe.drop(columns=['y_pred','y_lower','y_upper','obs-pred','squared_diference','pc1','pc2','pc3'])
        
        # dataframe_2.insert(0,'w_force',dataframe['w_force'])
        # dataframe_2.insert(0,'cpress',dataframe['cpress'])
        # dataframe_2.insert(0,'rpm',dataframe['rpm'])
        # dataframe_2.insert(0,'draft_mean',dataframe['draft_mean'])
        # dataframe_2.insert(0,'speed_sog',dataframe['speed_sog'])
        if dataframe.empty==False:
            dataframe=dataframe.reset_index(drop=True)
            x=dependent_variables
            y=identifier
            z_score=st.zscore(dataframe[y])
            dataframe['z_score']=z_score
            dataframe= dataframe.drop(index=dataframe[dataframe['z_score'] > 2].index)
            dataframe= dataframe.drop(index=dataframe[dataframe['z_score'] < -2].index)
            dataframe=dataframe.reset_index(drop=True)
            dataframe=dataframe.drop(columns='z_score')
            
            # dataframe= dataframe.drop(index=dataframe[dataframe['draft_mean'] >10].index)
            # dataframe=dataframe.reset_index(drop=True)
            
            X=dataframe[x]
            Y=dataframe[y]
        
            X_train_, X_test_, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=0)

            sc=StandardScaler()
            X_train=sc.fit_transform(X_train_)
            # X_test=sc.transform(X_test_)
            
            """norm=Normalizer()
            X_train=norm.fit_transform(X_train_)
            X_test=norm.fit_transform(X_test_)"""

            """minmax=MinMaxScaler()
            X_train=minmax.fit_transform(X_train_)
            X_test=minmax.transform(X_test_)"""

            """rs=RobustScaler()
            X_train=rs.fit_transform(X_train_)
            X_test=rs.transform(X_test_)"""
            
            pca=PCA(n_components=2)
            X_train=pca.fit_transform(X_train)
            # X_test=pca.transform(X_test)
            
            poly = PolynomialFeatures(degree = 2)
            X_poly = poly.fit_transform(X_train)
            poly.fit(X_poly, y_train)
            reg=Lasso(alpha=0.1)
            reg_2=LinearRegression()
            reg_2.fit(X_poly,y_train)
            reg.fit(X_poly,y_train)
            pred=reg.predict(poly.fit_transform(X_train))
            pred_lin=reg_2.predict(poly.fit_transform(X_train))
            
            pred_list=[]
            # data_new_fin=pd.DataFrame({'cpress':[],'w_force':[],'draft_mean':[],'rpm':[],'speed_sog':[],'pred':[]})
            # speedsog_arr=np.arange(8,12,0.1)
            # for i in speedsog_arr:
            temp_x_dict={}
            for i in x:
                temp_x_list=[]
                temp_x_list.append(processed_daily_data[i]['processed'])
                temp_x_dict[i]=temp_x_list
            
            data_new_=pd.DataFrame(temp_x_dict)
            data_new=sc.transform(data_new_)
            data_new=pca.transform(data_new)
            # data_new=pd.DataFrame(data_new)
            
            pred=reg.predict(poly.fit_transform(data_new))
            
            # data_new_['pred']=pred[0]
            # data_new_fin=data_new_fin.append(data_new_,ignore_index=True)

            pred_list.append(pred[0])
            return pred_list
        # data_new_fin= data_new_fin.sort_values(by=['speed_sog'])
        # data_new_fin= data_new_fin.reset_index(drop=True)

        # plt.plot(data_new_fin['speed_sog'],data_new_fin['pred'],'-ok',color='blue')
        # plt.xlabel("speed_sog")
        # plt.ylabel("pwr")
        # plt.show()
        """X_test_=X_test_.reset_index(drop=True)
        y_test=y_test.reset_index(drop=True)
        X_test_[y]=y_test
        X_test_['pred']=pred['Pred']
        X_test_['lower']=pred['lower']
        X_test_['upper']=pred['upper']"""
        
        # X_train_[y]=y_train
        # corr=sb.heatmap(X_train_.corr(), cmap="YlGnBu", annot=True)
        # # mp.show()
        # X_train_['pred']=pred
        # X_train_['lin_pred']=pred_lin

        # X_train_['diff']=X_train_['pred']-X_train_[y]
        # X_train_['square']=X_train_['diff']**2
        # rmse=sum(X_train_['square']/len(X_train_['square']))
        # X_train_=X_train_.sort_values(by=['speed_sog'])
        # X_train_=X_train_.reset_index(drop=True)
        # # z_score=st.zscore(X_train_[y])
        # # X_train_['z_score']=z_score
        # # filt=X_train_['z_score'] > 3
        # acc=r2_score(X_train_[y],X_train_['pred'])
        # acc_lin=r2_score(X_train_[y],X_train_['lin_pred'])
        # # X_train_.at[1,'draft_mean']=8.1
        # # X_train_=X_train_.drop(index=X_train_[X_train_['draft_mean']<10].index)
        # # X_train_=X_train_.drop(index=X_train_[X_train_['draft_mean']>10].index)
        
        # dis_list=[]
        # for i in range(0,len(X_train_[x])):
        #     data=np.array(X_train_[x])
        #     X_feat=np.array(X_train_[x].iloc[[i]])
        #     mean=np.mean(data,axis=0)
        #     X_feat_mean=X_feat-mean
        #     data=np.transpose(data)
        #     data=data.astype(np.float32)
        #     cov=np.cov(data,bias=False)
        #     inv_cov=np.linalg.inv(cov)
        #     tem1=np.dot(X_feat_mean,inv_cov)
        #     temp2=np.dot(tem1,np.transpose(X_feat_mean))
        #     m_dis=np.sqrt(temp2[0][0])
        #     dis_list.append(m_dis)

        # X_train_['mahalanobi']=dis_list
        # print(X_train_)
        

        # X_train_= X_train_.drop(index=X_train_[X_train_['z_score'] > 3].index)
        # X_train_=X_train_.drop(index=X_train_[X_train_['draft_mean']>9].index)
        # X_train_.to_csv('all_graphs.csv')
        
        # X_test_=X_test_.reset_index(drop=True)
        # y_test=y_test.reset_index(drop=True)
        """X_test_[y]=y_test
        X_test_['pred']=pred
        X_test_['lin_pred']=pred_lin
        X_test_['diff']=X_test_['pred']-X_test_[y]
        X_test_['square']=X_test_['diff']**2
        X_test_=X_test_.sort_values(by=['speed_sog'])
        X_test_=X_test_.reset_index(drop=True)
        acc=r2_score(X_test_[y],X_test_['pred'])
        acc_lin=r2_score(X_test_[y],X_test_['lin_pred'])
        X_test_.to_csv("all_graphs_test.csv")
        print(X_test_)
        print("ridge or lasso:",acc)
        print("linear:",acc_lin)"""
        # fig,ax=plt.subplots()
        # ax.plot(X_train_['speed_sog'], X_train_['lin_pred'], marker="o",color='black')
        # ax.plot(X_train_['speed_sog'],X_train_['pred'],marker="o",color='orange')
        # #ax.plot(X_test_['cpress'], X_test_[y], marker="o")
        # plt.show()
        # fig2,ax2=plt.subplots()

        # fig = plt.figure()
        # ax = fig.add_subplot(projection='3d')
        # ax.scatter(X_train_['pwr'],X_train_['pred'],X_train_['lin_pred'],c='b')
        # ax.set_xlabel('w_force')
        # ax.set_ylabel('rpm')
        # ax.set_zlabel('pwr')
        # plt.show()
        #variation = (X_train_['w_force']-np.nanmin(X_train_['w_force'])) /(np.nanmax(X_train_['w_force']) - np.nanmin(X_train_['w_force']))
        """p = [n**2 for n in (X_train_['draft_mean'])]
        col = [n**4 for n in (X_train_['w_force'])]
        ax=plt.axes(projection='3d')
        ax.plot_trisurf(X_train_['speed_sog'],X_train_['rpm'],X_train_['pred'],alpha=0.5)
        ax.plot_trisurf(X_train_['speed_sog'],X_train_['rpm'],X_train_['lin_pred'],alpha=0.3)
        ax.scatter(X_train_['speed_sog'],X_train_['rpm'],X_train_['pwr'],c=col,cmap = 'coolwarm',s=p)
        #ax.scatter(X_train_['speed_sog'],X_train_['rpm'],X_train_['pwr'],c='black',cmap = 'coolwarm',s=col)
        # ax.scatter(X_train_['speed_sog'],X_train_['draft_mean'],X_train_['lin_pred'],c='p')
        ax.set_xlabel('speed_sog')
        ax.set_ylabel('rpm')
        ax.set_zlabel('pred')
        plt.show()"""

						
        """p = [n**2 for n in (X_test_['draft_mean'])]
        col = [n**4 for n in (X_test_['w_force'])]
        ax=plt.axes(projection='3d')
        ax.plot_trisurf(X_test_['speed_sog'],X_test_['rpm'],X_test_['pred'],alpha=0.5)
        ax.plot_trisurf(X_test_['speed_sog'],X_test_['rpm'],X_test_['lin_pred'],alpha=0.3)
        ax.scatter(X_test_['speed_sog'],X_test_['rpm'],X_test_['pwr'],c=col,cmap = 'coolwarm',s=p)
        #ax.scatter(X_train_['speed_sog'],X_train_['rpm'],X_train_['pwr'],c='black',cmap = 'coolwarm',s=col)
        # ax.scatter(X_train_['speed_sog'],X_train_['draft_mean'],X_train_['lin_pred'],c='p')
        ax.set_xlabel('speed_sog')
        ax.set_ylabel('rpm')
        ax.set_zlabel('pred')
        plt.show()"""
        
        

        """fig, ax = plt.subplots()
        ax.scatter(X_train_[y], X_train_['pred'])
        ax.plot([X_train_[y].min(), X_train_[y].max()], [X_train_['pred'].min(), X_train_['pred'].max()], 'k--', lw=4)
        ax.set_xlabel('Measured')
        ax.set_ylabel('Predicted')
        plt.show()"""
        
        # make a plot with different y-axis using second axis object
        # ax2.plot(X_test_['cpress'], X_test_['pred'],color="blue",marker="o")
        # ax.set_xlabel("cpress")
        # ax.set_ylabel("pred")
       
        """reg.fit(X_train,y_train)
        pred=reg.predict(X_test)
        X_test=X_test.reset_index(drop=True)
        y_test=y_test.reset_index(drop=True)
        pred.insert(0,y,y_test)
        for i in X_test:
            newcol=X_test[i]
            pred.insert(0,i,newcol)
        
        data_new=pd.DataFrame({'cpress':[1015],'w_force':[3],'rpm':[65],'draft_mean':[8.5],'speed_sog':[10.5]})
        pred_new=reg.predict(data_new)"""
        
        

        
        #dataframe_2.to_csv('pwr_pca_allcol_prediction.csv')
        """fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        ax.scatter(dataframe_2['pc1'], dataframe_2['pc2'], dataframe_2['pc3'],c='b',cmap="Set2_r", s=60)
        xAxisLine = ((min(dataframe_2['pc1']), max(dataframe_2['pc1'])), (0, 0), (0,0))
        ax.plot(xAxisLine[0], xAxisLine[1], xAxisLine[2], 'r')
        yAxisLine = ((0, 0), (min(dataframe_2['pc2']), max(dataframe_2['pc2'])), (0,0))
        ax.plot(yAxisLine[0], yAxisLine[1], yAxisLine[2], 'r')
        zAxisLine = ((0, 0), (0,0), (min(dataframe_2['pc3']), max(dataframe_2['pc3'])))
        ax.plot(zAxisLine[0], zAxisLine[1], zAxisLine[2], 'r')

        ax.set_xlabel("PC1")
        ax.set_ylabel("PC2")
        ax.set_zlabel("PC3")
        ax.set_title("PCA on the given data set")"""


        """fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        ax.scatter(dataframe['pc1'], dataframe['pc2'], dataframe['pc3'],c='b',cmap="Set2_r", s=60)
        xAxisLine = ((min(dataframe['pc1']), max(dataframe['pc1'])), (0, 0), (0,0))
        ax.plot(xAxisLine[0], xAxisLine[1], xAxisLine[2], 'r')
        yAxisLine = ((0, 0), (min(dataframe['pc2']), max(dataframe['pc2'])), (0,0))
        ax.plot(yAxisLine[0], yAxisLine[1], yAxisLine[2], 'r')
        zAxisLine = ((0, 0), (0,0), (min(dataframe['pc3']), max(dataframe['pc3'])))
        ax.plot(zAxisLine[0], zAxisLine[1], zAxisLine[2], 'r')"""
        
        # label the axes
        """ax.set_xlabel("PC1")
        ax.set_ylabel("PC2")
        ax.set_zlabel("PC3")
        ax.set_title("PCA on the given data set")"""
        #plt.show()
        #print(mse_1)
        
        #print(pca.explained_variance_)
        # return pred_list       

    def base_prediction_processor(self,identifier,main_data_dict,dependent_variables,processed_daily_data):
        main_data_dict=main_data_dict
        pred={}
        m3_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,3,0)
        m3_dataframe=m3_dataframe.dropna()
        m3_pred=self.prediction(identifier,m3_dataframe,processed_daily_data,dependent_variables)
        pred['m3']=m3_pred
        m6_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,6,0)
        m6_dataframe=m6_dataframe.dropna()
        m6_pred=self.prediction(identifier,m6_dataframe,processed_daily_data,dependent_variables)
        pred['m6']=m6_pred
        m12_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,12,0)
        m12_dataframe=m12_dataframe.dropna()
        m12_pred=self.prediction(identifier,m12_dataframe,processed_daily_data,dependent_variables)
        pred['m12']=m12_pred
        ly_m3_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,0,3)
        ly_m3_dataframe=ly_m3_dataframe.dropna()
        ly_m3_pred=self.prediction(identifier,ly_m3_dataframe,processed_daily_data,dependent_variables)
        pred['ly_m3']=ly_m3_pred
        ly_m6_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,0,6)
        ly_m6_dataframe=ly_m6_dataframe.dropna()
        ly_m6_pred=self.prediction(identifier,ly_m6_dataframe,processed_daily_data,dependent_variables)
        pred['ly_m6']=ly_m6_pred
        ly_m12_dataframe=self.dataframe_generator(identifier,main_data_dict,dependent_variables,0,12)
        ly_m12_dataframe=ly_m12_dataframe.dropna()
        ly_m12_pred=self.prediction(identifier,ly_m6_dataframe,processed_daily_data,dependent_variables)
        pred['ly_m12']=ly_m12_pred
        
        return pred

    def rpm_processor(self,main_data_dict,processed_daily_data):
        return self.base_prediction_processor('pwr',main_data_dict,['cpress','w_force','draft_mean','rep_dt','rpm','speed_sog'],processed_daily_data)
    
    # def rpm_processor(self,Y,main_data_dict,X,processed_daily_data):
        
    #     return self.base_prediction_processor(Y,main_data_dict,X,processed_daily_data)
        
        
        



class LRPI:
    def __init__(self, normalize=False, n_jobs=1, t_value = 2.13144955):
        self.normalize = normalize
        self.n_jobs = n_jobs
        self.LR = linear_model.LinearRegression(normalize=self.normalize, n_jobs= self.n_jobs)
        self.t_value = t_value
        
    def fit(self, X_train, y_train):
        self.X_train = pd.DataFrame(X_train.values)
        self.y_train = pd.DataFrame(y_train.values)
        self.LR.fit(self.X_train, self.y_train)
        X_train_fit = self.LR.predict(self.X_train)
        self.MSE = np.power(self.y_train.subtract(X_train_fit), 2).sum(axis=0) / (self.X_train.shape[0] - self.X_train.shape[1] - 1)
        self.X_train.loc[:, 'const_one'] = 1
        self.XTX_inv = np.linalg.inv(np.dot(np.transpose(self.X_train.values) , self.X_train.values))
        
    def predict(self, X_test):
        self.X_test = pd.DataFrame(X_test.values)
        self.pred = self.LR.predict(self.X_test)
        self.X_test.loc[: , 'const_one'] =1
        SE = [np.dot(np.transpose(self.X_test.values[i]) , np.dot(self.XTX_inv, self.X_test.values[i]) ) for i in range(len(self.X_test)) ]
        results = pd.DataFrame(self.pred , columns=['Pred'])
        results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        return results