import os
from dotenv import load_dotenv
from numpy.core.defchararray import index
from pymongo import MongoClient
from src.processors.dd_processor.indices_procesor import Indice_Processing
from bson import json_util
import pandas as pd
import datetime
import scipy.stats as st
from pymongo import ASCENDING
from sklearn.cross_decomposition import PLSRegression
import functools
import numpy as np

load_dotenv()
# client = MongoClient("mongodb://localhost:27017/aranti")
client = MongoClient(os.getenv('MONGODB_ATLAS'))
db=client.get_database("aranti")
database = db

class Universal_limits():
    def __init__(self,shipconfig):
        self.ship_configs=shipconfig


    def spe_limit(self,dataframe,key):
        if dataframe is not None:
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
            dataframe=dataframe.reset_index(drop=True)
            
            if len(dataframe.columns)>2 and key in dataframe.columns and (dataframe[key] == 0).all()==False:
                x=[]
                y=key
                for col in dataframe.columns:
                    x.append(col)
                x.remove(y)
                

                z_score=st.zscore(dataframe[key])
                dataframe['z_score']=z_score
                dataframe= dataframe.drop(index=dataframe[dataframe['z_score'] > 2].index)
                dataframe= dataframe.drop(index=dataframe[dataframe['z_score'] < -2].index)
                dataframe=dataframe.reset_index(drop=True)
                dataframe=dataframe.drop(columns='z_score')
                # dataframe=dataframe.tail(static_length)
                dataframe=dataframe.reset_index(drop=True)
            
                pls_reg_2=PLSRegression(n_components=len(x))
                pls_reg_2.fit(dataframe[x],dataframe[y])
                pls_col=[]

                for i in range(1,len(x)+1):
                    pls_col.append("plsscore"+str(i))
                pls_dataframe = pd.DataFrame(data = pls_reg_2.x_scores_, columns = pls_col)
                pls_dataframe[y]=dataframe[y]

                t2_alpha=[]
                t2_alpha.append(self.ship_configs['parameter_anamoly']['T2_alpha1']['alpha'])
                t2_alpha.append(self.ship_configs['parameter_anamoly']['T2_alpha2']['alpha'])
                t2_alpha.append(self.ship_configs['parameter_anamoly']['T2_alpha3']['alpha'])
                m=len(dataframe)
                p=4
                # cl=((m-1)**2)/m
                numerator=p*(m -1)
                denominator=m-p
                multiplier=numerator/denominator
                t2_limit={}
                t2_alpha_str={"0.2":"zero_two","0.1":"zero_one","0.05":"zero_zero_five"}
                for i in t2_alpha:
                    val=st.f.ppf(1-i,p,m-p)*multiplier
                    t2_limit[t2_alpha_str[str(i)]]=round(val,2)
                return t2_limit
            else:
                return None
                
                # if len(x)>4:
                #     param_alpha=[]
                #     param_alpha.append(self.ship_configs['parameter_anamoly']['SPE_alpha1']['alpha'])
                #     param_alpha.append(self.ship_configs['parameter_anamoly']['SPE_alpha2']['alpha'])
                #     param_alpha.append(self.ship_configs['parameter_anamoly']['SPE_alpha3']['alpha'])
                    
                #     var_list=[]
                #     for i in range(4,len(pls_col)):
                #         var_list.append(np.var(pls_dataframe[pls_col[i]]))
                #     cube_list=[]
                #     for i in var_list:
                #         cube_list.append(i**3)
                #     theta_1=round(sum(var_list),3)
                #     theta_2 = round(functools.reduce(lambda i, j: i + j * j, [var_list[:1][0]**2]+var_list[1:]),3)
                #     theta_3=round(sum(cube_list),3)
                    
                #     upper=2*theta_1*theta_3
                #     theta_2_square=theta_2**2
                #     lower=3*theta_2_square
                    
                #     if lower!=0:
                #         h_zero=round(1-(upper/lower),3)
                #         if h_zero!=0:
                #             power=1/h_zero
                #             two_theta_2_h_zero_square=2*theta_2*(h_zero**2)
                #             theta_2_h_zero_minus1=theta_2*h_zero*(h_zero-1)
                #             spe_limit_array={}
                #             for i in param_alpha:
                #                 c_alpha=st.norm.ppf(1-i)
                #                 first_formula=c_alpha*(np.sqrt(two_theta_2_h_zero_square))/theta_1
                #                 second_formula=theta_2_h_zero_minus1/(theta_1**2)
                #                 formula=first_formula+1+second_formula
                #                 limit_array_val=round((theta_1*(formula**power)),3)
                #                 spe_limit_array[t2_alpha_str[str(i)]]=limit_array_val
                #             # print(spe_limit_array)
                #         else:
                #             spe_limit_array=None
                #     else:
                #         spe_limit_array=None
                # else:
                #     spe_limit_array=None
            return t2_limit
        else:
            return None