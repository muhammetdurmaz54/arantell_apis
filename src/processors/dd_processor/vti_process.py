import sys
import os
from dotenv import load_dotenv
import pandas
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from datetime import date, datetime
import numpy as np
from pymongo import MongoClient
from pymongo import ASCENDING
from dateutil.relativedelta import relativedelta
log = CommonLogger(__name__,debug=True).setup_logger()
from bson import json_util
import scipy.stats as st
import time
from pandas.core.indexes.datetimes import date_range
from sklearn.cross_decomposition import PLSRegression
import matplotlib.pyplot as plt

load_dotenv()

MONGODB_URI = os.getenv('MONGODB_ATLAS')
client = MongoClient(MONGODB_URI)

# client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
database = db


class LRPI:
    def __init__(self, normalize=False, n_jobs=1, t_value = 2.13144955):
        self.normalize = normalize
        self.n_jobs = n_jobs
        self.LR = PLSRegression(n_components=4)
        self.t_value = t_value
        
    def fit(self, X_train, y_train):
        self.X_train = pandas.DataFrame(X_train)
        self.y_train = pandas.DataFrame(y_train)
        self.LR.fit(self.X_train, self.y_train)
        X_train_fit = self.LR.predict(self.X_train)
        self.MSE = np.power(self.y_train.subtract(X_train_fit), 2).sum(axis=0) / (self.X_train.shape[0] - self.X_train.shape[1] - 1)
        self.X_train.loc[:, 'const_one'] = 1
        self.XTX_inv = np.linalg.pinv(np.dot(np.transpose(self.X_train.values.astype(float)) , self.X_train.values.astype(float)))

    def predict(self, X_test):
        self.X_test = pandas.DataFrame(X_test)
        # print(self.X_test)
        # self.X_test.to_csv("test_.csv")
        self.pred = self.LR.predict(self.X_test)
        self.X_test.loc[: , 'const_one'] =1
        SE = [np.dot(np.transpose(self.X_test.values[i]) , np.dot(self.XTX_inv, self.X_test.values[i]) ) for i in range(len(self.X_test))]
        quant=np.quantile(SE,0.9)
        if SE[-1]>quant:
            SE=[0.3]*len(self.X_test)
        results = pandas.DataFrame(self.pred , columns=['Pred'])
        results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        return results


def create_base_dataframe(ship_imo,complete_list):
    maindb = database.get_collection("Main_db")
    ship_configs_collection=database.get_collection("ship")
    ship_configs=ship_configs_collection.find({"ship_imo": ship_imo})[0]
    main_data_list=[]
    num=0
    temp_dict={}
    for name in complete_list:
        tempList=[]
        for doc in maindb.find({'ship_imo': ship_imo,'vessel_loaded_check':"Loaded"}, {'processed_daily_data.'+name: 1}).sort('final_rep_dt', ASCENDING):
            try:
                tempList.append(doc['processed_daily_data'][name]['processed'])
            except:
                tempList.append(None)
        temp_dict[name]=tempList
    dataframe=pandas.DataFrame(temp_dict)
    dataframe=dataframe.sort_values(by=['rep_dt'])
    dataframe=dataframe.reset_index(drop=True)
    print(dataframe)
    return dataframe

def clean_data(new_data):
    for column in new_data:
        if column!="rep_dt":
            new_data=new_data[new_data[column]!='']
            new_data=new_data[new_data[column]!=' ']
            new_data=new_data[new_data[column]!='  ']
            new_data=new_data[new_data[column]!='r[a-zA-Z]']
    new_data=new_data.reset_index(drop=True)
    
    for col in new_data.columns:
        if col!="rep_dt":
            for i in range(0,len(new_data)):
                if i in new_data.index and type(new_data[col][i])==str:
                    
                    new_data=new_data[new_data[col]!=new_data[col][i]]
                
                if i in new_data.index and type(new_data[col][i])==datetime:
                    
                    new_data=new_data[new_data[col]!=new_data[col][i]]
                

            if pandas.isnull(new_data[col]).all():
                new_data=new_data.drop(columns=col)
        
    new_data=new_data.dropna()
    new_data=new_data.reset_index(drop=True)
    return new_data

def z_score_data(new_data,identifier):
    mean_val=np.mean(new_data[identifier])
    standdev=np.std(new_data[identifier])
    zsc_list=[]
    try:
        for ident in identifier:
            z_score=st.zscore(new_data[ident])
            new_data['z_score_'+ident]=z_score
    except:
        for ident in identifier:
            for rowval in new_data[ident]:
                zsc=(rowval-mean_val)/standdev
                zsc_list.append(zsc)
            new_data['z_score_'+ident]=zsc_list
    for ident in identifier:
        new_data= new_data.drop(index=new_data[new_data['z_score_'+ident] > 2].index)
        new_data= new_data.drop(index=new_data[new_data['z_score_'+ident] < -2].index)
        new_data=new_data.reset_index(drop=True)
        new_data=new_data.drop(columns='z_score_'+ident)
        new_data=new_data.reset_index(drop=True)
    return new_data

def t2_check(dataframe,x,y):
    pls_t2_list=['plsscore1','plsscore2','plsscore3','plsscore4']
    pls_reg_2=PLSRegression(n_components=len(x))
    pls_reg_2.fit(dataframe[x],dataframe[y])
    pls_col=[]
    # print(pls_reg_2.y_scores_)
    for i in range(1,len(x)+1):
        pls_col.append("plsscore"+str(i))
    pls_dataframe = pandas.DataFrame(data = pls_reg_2.x_scores_, columns =pls_col)
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
    pls_dataframe=pls_dataframe[pls_dataframe['t_2']<=2]
    dataframe2=dataframe[dataframe.index.isin(pls_dataframe.index)]
    dataframe2=dataframe2.reset_index(drop=True)
    return dataframe2

def checkspe_limit(spe_data):
    mean_val=np.mean(spe_data['spe'])
    var_sped=np.var(spe_data['spe'])
    spe_h_val=2*(mean_val**2)/(var_sped)
    spe_g_val=(var_sped)/(2*(mean_val))
    chi_val=st.chi2.ppf(1-0.05, spe_h_val)
    spe_limit_g_val=spe_g_val*chi_val
    spe_data.drop(spe_data.tail(1).index,inplace = True)
    new_spe_data= spe_data.drop(index=spe_data[spe_data['spe'] > spe_limit_g_val].index)
    return new_spe_data.index

def avg_columns(data,eval_data):
    avg_w_rel_0=data["w_rel_0"].mean()
    avg_w_rel_90=data["w_rel_90"].mean()
    avg_current_rel_0=data["current_rel_0"].mean()
    avg_current_rel_90=data["current_rel_90"].mean()
    avg_swell_rel_0=data["swell_rel_0"].mean()
    avg_swell_rel_90=data["swell_rel_90"].mean()
    eval_data["current_rel_90"]=avg_current_rel_90
    eval_data["swell_rel_0"]=avg_swell_rel_0
    eval_data["swell_rel_90"]=avg_swell_rel_90
    eval_data["w_rel_0"]=avg_w_rel_0
    eval_data["w_rel_90"]=avg_w_rel_90
    eval_data["current_rel_0"]=avg_current_rel_0
    return data,eval_data


def vti_process(dry_dock_period,eval_data_period,imo):
    Y_list=["pwr"]
    X_list=["rep_dt","draft_mean","trim","speed_stw_calc","displ","w_rel_0","w_rel_90","current_rel_0","current_rel_90","swell_rel_0","swell_rel_90"]
    X_list_norepdt=["draft_mean","trim","speed_stw_calc","displ","w_rel_0","w_rel_90","current_rel_0","current_rel_90","swell_rel_0","swell_rel_90"]
    complete_list=X_list+Y_list

    dry_Dock_period_partition=dry_dock_period.split("-")
    eval_data_period_partition=eval_data_period.split("-")
    dry_Dock_period_start=datetime.strptime(dry_Dock_period_partition[0], '%d/%m/%Y')
    dry_Dock_period_end=datetime.strptime(dry_Dock_period_partition[1], '%d/%m/%Y')
    eval_data_period_start=datetime.strptime(eval_data_period_partition[0], '%d/%m/%Y')
    eval_data_period_end=datetime.strptime(eval_data_period_partition[1], '%d/%m/%Y')
    base_dataframe=create_base_dataframe(imo,complete_list)
    # print(base_dataframe)


    new_month=dry_Dock_period_end+relativedelta(months=9)
    ref_data=base_dataframe.loc[(base_dataframe['rep_dt'] >= dry_Dock_period_end) & (base_dataframe['rep_dt'] < new_month)]
    ref_data=ref_data.reset_index(drop=True)
    eval_data=base_dataframe.loc[(base_dataframe['rep_dt'] >= eval_data_period_end)]
    eval_data=eval_data.reset_index(drop=True)

    ref_data=ref_data[complete_list]
    eval_data=eval_data[complete_list]

    ref_data=clean_data(ref_data)
    eval_data=clean_data(eval_data)
    
    ref_data=z_score_data(ref_data,Y_list)
    ref_data=z_score_data(ref_data,Y_list)
    
    eval_data=z_score_data(eval_data,Y_list)
    eval_data=z_score_data(eval_data,Y_list)

    ref_data=t2_check(ref_data,X_list_norepdt,Y_list)
    eval_data=t2_check(eval_data,X_list_norepdt,Y_list)
    print("reff",ref_data)
    print("evalll",eval_data)
    temp_eval_data=eval_data.copy()

    sample_ref_data,temp_eval_data=avg_columns(ref_data,temp_eval_data)
    print("temp_eval_Data",temp_eval_data)

    pls_reg=LRPI()
    pls_reg.fit(ref_data[X_list_norepdt],ref_data[Y_list])
    eval_pred=pls_reg.predict(eval_data[X_list_norepdt])
    eval_data['Ypredict_actualEnv']=eval_pred['Pred']
    
    # zero_list=["w_rel_0","w_rel_90","current_rel_0","current_rel_90","swell_rel_0","swell_rel_90"]
    # for col in zero_list:
    #     temp_eval_data[col].values[:] = 0
    temp_eval_pred=pls_reg.predict(temp_eval_data[X_list_norepdt])
    temp_eval_data['n_ref_k']=temp_eval_pred['Pred']

    eval_data['Ypredict_refEnv']=temp_eval_data['n_ref_k']
    eval_data['Ynorm_correction']=abs(eval_data['Ypredict_actualEnv']-eval_data['Ypredict_refEnv'])
    
    eval_data['y_eval_k']=eval_data['pwr']-eval_data['Ynorm_correction']

    eval_data['n_ref_k']=temp_eval_data['n_ref_k']
    eval_data['vti']=eval_data['y_eval_k']/temp_eval_data['n_ref_k']
    # eval_data.to_csv("vti_process_avg.csv")
    print(eval_data)

# start_time = time.time()
# vti_process("01/01/2015-15/01/2015","16/01/2015-16/09/2015",9205926)
# end_time=time.time()
# print(end_time-start_time)








