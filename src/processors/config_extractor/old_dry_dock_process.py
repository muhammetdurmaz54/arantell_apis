import sys
import os
from dotenv import load_dotenv
import pandas
sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
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
from src.processors.config_extractor.configurator import Configurator


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


def create_base_dataframe(ship_imo, complete_list):
    maindb = database.get_collection("Main_db")
    ship_configs_collection=database.get_collection("ship")
    # ship_configs=ship_configs_collection.find({"ship_imo": int(ship_imo)})[0]
    main_data_list=[]

    num=0
    
    # for doc in maindb.find({"ship_imo": int(ship_imo)}):
    #     num=num+1
    #     # print(num)
    #     main_data_list.append(doc)
    
    # ident_list=[]
    # for i in ship_configs['data']:
    #     ident_list.append(i)
    # temp_dict={}
    # for j in range(0,len(ident_list)):
    #     temp_list_2=[]
    #     for i in range(len(main_data_list)):
    #         if main_data_list[i]['within_good_voyage_limit']==True:
    #             try:
    #                 temp_list_2.append(main_data_list[i]['processed_daily_data'][ident_list[j]]['processed'])
    #             except:
    #                 try:
    #                     temp_list_2.append(main_data_list[i]['independent_indices'][ident_list[j]]['processed'])
    #                 except:
    #                     temp_list_2.append(None)
                
    #             temp_dict[ident_list[j]]=temp_list_2      
    # dataframe=pandas.DataFrame(temp_dict)
    # dataframe=dataframe.sort_values(by=['rep_dt'])
    # dataframe=dataframe.reset_index(drop=True)
    # return dataframe

    temp_dict={}
    for name in complete_list:
        tempList=[]
        for doc in maindb.find({'ship_imo': int(ship_imo)}, {'processed_daily_data.'+name: 1}).sort('final_rep_dt', ASCENDING):
            try:
                tempList.append(doc['processed_daily_data'][name]['processed'])
            except:
                tempList.append(None)
        temp_dict[name]=tempList
    dataframe=pandas.DataFrame(temp_dict)
    # dataframe=dataframe.sort_values(by=['rep_dt'])
    # dataframe=dataframe.reset_index(drop=True)
    # print(dataframe)
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
    # print("NEW DATA", new_data)
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
    # print("Z SCORE DATA", new_data)
    return new_data

def avg_columns(data,eval_data):
    avg_w_force=data["w_force"].mean()
    avg_w_dir_rel=data["w_dir_rel"].mean()
    avg_swell_dir_rel=data["swell_dir_rel"].mean()
    eval_data["w_force"]=avg_w_force
    eval_data["w_dir_rel"]=avg_w_dir_rel
    eval_data["swell_dir_rel"]=avg_swell_dir_rel
    return data,eval_data

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

def drydock(dry_dock_period,eval_data_period,imo,performance_type):
    # Y_list=["speed_stw_calc","pwr","app_slip","main_fuel_per_dst"]
    # X_list=["rep_dt","draft_mean","trim","rpm","w_dir_rel","swell_dir_rel","amb_temp","cpress","w_force"]
    # X_list_norepdt=["draft_mean","trim","rpm","w_dir_rel","swell_dir_rel","amb_temp","cpress","w_force"]
    # complete_list=X_list+Y_list
    # dry_Dock_period_partition=dry_dock_period.split("-")
    # eval_data_period_partition=eval_data_period.split("-")
    # dry_Dock_period_end=datetime.strptime(dry_Dock_period_partition[1], '%d/%m/%Y')
    # eval_data_period_start=datetime.strptime(eval_data_period_partition[0], '%d/%m/%Y')
    # eval_data_period_end=datetime.strptime(eval_data_period_partition[1], '%d/%m/%Y')
    # print("DATAFRAME")
    # base_dataframe=create_base_dataframe(imo, complete_list)
    # new_month=dry_Dock_period_end+relativedelta(months=6)
    # ref_data=base_dataframe.loc[(base_dataframe['rep_dt'] >= dry_Dock_period_end) & (base_dataframe['rep_dt'] < new_month)]
    # ref_data=ref_data.reset_index(drop=True)
    # eval_data=base_dataframe.loc[(base_dataframe['rep_dt'] >= eval_data_period_start) & (base_dataframe['rep_dt'] < eval_data_period_end)]
    # eval_data=eval_data.reset_index(drop=True)
    # ref_data=ref_data[complete_list]
    # eval_data=eval_data[complete_list]
    # configuration = Configurator(int(imo))

    # print("CLEAN DATA")
    # ref_data=clean_data(ref_data)
    # eval_data=clean_data(eval_data)
    
    # print("Z SCORE")
    # ref_data=z_score_data(ref_data,Y_list)
    # ref_data=z_score_data(ref_data,Y_list)

    # eval_data=avg_columns(eval_data)
    # for y_axis in Y_list:
    #     # print(y_axis)
    #     pls_reg=LRPI()
    #     pls_reg.fit(ref_data[X_list_norepdt],ref_data[y_axis])
    #     eval_pred=pls_reg.predict(eval_data[X_list_norepdt])
    #     # print(eval_pred)
    #     eval_data[y_axis+'_pred']=eval_pred['Pred']

    # # eval_data.to_csv('Evaluation.csv')
    # eval_data['pwr_pred_x']=eval_data['pwr_pred']
    # ref_data['pwr_pred_x']=ref_data['pwr']
    # new_xlist=X_list
    # new_xlist=new_xlist+["pwr_pred_x"]
    # new_xlist.remove("rep_dt")
    # new_ylist=Y_list
    # new_ylist.remove("pwr")

    # for y_axis in new_ylist:
    #     pls_reg=LRPI()
    #     pls_reg.fit(ref_data[new_xlist],ref_data[y_axis])
    #     eval_pred=pls_reg.predict(eval_data[new_xlist])
    #     eval_data[y_axis+'_second_pred']=eval_pred['Pred']

    # for y_axis in Y_list:
    #     loss_var_list=[]
    #     for row in range(0,len(eval_data)):
    #         loss_var=((eval_data[y_axis+"_second_pred"][row]-eval_data[y_axis+"_pred"][row])/eval_data[y_axis+"_pred"][row])*100
    #         loss_var_list.append(loss_var)
    #     eval_data[y_axis+'_loss']=loss_var_list

    

    
    configuration = Configurator(int(imo))

    Y_list=["speed_stw_calc","pwr","app_slip","main_fuel_per_dst"]
    X_list=["rep_dt","draft_mean","trim","rpm","w_dir_rel","swell_dir_rel","amb_temp","cpress","w_force"]
    X_list_norepdt=["draft_mean","trim","rpm","w_dir_rel","swell_dir_rel","amb_temp","cpress","w_force"]
    complete_list=X_list+Y_list

    dry_Dock_period_partition=dry_dock_period.split("-")
    eval_data_period_partition=eval_data_period.split("-")
    dry_Dock_period_start=datetime.strptime(dry_Dock_period_partition[0], '%d/%m/%Y')
    dry_Dock_period_end=datetime.strptime(dry_Dock_period_partition[1], '%d/%m/%Y')
    eval_data_period_start=datetime.strptime(eval_data_period_partition[0], '%d/%m/%Y')
    eval_data_period_end=datetime.strptime(eval_data_period_partition[1], '%d/%m/%Y')
    base_dataframe=create_base_dataframe(imo,complete_list)

    temp_ref_data_period = {}
    temp_eval_data_period = {}
    if performance_type=='maintainance_trigger':
        new_month=dry_Dock_period_end+relativedelta(months=6)
        ref_data=base_dataframe.loc[(base_dataframe['rep_dt'] >= dry_Dock_period_end) & (base_dataframe['rep_dt'] < new_month)]
        ref_data=ref_data.reset_index(drop=True)
        eval_data=base_dataframe.loc[(base_dataframe['rep_dt'] >= eval_data_period_start) & (base_dataframe['rep_dt'] < eval_data_period_end)]
        eval_data=eval_data.reset_index(drop=True)
        temp_ref_data_period['Reference'] = {
            'Start': dry_Dock_period_end.strftime('%Y/%m/%d'),
            'End': new_month.strftime('%Y/%m/%d')
        }
        temp_eval_data_period['Evaluation'] = {
            'Start': eval_data_period_start.strftime('%Y/%m/%d'),
            'End': eval_data_period_end.strftime('%Y/%m/%d')
        }

    elif performance_type=='maintainance_effect':
        new_month=dry_Dock_period_start-relativedelta(months=6)
        eval_new_month=dry_Dock_period_end+relativedelta(months=6)
        ref_data=base_dataframe.loc[(base_dataframe['rep_dt'] >= new_month) & (base_dataframe['rep_dt'] < dry_Dock_period_start)]
        ref_data=ref_data.reset_index(drop=True)
        eval_data=base_dataframe.loc[(base_dataframe['rep_dt'] >= dry_Dock_period_end) & (base_dataframe['rep_dt'] < eval_new_month)]
        eval_data=eval_data.reset_index(drop=True)
        temp_ref_data_period['Reference'] = {
            'Start': new_month.strftime('%Y/%m/%d'),
            'End': dry_Dock_period_start.strftime('%Y/%m/%d')
        }
        temp_eval_data_period['Evaluation'] = {
            'Start': dry_Dock_period_end.strftime('%Y/%m/%d'),
            'End': eval_new_month.strftime('%Y/%m/%d')
        }


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
    
    temp_eval_data=eval_data.copy()

    temp_ref_data,eval_data=avg_columns(ref_data,eval_data)
    
    for y_axis in Y_list:
        # print(y_axis)
        pls_reg=LRPI()
        pls_reg.fit(temp_eval_data[X_list_norepdt],temp_eval_data[y_axis])
        eval_pred=pls_reg.predict(eval_data[X_list_norepdt])
        eval_data[y_axis+'_pred']=eval_pred['Pred']
    pwr_loss_var_list=[]
    for row in range(0,len(eval_data)):
        pwr_loss_var=((eval_data["pwr_pred"][row]-eval_data["pwr"][row])/eval_data["pwr"][row])*100
        pwr_loss_var_list.append(pwr_loss_var)
    eval_data['pwr_loss']=pwr_loss_var_list
    eval_data["pwr_loss_avg"]=np.mean(pwr_loss_var_list)

    for y_axis in Y_list:
        pls_reg=LRPI()
        pls_reg.fit(ref_data[X_list_norepdt],ref_data[y_axis])
        eval_pred=pls_reg.predict(eval_data[X_list_norepdt])
        eval_data[y_axis+'_second_pred']=eval_pred['Pred']

    eval_data['pwr_pred_x']=eval_data['pwr_pred']
    ref_data['pwr_pred_x']=ref_data['pwr']
    new_xlist=X_list
    new_xlist=new_xlist+["pwr_pred_x"]
    new_xlist.remove("rep_dt")
    new_ylist=Y_list
    new_ylist.remove("pwr")

    

    for y_axis in new_ylist:
        pls_reg=LRPI()
        pls_reg.fit(ref_data[new_xlist],ref_data[y_axis])
        eval_pred=pls_reg.predict(eval_data[new_xlist])
        eval_data[y_axis+'_third_pred']=eval_pred['Pred']


    for y_axis in Y_list:
        loss_var_list=[]
        for row in range(0,len(eval_data)):
            loss_var=((eval_data[y_axis+"_third_pred"][row]-eval_data[y_axis+"_pred"][row])/eval_data[y_axis+"_pred"][row])*100
            if y_axis == 'main_fuel_per_dst':
                loss_var_list.append(abs(loss_var))
            else:
                loss_var_list.append(loss_var)
        eval_data[y_axis+'_loss']=loss_var_list
        eval_data[y_axis+"_loss_avg"]=np.mean(loss_var_list)
    
    # ref_data.to_csv("Reference.csv")
    # eval_data.to_csv("Evaluation.csv")
    short_names = configuration.create_short_names_dictionary()
    label_names = ['Regressed based on evaluation data', 'Regressed based on reference data', 'Loss']
    # ref_dict={}
    eval_dict={}
    group=[
        {
            "identifier": 'pwr_pred',
            "short_name": "Power based on evaluation data",
            "block_number": 1
        },
        {
            "identifier": 'pwr_second_pred',
            "short_name": "Power based on reference data",
            "block_number": 1
        },
        # {
        #     "identifier": 'app_slip_pred',
        #     "short_name": short_names['app_slip'] + " based on evaluation data",
        #     "block_number": 2
        # },
        {
            "identifier": 'main_fuel_per_dst_pred',
            "short_name": "Fuel per Distance based on evaluation data",
            "block_number": 2
        },
        {
            "identifier": 'speed_stw_calc_pred',
            "short_name": "Speed Through Water based on evaluation data",
            "block_number": 3
        },
        # {
        #     "identifier": 'app_slip_third_pred',
        #     "short_name": short_names['app_slip'] + " based on reference data",
        #     "block_number": 2
        # },
        {
            "identifier": 'main_fuel_per_dst_third_pred',
            "short_name": "Fuel per Distance based on reference data",
            "block_number": 2
        },
        {
            "identifier": 'speed_stw_calc_third_pred',
            "short_name": "Speed Through Water based on reference data",
            "block_number": 3
        },
        {
            "identifier": 'speed_stw_calc_loss',
            "short_name": "Speed Through Water - Loss(%) Now",
            "block_number": 4
        },
        {
            "identifier": 'pwr_loss',
            "short_name": "Power Loss(%) Now",
            "block_number": 4
        },
        # {
        #     "identifier": 'app_slip_loss',
        #     "short_name": short_names['app_slip'] + " Loss",
        #     "block_number": 5
        # },
        {
            "identifier": 'main_fuel_per_dst_loss',
            "short_name": "Fuel per Distance - Rise Now(%)",
            "block_number": 4
        },
        {
            "identifier": 'pwr_loss_avg',
            "short_name": "Power Loss(%) Average",
            "block_number": 4
        },
        # {
        #     "identifier": 'app_slip_loss_avg',
        #     "short_name": short_names['app_slip'] + " Average",
        #     "block_number": 5
        # },
        {
            "identifier": 'main_fuel_per_dst_loss_avg',
            "short_name": "Fuel per Distance - Rise Average(%)",
            "block_number": 4
        },
        {
            "identifier": 'speed_stw_calc_loss_avg',
            "short_name": "Speed Through Water Loss(%) Average",
            "block_number": 4
        }
    ]

    chart_height = configuration.get_height_of_chart(group)

    order_of_loss_avg = [
        'pwr_loss_avg',
        'speed_stw_calc_loss_avg',
        'main_fuel_per_dst_loss_avg',
        'pwr_loss',
        'speed_stw_calc_loss',
        'main_fuel_per_dst_loss'
    ]
    # for column in ref_data.columns:
    #     if column == 'rep_dt':
    #         temp = []
    #         for d in ref_data[column]:
    #             temp.append(d.strftime('%Y-%m-%d'))
    #         ref_dict[column] = temp
    #     else:
    #         temp_list=list(ref_data[column])
    #         ref_dict[column] = temp_list
    print("RESTRUCTURE")
    for column in eval_data.columns:
        if column == 'rep_dt':
            temp = []
            for d in eval_data[column]:
                temp.append(d.strftime('%Y-%m-%d'))
            eval_dict[column] = temp
        else:
            if column == 'pwr_pred' or column == 'pwr_second_pred' or column == 'main_fuel_per_dst_pred' or column == 'speed_stw_calc_pred' or column == 'speed_stw_calc_third_pred' or column == 'main_fuel_per_dst_third_pred' or column == 'speed_stw_calc_loss' or column == 'main_fuel_per_dst_loss' or column == 'pwr_loss' or column == 'pwr_loss_avg' or column == 'main_fuel_per_dst_loss_avg' or column == 'speed_stw_calc_loss_avg':
                temp_list=list(map(configuration.makeDecimal, eval_data[column]))
                eval_dict[column] = temp_list
    
    
    return eval_dict, short_names, group, chart_height, temp_ref_data_period, temp_eval_data_period, order_of_loss_avg

# start_time = time.time()
# drydock("01/12/2015-15/12/2015","15/05/2015-15/11/2015",9205926)

# end_time=time.time()
# print(end_time-start_time)