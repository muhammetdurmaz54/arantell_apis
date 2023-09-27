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


from sklearn.linear_model import LinearRegression, Ridge
from sklearn import preprocessing
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




def new_pls_method(data,test_data):
    X_list=["draft_mean","trim","speed_stw_calc","displ","w_rel_0","w_rel_90","current_rel_0","current_rel_90","swell_rel_0","swell_rel_90"]
    X_list_norpm=["draft_mean","trim","speed_stw_calc","displ","w_rel_0","w_rel_90","current_rel_0","current_rel_90","swell_rel_0","swell_rel_90"]
    
    Y='pwr'
    complete_list=["pwr","draft_mean","trim","displ","swell","w_rel_0_newFormula","swell_rel_0","rpm","w_rel_90_newFormula","swell_rel_90","speed_sog_calc","w_dir_rel_new"]
    avg_list=['current_rel_0','current_rel_90']
    # data=pandas.read_csv("weland_data_clean_speedsog.csv")
    # test_data_new=pandas.read_excel("weland_data_clean_evalspeedsog.xlsx",engine='openpyxl')
    data=data.copy()
    test_data_new=test_data.copy()
    test_data_new=test_data_new
    # data=data.head(221)
    actual_y_test=test_data_new[Y]
    # print(actual_y_test)

    # data=z_score_data(data,[Y])
    # data=z_score_data(data,[Y])   #training data 26 rows
    data['speed_stw_calc']=data['speed_stw_calc']

    # print(data)
    # print(test_data.iloc[[1]])
    test_data=test_data_new.copy(deep=True)
    # test_data=test_data.append(test_data_new)
    test_data=test_data.reset_index(drop=True)  #testin data 27 rows
    test_data['speed_stw_calc']=test_data['speed_stw_calc']
    test_data_new['speed_stw_calc']=test_data['speed_stw_calc']
    # print(data)
    # print(test_data)
    # data.to_csv("training_data.csv")
    # test_data.to_csv("testing_data.csv")

    #standardization part
    training_dict={}
    testing_dict={}
    training_dataframe=pandas.DataFrame(training_dict)
    testing_dataframe=pandas.DataFrame(testing_dict)

    std_y=np.std(data[Y])
    mean_y=np.mean(data[Y])
    # print(std_y,mean_y)
    y_list=[]
    test_y_list=[]
    X_train=pandas.DataFrame({})
    X_test=pandas.DataFrame({})

    for i in X_list:
        if i not in avg_list:
            train_x_list=[]
            test_x_list=[]
            mean_x=np.mean(data[i])
            std_x=np.std(data[i])
            mean_x_test=np.mean(test_data[i])
            std_x_test=np.std(test_data[i])
            try:
                for j in data[i]:
                    val=(j-mean_x)/std_x
                    if pandas.isnull(val)==False:
                        train_x_list.append(val)
                    else:
                        train_x_list.append(j)
                X_train[i]=train_x_list
                
            except:
                X_train[i]=data[i]
            try:
                for j in test_data[i]:
                    test_val=(j-mean_x)/std_x
                    if pandas.isnull(test_val)==False:
                        test_x_list.append(test_val)
                    else:
                        test_x_list.append(j)
                X_test[i]=test_x_list
            except:
                X_test[i]=test_data[i]
        else:
            X_train[i]=data[i]
            X_test[i]=test_data[i]


    for i in data[Y]:
        y_val=i-mean_y
        y_list.append(y_val)
    for i in test_data[Y]:
        test_y_val=i-mean_y
        test_y_list.append(test_y_val)
    training_dataframe[Y]=y_list
    Y_train=training_dataframe[Y]
    testing_dataframe[Y]=test_y_list
    Y_test=testing_dataframe
    # print(Y_train)
    # print(X_train,Y_train)
    # print(X_test,Y_test)


    #old pls trends method here
    old_y_list=[]
    old_test_y_list=[]
    for i in data[Y]:
        y_val=(i-mean_y)/std_y
        old_y_list.append(y_val)
    for i in test_data[Y]:
        test_y_val=(i-mean_y)/std_y
        old_test_y_list.append(test_y_val)
    old_training_dataframe=pandas.DataFrame({})
    old_testing_dataframe=pandas.DataFrame({})
    old_training_dataframe[Y]=old_y_list
    old_Y_train=old_training_dataframe[Y]
    old_testing_dataframe[Y]=old_test_y_list
    old_Y_test=old_testing_dataframe
    pls_reg=LRPI()
    pls_reg.fit(X_train,old_Y_train)
    # print("testtttttttttttttttt",X_test)
    # print(X_train)
    # X_test.to_csv("newplsvtitest.csv")
    pred=pls_reg.predict(X_test)

    pred_temp=[]
    for i in range(0,len(pred['Pred'])):
        pred_temp.append((pred['Pred'].iloc[i]*std_y)+mean_y)
    test_data_new['current_pls_pred']=pred_temp
    test_data_new['current_pls_pred_diff']=test_data_new['current_pls_pred']-test_data_new[Y]
    test_data_new['current_pls_pred_diff_square']=test_data_new['current_pls_pred_diff']**2





    #new ypls + yols formula from here
    rpm_data=X_train['speed_stw_calc'].to_numpy()
    reshaped=rpm_data.reshape(-1,1)
    y_data=Y_train.to_numpy()
    reshaped_y=y_data.reshape(-1,1)


    reg = LinearRegression(fit_intercept=True).fit(reshaped,Y_train)
    reg_2=Ridge(alpha=0.2,fit_intercept=True).fit(reshaped,X_train[X_list_norpm])
    # print("intercept",reg.intercept_)
    # print("intercept_2",reg_2.intercept_)

    delta=[]
    rpm_row=[]
    beta=reg.coef_
    # print("beta",beta)
    # print(np.dot(reshaped,beta))
    for i in reg_2.coef_:
        delta.append(i)
    for i in data['speed_stw_calc']:
        rpm_row.append(i)
    delta=np.array([delta])
    # print(np.dot(rpm_data.reshape(1,-1),np.transpose(delta)))
    # delta=np.transpose(delta)
    delta=delta.reshape(1,-1)
    # print("delta",delta)
    res = np.dot(reshaped,delta)
    # print(np.dot(reshaped,beta))

    # print("x * delta T",res)
    z_axis=X_train[X_list_norpm].to_numpy()
    # print("Z other than rpm",z_axis)
    Z_ortho=z_axis-res
    # print("Z_ORTHO",Z_ortho)
    Z_ortho_data=pandas.DataFrame(Z_ortho)
    # print(Z_ortho_data)
    pls_reg=PLSRegression(n_components=4)
    fitting=pls_reg.fit(Z_ortho_data,Y_train)
    loading_matrix=fitting.x_loadings_
    weight_matrix=fitting.x_weights_
    t_scores=fitting.x_scores_
    # lambda_pls=np.linalg.inv(np.dot(np.transpose(t_scores),t_scores))*np.transpse(t_scores)*y_data

    gama_pls_first=np.linalg.inv(np.dot(np.transpose(t_scores),t_scores))

    gama_pls_second=np.dot(np.transpose(t_scores),y_data)
    gama_pls=np.dot(gama_pls_first,gama_pls_second)
    # print("gama pls",gama_pls)

    gama_first_part=np.linalg.inv(np.dot(np.transpose(loading_matrix),weight_matrix))
    gama_second_part=np.dot(weight_matrix,gama_first_part)
    # gama_value=np.dot(gama_second_part,gama_pls)
    # print("gama",gama_value)
    # print(X_test)
    testing_array=X_test[X_list]
    testing_rpm=testing_array['speed_stw_calc'].to_numpy()
    reshaped_testing_rpm=testing_rpm.reshape(-1,1)
    testing_res=np.dot(reshaped_testing_rpm,delta)
    # print("testing_rpm",reshaped_testing_rpm)
    testing_z_axis=testing_array[X_list_norpm].to_numpy()
    testing_z_ortho=testing_z_axis-testing_res
    # print("test zortho",testing_z_ortho)

    training_T_calculated=np.dot(Z_ortho,gama_second_part)
    training_Y_pls=np.dot(training_T_calculated,gama_pls)
    training_Y_ols=np.dot(reshaped,beta)+mean_y
    # print("training y pls", training_Y_pls)
    # print("training y ols", training_Y_ols)

    T_calculated=np.dot(testing_z_ortho,gama_second_part)
    # print("T Calculated",T_calculated)
    Y_pls=np.dot(T_calculated,gama_pls)
    # print("YPLS",Y_pls)
    Y_ols=np.dot(reshaped_testing_rpm,beta)+mean_y
    # print("YOLS",Y_ols)
    final_y=Y_ols+Y_pls
    test_data_new['pred']=final_y
    test_data_new['diff']=test_data_new[Y]-test_data_new['pred']
    test_data_new['diff_sqaured']=test_data_new['diff']**2
    test_data_new['y_ols']=Y_ols
    test_data_new['y_pls']=Y_pls
    test_data_new['y_ols_diff']=test_data_new[Y]-test_data_new['y_ols']
    test_data_new['y_ols_diff_squared']=test_data_new['y_ols_diff']**2
    # test_data_new.to_csv("prediction_data.csv")
    # print(test_data_new)
    return test_data_new
# print("second_pred",final_y)
# print(testing_z_ortho)
# print(final_y-actual_y_test)







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
    # eval_data=base_dataframe.loc[(base_dataframe['rep_dt'] >= eval_data_period_end)]
    # eval_data=eval_data.reset_index(drop=True)

    ref_data=ref_data[complete_list]
    # eval_data=eval_data[complete_list]

    ref_data=clean_data(ref_data)
    # eval_data=clean_data(eval_data)
    
    ref_data=z_score_data(ref_data,Y_list)
    ref_data=z_score_data(ref_data,Y_list)
    
    # eval_data=z_score_data(eval_data,Y_list)
    # eval_data=z_score_data(eval_data,Y_list)

    ref_data=t2_check(ref_data,X_list_norepdt,Y_list)
    # eval_data=t2_check(eval_data,X_list_norepdt,Y_list)
    # print("reffff", ref_data)
    maindb = database.get_collection("Main_db")
    # maindata = maindb.find({"ship_imo": int(imo)}).sort('final_rep_dt', ASCENDING)
    maindata = maindb.find({"ship_imo": int(imo)})
    # print(maindata.count())
    sample_vti_list=[]
    date_list=[]
    for i in range(0,maindata.count()):
        print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom",i)
        # print(type(maindata[i]['processed_daily_data']['rep_dt']['processed']),maindata[i]['processed_daily_data']['rep_dt']['processed'])
        # print(type(ref_data.iloc[-1]['rep_dt']),ref_data.iloc[-1]['rep_dt'])
        if maindata[i]['vessel_loaded_check']=="Loaded" and maindata[i]['processed_daily_data']['rep_dt']['processed'] > ref_data.iloc[-1]['rep_dt']:
            try:
                temp_dict={}
                for key in complete_list:
                    if key in maindata[i]['processed_daily_data']:
                        temp_dict[key]=[maindata[i]['processed_daily_data'][key]['processed']]
                    else:
                        temp_dict[key]=[None]
                eval_data=pandas.DataFrame(temp_dict)
                eval_data=eval_data.reset_index(drop=True)
                # print("evalll",eval_data)

                # print("reff",ref_data)
                # print("evalll",eval_data)
                # eval_data=eval_data.tail(1)
                temp_eval_data=eval_data.copy()

                sample_ref_data,temp_eval_data=avg_columns(ref_data,temp_eval_data)
                # print("temp_eval_Data",temp_eval_data)



                pls_reg=LRPI()
                pls_reg.fit(ref_data[X_list_norepdt],ref_data[Y_list])
                # eval_pred=pls_reg.predict(eval_data[X_list_norepdt])
                # print(ref_data)
                temp_eval_pred=pls_reg.predict(temp_eval_data[X_list_norepdt])
                # print(temp_eval_pred['Pred'][0])
                temp_eval_data['n_ref_k']=temp_eval_pred['Pred'][0]
                # print("temp eval data",temp_eval_data)



                eval_pred=new_pls_method(ref_data,eval_data)
                eval_data['Ypredict_actualEnv']=eval_pred['y_pls']
                # print("eval data",eval_data)
                # zero_list=["w_rel_0","w_rel_90","current_rel_0","current_rel_90","swell_rel_0","swell_rel_90"]
                # for col in zero_list:
                #     temp_eval_data[col].values[:] = 0
                # exit()

                # eval_data['Ypredict_refEnv']=temp_eval_data['n_ref_k']
                # eval_data['Ynorm_correction']=abs(eval_data['Ypredict_actualEnv']-eval_data['Ypredict_refEnv'])
                
                # eval_data['y_eval_k']=eval_data['pwr']-eval_data['Ynorm_correction']

                eval_data['n_ref_k']=temp_eval_data['n_ref_k']
                # eval_data['vti']=eval_data['y_eval_k']/temp_eval_data['n_ref_k']
                eval_data['vti']=abs(eval_data['pwr']-eval_data['Ypredict_actualEnv'])/temp_eval_data['n_ref_k']
                # eval_data.to_csv("vti_process_avg_newpls.csv")
                # print("finall",eval_data)
                final_vti=eval_data['vti'][0]
                # sample_vti_list.append(final_vti)
                # date_list.append(maindata[i]['processed_daily_data']['rep_dt']['processed'])
            except:
                final_vti=None
        else:
            final_vti=None
        print("vtiii",final_vti)
        # maindb.update_one(maindb.find({"ship_imo": int(imo)})[i],{"$set":{"vti":final_vti}})
        print("updated")
    # date_list=date_list[::-1]
    # print(sample_vti_list)
    # print(round(np.average(sample_vti_list[-40]),2))
    # print(date_list[0:40])
    # print("final", np.round(eval_data.iloc[-1]['vti'],2))
    # print("avg",np.round(np.average(eval_data.tail(-30)['vti']),2))
    

# start_time = time.time()
vti_process("01/01/2015-15/01/2015","16/01/2015-16/09/2015",9205926)
# end_time=time.time()
# print(end_time-start_time)

