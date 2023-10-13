# not react application backend, new pls method trial by using rpm,draftmean and trim multiple columns

import pandas 
import random
# from bson import json_util
import scipy.stats as st
import time
from pandas.core.indexes.datetimes import date_range
from sklearn.cross_decomposition import PLSRegression
from dateutil.relativedelta import relativedelta
from datetime import date, datetime
import numpy as np
from sklearn.linear_model import LinearRegression, Ridge
from sklearn import preprocessing

import statsmodels.api as sm

class LRPI:
    def __init__(self, normalize=False, n_jobs=1, t_value = 2.13144955):
        self.normalize = normalize
        self.n_jobs = n_jobs
        self.LR = PLSRegression(n_components=4)
        self.t_value = t_value
        
    def fit(self, X_train, y_train):
        self.X_train = pandas.DataFrame(X_train)
        self.y_train = pandas.DataFrame(y_train)
        # print(self.X_train)
        # print(self.y_train)
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
            # print("greaaaaaaaaaaaaat")
            SE=[0.3]*len(self.X_test)
        # SE=[0.3]*len(self.X_test)
        results = pandas.DataFrame(self.pred , columns=['Pred'])
        results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE.values + np.multiply(SE,self.MSE.values) )),  axis=0)
        # print(results)
        return results

def z_score_data(new_data,identifier):
    mean_val=np.mean(new_data[identifier])
    standdev=np.std(new_data[identifier])
    # print(standdev)
    zsc_list=[]
    try:
        for ident in identifier:
            z_score=st.zscore(new_data[ident])
            new_data['z_score_'+ident]=z_score
    except:
        for ident in identifier:
            for rowval in new_data[ident]:
                print(rowval)
                zsc=(rowval-mean_val[ident])/standdev[ident]
                zsc_list.append(zsc)
            new_data['z_score_'+ident]=zsc_list
    for ident in identifier:
        new_data= new_data.drop(index=new_data[new_data['z_score_'+ident] > 2].index)
        new_data= new_data.drop(index=new_data[new_data['z_score_'+ident] < -2].index)
        new_data=new_data.reset_index(drop=True)
        new_data=new_data.drop(columns='z_score_'+ident)
        new_data=new_data.reset_index(drop=True)
    return new_data



# X_list=["draft_mean","trim","displ","w_force","swell","w_rel_0","swell_rel_0","rpm","w_rel_90","swell_rel_90","speed_stw_calc"]
X_list=["draft_mean","trim","swell","w_rel_0_newFormula","swell_rel_0","rpm","w_rel_90_newFormula","swell_rel_90","speed_sog_calc","w_dir_rel_new"]
X_list_norpm=["swell","w_rel_0_newFormula","swell_rel_0","w_rel_90_newFormula","swell_rel_90","speed_sog_calc","w_dir_rel_new"]
Y='pwr'
X_pls_list=['rpm','draft_mean','trim']
complete_list=["pwr","draft_mean","trim","swell","w_rel_0_newFormula","swell_rel_0","rpm","w_rel_90_newFormula","swell_rel_90","speed_sog_calc","w_dir_rel_new"]
data=pandas.read_csv("weland_data_clean_speedsog.csv")
test_data_new=pandas.read_excel("weland_data_clean_evalspeedsog.xlsx")
test_data_new=test_data_new
data=data.head(221)
actual_y_test=test_data_new[Y]
# print(actual_y_test)

data=z_score_data(data,complete_list)
data=z_score_data(data,complete_list)   #training data 26 rows
data[X_pls_list]=data[X_pls_list]

# print(data)
# print(test_data.iloc[[1]])
test_data=test_data_new.copy(deep=True)
# test_data=test_data.append(test_data_new)
test_data=test_data.reset_index(drop=True)  #testin data 27 rows
test_data[X_pls_list]=test_data[X_pls_list]
test_data_new[X_pls_list]=test_data[X_pls_list]
# print(data)
# print(test_data)
data.to_csv("training_data.csv")
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

pred=pls_reg.predict(X_test)

pred_temp=[]
for i in range(0,len(pred['Pred'])):
    pred_temp.append((pred['Pred'].iloc[i]*std_y)+mean_y)
test_data_new['current_pls_pred']=pred_temp
test_data_new['current_pls_pred_diff']=test_data_new['current_pls_pred']-test_data_new[Y]
test_data_new['current_pls_pred_diff_square']=test_data_new['current_pls_pred_diff']**2


#new ypls + yols formula from here
reshaped=X_train[X_pls_list].to_numpy()
# reshaped=rpm_data.reshape(-1,1)
y_data=Y_train.to_numpy()
reshaped_y=y_data.reshape(-1,1)
col_1_reshaped=X_train['rpm'].to_numpy()
col_1_reshaped=col_1_reshaped.reshape(-1,1)
col_2_reshaped=X_train['draft_mean'].to_numpy()
col_2_reshaped=col_2_reshaped.reshape(-1,1)
col_3_reshaped=X_train['trim'].to_numpy()
col_3_reshaped=col_3_reshaped.reshape(-1,1)

reg = Ridge(alpha=0.2,fit_intercept=True).fit(reshaped,Y_train)
reg_2=Ridge(alpha=0.2,fit_intercept=True).fit(col_1_reshaped,X_train[X_list_norpm])
reg_3=Ridge(alpha=0.2,fit_intercept=True).fit(col_2_reshaped,X_train[X_list_norpm])
reg_4=Ridge(alpha=0.2,fit_intercept=True).fit(col_3_reshaped,X_train[X_list_norpm])
beta=reg.coef_
# Y_OLS=np.dot(reshaped,beta)+mean_y

delta1=[]
for i in reg_2.coef_:
  delta1.append(i)
delta1=np.array([delta1])
delta1=delta1.reshape(1,-1)
delta2=[]
for i in reg_3.coef_:
  delta2.append(i)
delta2=np.array([delta2])
delta2=delta2.reshape(1,-1)
delta3=[]
for i in reg_4.coef_:
  delta3.append(i)
delta3=np.array([delta3])
delta3=delta3.reshape(1,-1)
#Z1ortho here
col_1_reshaped=X_train['rpm'].to_numpy()
col_1_reshaped=col_1_reshaped.reshape(-1,1)
res = np.dot(col_1_reshaped,delta1)
# print("x * delta T",res)
z_axis=X_train[X_list_norpm].to_numpy()
# print("Z other than rpm",z_axis)
Z1_ortho=z_axis-res

#Z2ortho here
col_2_reshaped=X_train['draft_mean'].to_numpy()
col_2_reshaped=col_2_reshaped.reshape(-1,1)
res = np.dot(col_2_reshaped,delta2)
Z2_ortho=Z1_ortho-res

#final Zortho here
col_3_reshaped=X_train['trim'].to_numpy()
col_3_reshaped=col_3_reshaped.reshape(-1,1)
res = np.dot(col_3_reshaped,delta3)
Z_ortho=Z2_ortho-res

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
test_col_1_reshaped=X_test['rpm'].to_numpy()
test_col_1_reshaped=test_col_1_reshaped.reshape(-1,1)
test_col_2_reshaped=X_test['draft_mean'].to_numpy()
test_col_2_reshaped=test_col_2_reshaped.reshape(-1,1)
test_col_3_reshaped=X_test['trim'].to_numpy()
test_col_3_reshaped=test_col_3_reshaped.reshape(-1,1)

testing_res=np.dot(test_col_1_reshaped,delta1)
testing_z_axis=testing_array[X_list_norpm].to_numpy()
testing_z_ortho1=testing_z_axis-testing_res
# print("test zortho",testing_z_ortho)

testing_res=np.dot(test_col_2_reshaped,delta2)
testing_z_ortho2=testing_z_ortho1-testing_res
# print("test zortho",testing_z_ortho)

testing_res=np.dot(test_col_3_reshaped,delta3)
testing_z_ortho=testing_z_ortho2-testing_res
# print("test zortho",testing_z_ortho)

training_T_calculated=np.dot(Z_ortho,gama_second_part)
training_Y_pls=np.dot(training_T_calculated,gama_pls)
training_Y_ols=np.dot(reshaped,beta)+mean_y
# print("training y pls", training_Y_pls)
# print("training y ols", training_Y_ols)

T_calculated=np.dot(testing_z_ortho,gama_second_part)
# print("T Calculated",T_calculated)
Y_pls=np.dot(T_calculated,gama_pls)
print("YPLS",Y_pls)
reshaped_testing_rpm=testing_array[X_pls_list].to_numpy()
Y_ols=np.dot(reshaped_testing_rpm,beta)+mean_y
print("YOLS",Y_ols)
final_y=Y_ols+Y_pls
test_data_new['pred']=final_y
test_data_new['diff']=test_data_new[Y]-test_data_new['pred']
test_data_new['diff_sqaured']=test_data_new['diff']**2
test_data_new['y_ols']=Y_ols
test_data_new['y_pls']=Y_pls
test_data_new['y_ols_diff']=test_data_new[Y]-test_data_new['y_ols']
test_data_new['y_ols_diff_squared']=test_data_new['y_ols_diff']**2
test_data_new.to_csv("prediction_data.csv")
print(test_data_new)