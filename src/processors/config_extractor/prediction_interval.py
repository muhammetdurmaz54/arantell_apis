from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd
from sklearn import linear_model

class LRPI:
    def __init__(self, normalize=False, n_jobs=1, t_value = 2.13144955):
        self.normalize = normalize
        self.n_jobs = n_jobs
        self.LR = linear_model.LinearRegression(normalize=self.normalize, n_jobs= self.n_jobs)
        self.t_value = t_value
        
    def fit(self, X_train, y_train):
        self.X_train = pd.DataFrame(X_train)
        self.y_train = pd.DataFrame(y_train)
        self.LR.fit(self.X_train, self.y_train)
        X_train_fit = self.LR.predict(self.X_train)
        self.MSE = np.power(self.y_train.subtract(X_train_fit), 2).sum(axis=0) / (self.X_train.shape[0] - self.X_train.shape[1] - 1)
        self.X_train.loc[:, 'const_one'] = 1
        # self.XTX_inv = np.linalg.inv(np.dot(np.transpose(self.X_train) , self.X_train))
        
    def predict(self, X_test):
        self.X_test = pd.DataFrame(X_test)
        self.pred = self.LR.predict(self.X_test)
        self.X_test.loc[: , 'const_one'] =1
        # SE = [np.dot(np.transpose(self.X_test[i]) , np.dot(self.XTX_inv, self.X_test[i]) ) for i in range(len(self.X_test)) ]
        results = pd.DataFrame(self.pred , columns=['Pred'])
        # results.loc[:,"lower"] = results['Pred'].subtract((self.t_value)* (np.sqrt(self.MSE + np.multiply(SE,self.MSE) )),  axis=0)
        # results.loc[:,"upper"] = results['Pred'].add((self.t_value)* (np.sqrt(self.MSE + np.multiply(SE,self.MSE) )),  axis=0)
        return results