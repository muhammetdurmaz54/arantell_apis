import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn.ensemble import GradientBoostingRegressor
import statsmodels.api as sm
import scipy.stats as stats
from statsmodels.sandbox.regression.predstd import wls_prediction_std
import pandas as pd


class regress(object):

    def __init__(self, data, x, y, impute=None, standardize=True, polynomials=0, train_test=0):
        self.data = data.reset_index(drop=True)
        self.x = x
        self.y = y
        self.standardize = standardize
        self.impute = impute
        self.polynomials = polynomials
        self.train_test = train_test


    def fit(self):
        before = self.data.shape[0]
        self.data = self.data.dropna(subset=[self.y])
        after = self.data.shape[0]
        
        if before - after != 0:
            print(f"Dropped {before-after} rows, because of NaN in target")
        
        target = self.data[self.y]
        print(f"Target {self.y} is set.")
        
        X = self.data[self.x]
        print(f"X {X.shape} is set with columns f{self.x}")
        
        if self.impute != None:
            print("Imputing")
            pass

        if self.polynomials != 0:
            self.polynomimal_features = PolynomialFeatures(degree=self.polynomials)
            XP = self.polynomimal_features.fit_transform(X, )
            print(f"Polynomials Generated with shape {XP.shape}")
        else:
            print("No Polynomials")
            XP = X

        if self.standardize is True:
            self.scaler = StandardScaler()
            XPS = self.scaler.fit_transform(XP)
            print("Data Standardized")
        else:
            XPS = XP

        self.reg = LinearRegression().fit(XPS, np.array(target))
        score = self.reg.score(XPS, np.array(target))
        print(f"Score is {score}")

    def predict(self, x):
        if self.polynomials != 0:
            x = self.polynomimal_features.transform(x)
        if self.standardize is True:
            x = self.scaler.transform(x)

        return self.reg.predict(x)