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

# Set lower and upper quantile
LOWER_ALPHA = 0.1
UPPER_ALPHA = 0.9

class regress(object):

    def __init__(self, data, x, y, impute=None, standardize=True, polynomials=0, train_test=0,interaction_only=True,type='LR'):
        self.data = data.reset_index(drop=True)
        self.x = x
        self.y = y
        self.standardize = standardize
        self.impute = impute
        self.polynomials = polynomials
        self.train_test = train_test
        self.interaction_only=interaction_only
        self.type=type

    def fit(self):
        before = self.data.shape[0]
        self.data = self.data.dropna(subset=[self.y])
        after = self.data.shape[0]

        if before - after != 0:
            print(f"Dropped {before - after} rows, because of NaN in target")

        target = self.data[self.y]
      #  print(f"Target {self.y} is set.")

        X = self.data[self.x].reset_index(drop=True)

        print(f"X {X.shape} is set with columns f{self.x}")

        if self.impute != None:
            print("Imputing")
            pass

        if self.polynomials != 0:
            self.polynomimal_features = PolynomialFeatures(degree=self.polynomials,
                                                           interaction_only=self.interaction_only,
                                                           include_bias=False)
            XP = self.polynomimal_features.fit_transform(X)
            print(XP.shape,"Polynomials shape")
            tempx = pd.DataFrame(XP)
            tempx.to_csv("tempx.csv")

        else:
            print("No Polynomials")
            XP = X

        if self.standardize is True:
            self.scaler = StandardScaler()
            XPS = self.scaler.fit_transform(XP)
            print("Data Standardized")
        else:
            XPS = XP

        XPS = pd.DataFrame(XP)
      #  XPS.to_csv("XPS.csv")
       # pd.Series(target).to_csv("target.csv")
      #  print(target.shape)
      #  print(XPS.shape)
       # print(XPS)
        #self.reg = linear_model.LinearRegression(normalize=True).fit(XPS, target)
        self.reg = sm.OLS(target,XPS).fit()


        self.score = 9 #self.reg.score(XPS, np.array(target))
        self.y_fit = self.reg.predict(XPS)
        resid = np.array(target) - self.y_fit
        sigma2_est = sum(resid ** 2) / (len(target) - 2)

        self.var_beta = sigma2_est * np.linalg.inv(np.dot(np.transpose(XPS), XPS))
        self.std_err = np.sqrt(np.diag(self.var_beta))


    def predict_sm(self, x):

        if self.polynomials != 0:
            x = self.polynomimal_features.transform(x.reset_index(drop=True))
            # for col in x.columns:
            #     x[col+"poly"]=x[col]*x[col]
        if self.standardize is True:
            x = self.scaler.transform(x)
        ypred = self.model.predict(x)
        _, upper, lower = wls_prediction_std(self.model,alpha=0.9)

        return lower,ypred,upper

    def predict(self, x):
        if self.polynomials != 0:
            x = self.polynomimal_features.transform(x.reset_index(drop=True))
        if self.standardize is True:
            x = self.scaler.transform(x)

        return self.reg.predict(x)


    def predict_intervals(self, x,alpha):
        if self.polynomials != 0:
            print("Creating Polynomials in predict")
            print(x.shape)
            x = self.polynomimal_features.transform(x.reset_index(drop=True))
            print(x.shape)
        tempxpred = pd.DataFrame(x)
        tempxpred.to_csv("tempxpred.csv")
        y_pred = self.reg.predict(x)

        ym_se = np.dot(np.dot(x, self.var_beta), np.transpose(x))
        ym_se = np.sqrt(np.diag(ym_se))
        N = x.shape[0]
        # Confidence intervals for the MEAN resposne of Y:
        ym_lower = y_pred - stats.t.ppf(q=1 - alpha / 2, df=N - 2) * ym_se
        ym_upper = y_pred + stats.t.ppf(q=1 - alpha / 2, df=N - 2) * ym_se
      #  print(pd.DataFrame(np.column_stack([self.y_fit, ym_lower, ym_upper])).head())
        return ym_lower, y_pred, ym_upper


