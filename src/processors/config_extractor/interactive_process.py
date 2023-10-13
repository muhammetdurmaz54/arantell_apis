# react_application backend for handling interactive process


# import sys
# from time import process_time_ns
# sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
# from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.processors.config_extractor.configurator import Configurator
import re
# from src.helpers.check_status import check_status
# from flask import request,jsonify
# from pymongo import DESCENDING, MongoClient, ASCENDING
# from bson.json_util import dumps, loads
# import numpy as np
# from datetime import datetime

log = CommonLogger(__name__,debug=True).setup_logger()

class InteractiveExtractor():
    def __init__(self, ship_imo, X, Y, duration, load, compare="None", Z=None, color=None, size=None, shape=None, typeofinput="input", **other_X):
        self.ship_imo = int(ship_imo)
        self.X = X
        self.Y = Y
        self.Z = Z
        self.color = color
        self.size = size
        self.shape = shape
        self.duration = duration
        self.other_X = other_X
        self.typeofinput = typeofinput
        self.load = load
        self.compare = compare
        self.sisterorsimilar = None
        x = re.search("[0-9]", self.compare)
        if x:
            self.sisterorsimilar = True
        else:
            self.sisterorsimilar = False
    
    def read_data(self):
        self.configuration = Configurator(self.ship_imo)
        self.ship_configs = self.configuration.get_ship_configs()
        self.main_db = self.configuration.get_main_data()
        short_names = self.configuration.create_short_names_dictionary()
        other_dict={}
        for key in self.other_X.keys():
            other_dict[short_names[key]] = self.other_X[key]
        result={}
        if self.Z is not None:
            compare_param = self.sisterorsimilar if self.sisterorsimilar else self.compare
            sis_or_sim = self.compare if self.sisterorsimilar else ""
            if self.typeofinput == 'input':
                try:
                    X_name, Y_name, Z_name, X_list, Y_list, Z_list, actual_X_list, actual_Y_list, actual_Z_list = self.configuration.create_surface_data(self.X, self.Y, self.duration, self.Z, self.load, compare_param, sis_or_sim, **self.other_X)
                    # dataframe, X_name, Y_name, Z_name, X_list, Y_list, Z_list = self.configuration.create_dataframe(self.X, self.Y, self.duration, self.Z, **self.other_X)
                    # X1, Y1, pred_list = self.configuration.regress_for_constant_x(dataframe, self.X, self.Y, self.Z, 'input', **self.other_X)
                    result['Prediction'] = {
                        'x': X_list,
                        'y': Y_list,
                        'z': Z_list
                    }
                    result['Actual Values'] = {
                        'x': actual_X_list,
                        'y': actual_Y_list,
                        'z': actual_Z_list
                    }
                    return X_name, Y_name, Z_name, result, other_dict
                except ValueError:
                    result = self.configuration.create_surface_data(self.X, self.Y, self.duration, self.Z, self.load, compare_param, sis_or_sim, **self.other_X)
                    return result, other_dict
                # result['Prediction'] = {
                #     'x': X1,
                #     'y': Y1,
                #     'z': pred_list
                # }
                # result['Actual Values'] = {
                #     'x': X_list,
                #     'y': Y_list,
                #     'z': Z_list
                # }
                # return X_name, Y_name, Z_name, result
            if self.typeofinput == 'target':
                empty_x_constant_list = []
                compare_param = self.sisterorsimilar if self.sisterorsimilar else self.compare
                sis_or_sim = self.compare if self.sisterorsimilar else ""
                dataframe, X_name, Y_name, Z_name, X_list, Y_list, Z_list = self.configuration.create_dataframe(self.X, self.Y, self.duration,self.load, compare_param, sis_or_sim, self.Z, **self.other_X)
                if len(set(X_list)) == 1 or len(set(X_list)) == 0:
                    X_empty = "{} (X variable) does not have values. Hence, a prediction cannot be made.".format(X_name)
                    return X_empty
                elif len(set(Y_list)) == 1 or len(set(Y_list)) == 0:
                    Y_empty = "{} (Y variable) does not have values. Hence, a prediction cannot be made.".format(Y_name)
                    return Y_empty
                elif len(set(Z_list)) == 1 or len(set(Z_list)) == 0:
                    Z_empty = "{} (Z variable) does not have values. Hence, a prediction cannot be made.".format(Z_name)
                    return Z_empty
                else:
                    for col in dataframe.columns:
                        if col not in list(self.other_X.keys()):
                            empty_x_constant_list.append(col)
                    X1, Y1, pred_list, clean_X_list, clean_Y_list, clean_Z_list = self.configuration.regress_for_constant_x(dataframe, self.X, self.Y, self.Z, **self.other_X)
                    result['Prediction'] = {
                        'x': X1,
                        'y': pred_list,
                        'z': Y1
                    }
                    result['Actual Values'] = {
                        'x': clean_X_list,
                        'y': clean_Y_list,
                        'z': clean_Z_list
                    }
                    return X_name, Y_name, Z_name, result, other_dict
        else:
            empty_x_constant_list = []
            compare_param = self.sisterorsimilar if self.sisterorsimilar else self.compare
            sis_or_sim = self.compare if self.sisterorsimilar else ""
            dataframe, X_name, Y_name, X_list, Y_list = self.configuration.create_dataframe(self.X, self.Y, self.duration, self.load, compare_param, sis_or_sim, **self.other_X)
            print("X & Y LIST!!!!", set(X_list), set(Y_list))
            if len(set(X_list)) == 1 or len(set(X_list)) == 0:
                X_empty = "{} (X variable) does not have values. Hence, a prediction cannot be made.".format(X_name)
                return X_empty
            elif len(set(Y_list)) == 1 or len(set(Y_list)) == 0:
                Y_empty = "{} (Y variable) does not have values. Hence, a prediction cannot be made.".format(Y_name)
                return Y_empty
            else:
                for col in dataframe.columns:
                    if col not in list(self.other_X.keys()):
                        empty_x_constant_list.append(col)
                X1, pred_list, clean_X_list, clean_Y_list = self.configuration.regress_for_constant_x(dataframe, self.X, self.Y, **self.other_X)
                temp=['draft_mean', 'sea_st', 'trim']
                # print(self.configuration.get_ship_stats(*temp))
                # X1.sort()
                # pred_list.sort()
                # X_list.sort()
                # Y_list.sort()
                result['Prediction'] = {
                    'x': X1,
                    'y': pred_list
                }
                result['Actual Values'] = {
                    'x': clean_X_list,
                    'y': clean_Y_list
                }
                return X_name, Y_name, result, other_dict