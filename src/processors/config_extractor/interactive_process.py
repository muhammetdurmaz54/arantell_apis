import sys
from time import process_time_ns
# sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.processors.config_extractor.configurator import Configurator
from src.helpers.check_status import check_status
from flask import request,jsonify
from pymongo import DESCENDING, MongoClient, ASCENDING
from bson.json_util import dumps, loads
import numpy as np
from datetime import datetime

log = CommonLogger(__name__,debug=True).setup_logger()

class InteractiveExtractor():
    def __init__(self, ship_imo, X, Y, duration, Z=None, color=None, size=None, shape=None, **other_X):
        self.ship_imo = int(ship_imo)
        self.X = X
        self.Y = Y
        self.Z = Z
        self.color = color
        self.size = size
        self.shape = shape
        self.duration = duration
        self.other_X = other_X
    
    def read_data(self):
        self.configuration = Configurator(self.ship_imo)
        self.ship_configs = self.configuration.get_ship_configs()
        self.main_db = self.configuration.get_main_data()
        result={}
        if self.Z is not None:
            dataframe, X_name, Y_name, Z_name, X_list, Y_list, Z_list = self.configuration.create_dataframe(self.X, self.Y, self.duration, self.Z, **self.other_X)
            X1, Y1, pred_list = self.configuration.regress_for_constant_x(dataframe, self.X, self.Y, self.Z, **self.other_X)
            result['Prediction'] = {
                'x': X1,
                'y': Y1,
                'z': pred_list
            }
            result['Actual Values'] = {
                'x': X_list,
                'y': Y_list,
                'z': Z_list
            }
            return X_name, Y_name, Z_name, result
        else:
            dataframe, X_name, Y_name, X_list, Y_list = self.configuration.create_dataframe(self.X, self.Y, self.duration, **self.other_X)
            X1, pred_list = self.configuration.regress_for_constant_x(dataframe, self.X, self.Y, **self.other_X)
            temp=['draft_mean', 'sea_st', 'trim']
            # print(self.configuration.get_ship_stats(*temp))
            result['Prediction'] = {
                'x': X1,
                'y': pred_list
            }
            result['Actual Values'] = {
                'x': X_list,
                'y': Y_list
            }
            return X_name, Y_name, result