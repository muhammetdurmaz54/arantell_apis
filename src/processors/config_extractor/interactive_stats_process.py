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
import math

log = CommonLogger(__name__,debug=True).setup_logger()

class InteractiveStatsExtractor():
    def __init__(self, ship_imo, duration, variables):
        self.ship_imo = int(ship_imo)
        self.duration = duration
        self.variables = variables

    def read_data_for_stats(self):
        self.configuration = Configurator(self.ship_imo)
        self.ship_configs = self.configuration.get_ship_configs()
        self.main_db = self.configuration.get_main_data()
        # maindb = self.main_db.find({"ship_imo": self.ship_imo}).sort('processed_daily_data.rep_dt.processed', ASCENDING)
        close_by_date = self.configuration.create_maindb_according_to_duration(self.duration)
        # short_name_dict = self.configuration.create_short_names_dictionary()
        # newvariables = [short_name_dict[i].strip() for i in self.variables for j in short_name_dict.keys() if i == j]
        print(self.variables)
        # stats = self.configuration.get_ship_stats(actual_maindb, *variables)
        stats = self.configuration.get_ship_stats_2(close_by_date, *self.variables)
        print(stats)
        # for i in self.variables:
        #     try:
                # step = stats[i]['Max'] - stats[i]['Min']
                # if int(step) > 10:
                #     step = 6
                # if int(step) < 5:
                #     step = 5
                # stats[i]['Step'] = 5
                # if i == 'trim' or i == 'Trim':
                #     step = 0.5
                #     stats[i]['Step'] = step
                # else:
                #     minvalue = stats[i]['Min']
                #     maxvalue = stats[i]['Max']
                #     step = (maxvalue - minvalue)
                #     if step < 10:
                #         step = step / 6
                #         new_step = self.configuration.makeDecimal(step, True)
                #         stats[i]['Step'] = new_step
                #     else:
                #         step = step / 4
                #         new_step = self.configuration.makeDecimal(step, True)
                #         stats[i]['Step'] = new_step
            # except KeyError:
            #     continue
        print("END CREATE STEPS")
        
        new_stats_dict = self.create_marks_based_on_steps(stats)
        print("END CREATE MARKS")
        # print(new_stats_dict)
        return new_stats_dict
    
    def create_marks_based_on_steps(self, stats_dict):
        tempResult = {}
        for key in stats_dict.keys():
            # tempList=np.linspace(stats_dict[key]['Min'], stats_dict[key]['Max'], stats_dict[key]['Max']-stats_dict[key]['Min'])
            if key == 'trim':
                # tempList=np.linspace(stats_dict[key]['Min'], stats_dict[key]['Max'], 6)
                tempResult[key] = [-0.2, -0.1, 0, 0.1, 0.2, 0.4]
            else:
                tempList=np.linspace(stats_dict[key]['Min'], stats_dict[key]['Max'], 6)
                tempResult[key] = tempList
            print(tempList)
            if key == 'trim':
                stats_dict[key]['Step'] = 0.1
            else:
                stats_dict[key]['Step'] = len(tempList) / 2

        for key in tempResult.keys():
            tempDict={}
            if key == 'trim':
                for i in tempResult[key]:
                    tempDict.update({str(i): str(i)})
                stats_dict[key]["Marks"] = tempDict
            else:
                for i in tempResult[key]:
                    tempDict.update({str(int(i)): str(int(i))})
                stats_dict[key]["Marks"] = tempDict
            # for i in np.linspace(stats_dict[key]['Min'], stats_dict[key]['Max'], stats_dict[key]['Step']):
            #     # if i<= math.floor(stats_dict[key]['Max']):
            #     #     a = i + stats_dict[key]['Step']
            #     #     new_a = self.configuration.makeDecimal(a,True)
            #     #     tempDict.update({str(int(new_a)): str(int(new_a))})
            #     tempDict.update({str(int(i)): str(int(i))})
            # stats_dict[key]['Marks'] = tempDict
        
        return stats_dict