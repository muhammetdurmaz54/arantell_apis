import sys
from time import process_time_ns
# sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.processors.config_extractor.configurator import Configurator
from src.helpers.check_status import check_status
from flask import request
from pymongo import DESCENDING, MongoClient, ASCENDING
from bson.json_util import dumps, loads
import datetime
import ast
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import re
import pprint
import numpy as np
import time
import pandas as pd

log = CommonLogger(__name__,debug=True).setup_logger()
# df = pd.read_excel("D:/Internship/Helper Files/ConfiguratorRev_04 A.xlsx", sheet_name="Groups", engine="openpyxl", header=[0, 1], index_col=[0])
# a = df.columns.get_level_values(0).to_series()
# b = a.mask(a.str.startswith('Unnamed')).ffill().fillna('')
# df.columns = [b, df.columns.get_level_values(1)]

spe_limit_global = 1/0.8
t2_limit_global = 1/0.65
ewma_limit_global = 1


class TrendsExtractor():
    def __init__(self, ship_imo, group, duration, individual_params, include_outliers='false', compare='None', anomalies='true', noonorlogs="noon"):
        self.ship_imo = int(ship_imo)
        self.group = group
        self.duration = duration
        self.include_outliers = include_outliers
        self.individual_params = individual_params
        self.compare = compare
        self.anomalies = anomalies
        self.noonorlogs = noonorlogs
        self.groupsList = None
        self.corrected = None
        self.db_aranti = None
        self.db_test = None
        self.error = False
        self.traceback_msg = None

    def do_steps(self):
        # self.connect()
        # self.read_data()
        # if self.group == '':
        #     a = self.process_data()
        #     return a
        # else:
        if 'Multi Axis' not in self.group:
            if self.include_outliers == 'true':
                if 'Lastyear' in self.duration:
                    a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, q = self.process_data()
                    return a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, q
                else:
                    a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r = self.process_data()
                    return a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r
            else:
                if 'Lastyear' in self.duration:
                    a, b, c, d, e, g, h, i, j, k, l, m, n, o, p, q, r, s, t = self.process_data()
                    return a, b, c, d, e, g, h, i, j, k, l, m, n, o, p, q, r, s, t
                else:
                    a, b, c, d, e, g, h, i, j, k, l, m, n, o, p, q = self.process_data()
                    return a, b, c, d, e, g, h, i, j, k, l, m, n, o, p, q
        if 'Multi Axis' in self.group:
            a, b, c, d, e, f, g, h, i, j, k, l = self.process_data()
            return a, b, c, d, e, f, g, h, i, j, k, l
    
    @check_status
    def connect(self):
        ''' Uncomment Later
        self.db = connect_db()
        '''
        # Remove later
        # client = MongoClient("mongodb://localhost:27017/")
        # self.db_aranti = client['aranti']
        # self.db_test = client['test_db']
        # return db_aranti, db_test
    
    # Function to get the range of dates according to the specified duration
    # def get_duration(self, duration):
    #     '''Get the date in the most recent document after sorting it and limiting the number 
    #         of dates to 1
    #     '''
    #     startDate = self.db_test.Main_db.find(
    #         {},
    #         {
    #             "processed_daily_data.rep_dt.processed": 1
    #         }
    #     ).sort("processed_daily_data.rep_dt.processed", DESCENDING).limit(1)

    #     ''' Regular Expression to check if duration matches the format "number+string"
    #         Returns a tuple in the form (number, string)
    #     '''
    #     temp = re.compile("([0-9]+)([a-zA-Z]+)")
    #     dur = temp.match(duration).groups()
    #     # for i in startDate[0]['processed_daily_data']:
    #     #     print(i)
    #     print(startDate[0]['processed_daily_data']['rep_dt']['processed'])
    #     start_Date = startDate[0]['processed_daily_data']['rep_dt']['processed'].replace(tzinfo=timezone.utc)
    #     start_Date = start_Date.timestamp()
    #     print(start_Date)

    #     if "Days" in dur:
    #         end = datetime.utcfromtimestamp(start_Date) - timedelta(days=int(dur[0]))
    #     elif "Year" in dur:
    #         '''Using relativedelta takes care of leap years'''
    #         end = datetime.utcfromtimestamp(start_Date) - relativedelta(years=int(dur[0]))
    #     print("end",end)
    #     end = end.replace(tzinfo = timezone.utc)
    #     endDate = end.timestamp()
    #     print(endDate)

    #     return startDate[0]['processed_daily_data']['rep_dt']['processed'], end
    
  
    # def read_data(self):
    #     ''' Initializing the Configurator instance and running get_ship_configs 
    #         to connect to the database
    #     '''
    #     self.configuration = Configurator(self.ship_imo)
    #     self.ship_configs = self.configuration.get_ship_configs()
    #     self.main_db = self.configuration.get_main_data()

    #     mainres = []
    #     print("START DATE FROM MAINDB")
    #     res = self.main_db.find(
    #         {
    #             'ship_imo': self.ship_imo
    #         },
    #         {
    #             "processed_daily_data.rep_dt.processed": 1,
    #             "_id": 0
    #         }
    #     ).sort("processed_daily_data.rep_dt.processed", ASCENDING)
    #     mainres.append(res)
    #     print("END DATE FROM MAINDB")
    #     print("START GET GROUPS LIST")
    #     if self.group == "" and self.individual_params != "":
    #         self.groupsList = self.configuration.get_group_selection_for_individual_parameters(self.individual_params)
    #     if self.group != "" and self.individual_params == "":
    #         self.groupsList = self.configuration.get_group_selection(self.group)
    #     if self.group != "" and self.individual_params != "":
    #         self.groupsList = self.configuration.get_group_selection_for_individual_parameters(self.individual_params, self.group)
    #     # print(self.individual)
    #     print("END GET GROUPS LIST")
    
    #     '''For getting the data of all the variables in the group list'''
    #     # for vars in singledata.keys():
    #     #     grps = singledata[vars]['group_selection']
    #     print("START GET DATA ACC TO GROUP")
    #     for grp in self.groupsList:
    #         # if grp['groupname'] == self.group:                    
    #         res = self.main_db.find(
    #             {
    #                 'ship_imo': self.ship_imo
    #             },
    #             {
    #                 "ship_imo": 1,
    #                 "processed_daily_data."+grp['name']: 1,
    #                 "_id": 0
    #             }
    #         ).sort("processed_daily_data.rep_dt.processed", ASCENDING) # Sort dates in the ascending order
    #         mainres.append(res)

    #         ''' FOR TEMPORARY PURPOSES'''
    #         if 'Multi Axis' in self.group:
    #             if grp['groupname'] == 'COMBUSTION PROCESS':
    #                 # if 'Combst' in singledata[vars]['short_names'] and '1' in singledata[vars]['short_names']:
    #                 if 'Combst' in grp['short_names'] and '1' in grp['short_names']:
    #                     res = self.main_db.find(
    #                         {
    #                             'ship_imo': self.ship_imo
    #                         },
    #                         {
    #                             "ship_imo": 1,
    #                             "processed_daily_data."+grp['name']: 1,
    #                             "_id": 0
    #                         }
    #                     ).sort("processed_daily_data.rep_dt.processed", ASCENDING) # Sort dates in the ascending order
    #                     mainres.append(res)
    #                 # if 'ME Exh Temp' in singledata[vars]['short_names'] and '1' in singledata[vars]['short_names']:
    #                 if 'ME Exh Temp' in grp['short_names'] and '2' in grp['short_names']:
    #                     res = self.main_db.find(
    #                         {
    #                             'ship_imo': self.ship_imo
    #                         },
    #                         {
    #                             "ship_imo": 1,
    #                             "processed_daily_data."+grp['name']: 1,
    #                             "_id": 0
    #                         }
    #                     ).sort("processed_daily_data.rep_dt.processed", ASCENDING) # Sort dates in the ascending order
    #                     mainres.append(res)
    #     print("END GET DATA ACC TO GROUP")
                        

    #     self.maindb_res = mainres
    
    def process_data(self):

        self.configuration = Configurator(self.ship_imo)
        self.ship_configs = self.configuration.get_ship_configs()
        self.main_db = self.configuration.get_main_data()

        if self.group == "" and len(self.individual_params) > 1:
            self.groupsList = self.configuration.get_group_selection_for_individual_parameters(self.individual_params)
        if self.group != "" and len(self.individual_params) == 1:
            self.groupsList = self.configuration.get_group_selection(self.group)
        if self.group != "" and len(self.individual_params) > 1:
            self.groupsList = self.configuration.get_group_selection_for_individual_parameters(self.individual_params, self.group)
        print(self.groupsList)

        subgroup_dict = self.configuration.get_subgroup_names("group_"+self.group)

        print("START CURRENT DURATION")
        durationActual = self.configuration.create_current_duration(self.duration)
        print("END CURRENT DURATION")
        datadict = {}
        expecteddict = {}
        lyexpecteddict = {}
        lowerdict = {}
        upperdict = {}
        lylowerdict = {}
        lyupperdict = {}
        outlierdict = {}
        spedict = {}
        spelimitdict = {}
        t2dict = {}
        t2limitdict = {}
        ewmadict = {}
        ewmalimitdict = {}
        spe_number_dict = {}
        t2_number_dict = {}
        ewma_number_dict = {}
        spe_messages_dict = {}
        outlier_messages_dict = {}
        # new_duration = durationActual.replace('ly_','') if 'ly_' in durationActual else durationActual # Remove when Last year duration added in DB.
        new_duration = durationActual
        loaded_ballast_list = self.configuration.get_shapes_for_loaded_ballast_data(self.noonorlogs)
        # dict_of_issues, issuesCount = self.configuration.create_dict_of_issues('')
        # dict_of_issues = {}
        if('Multi Axis' in self.group):
            ''' TEMPORARY PURPOSES'''
            number_of_the_unit = self.group.replace('Multi Axis - Unit ', '')
            print("UNIT!!!!!!!", number_of_the_unit)
            group = self.configuration.temporary_group_selection_for_multi_axis(number_of_the_unit)
            print("GROUP!!!!!!!",group)
            nameslist = self.configuration.temporary_dict_and_list_according_to_groups(group)
            short_names_list = self.configuration.get_short_names_list_for_multi_axis(number_of_the_unit)
            for name in nameslist:
                newlist=[]
                explist=[]
                upperlist=[]
                lowerlist=[]
                print("NAME!!!!!", name)
                for doc in self.main_db.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'final_rep_dt': 1, 'processed_daily_data.'+name: 1, '_id': 0}).sort('final_rep_dt', ASCENDING):
                    if name != 'rep_dt':
                        try:
                            newlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['processed']))
                        except TypeError:
                            newlist.append(None)
                        try:
                            explist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][1]))
                        except TypeError:
                            explist.append(None)
                        try:
                            upperlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][2]))
                        except TypeError:
                            upperlist.append(None)
                        try:
                            lowerlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][0]))
                        except TypeError:
                            lowerlist.append(None)
                    else:
                        # newdate = doc['processed_daily_data'][name]['processed'].strftime("%Y-%m-%d")
                        newdate = doc['final_rep_dt'].strftime("%Y-%m-%d %H:%M:%S")
                        newlist.append(newdate)
                        explist.append(newdate)
                datadict[name] = newlist
                expecteddict['expected_'+name] = explist
                lowerdict[name] = lowerlist
                upperdict[name] = upperlist
            print(datadict, expecteddict, lowerdict, upperdict)
            for k in datadict.copy():
                if datadict[k] == []:
                    del datadict[k]
                try:
                    if all(x is None for x in datadict[k]):
                        del datadict[k]
                except KeyError:
                    continue
            for k in expecteddict.copy():
                if expecteddict[k] == []:
                    del expecteddict[k]
                try:
                    if all(x is None for x in expecteddict[k]):
                        del expecteddict[k]
                except KeyError:
                    continue
            for k in lowerdict.copy():
                if lowerdict[k] == []:
                    del lowerdict[k]
                try:
                    if all(x is None for x in lowerdict[k]):
                        del lowerdict[k]
                except KeyError:
                    continue
            for k in upperdict.copy():
                if upperdict[k] == []:
                    del upperdict[k]
                try:
                    if all(x is None for x in upperdict[k]):
                        del upperdict[k]
                except KeyError:
                    continue
            fuel_consumption = self.process_fuel_consumption()
            new_group = []
            new_short_names_list = []
            for param in datadict.keys():
                for grp in group:
                    if param == grp['name']:
                        new_group.append(grp)
                for name in short_names_list:
                    if name == grp['short_names'] and name not in new_short_names_list:
                        new_short_names_list.append(name)
            
            chart_height = self.get_chart_height(new_group)
            variables_list = self.configuration.get_variables_list_in_order_of_blocks(new_group)
            return datadict, expecteddict, lowerdict, upperdict, new_short_names_list, loaded_ballast_list, fuel_consumption, new_group, chart_height, variables_list
        else:
            print("START DICT AND LIST ACC TO GROUPS")
            if self.group == "" and self.individual_params != "":
                nameslist = self.individual_params
            if self.group != "" and self.individual_params == "":
                paramsList, indicesList = self.configuration.get_dict_and_list_according_to_groups(self.groupsList)
            if self.group != "" and self.individual_params != "":
                paramsList, indicesList = self.configuration.get_dict_and_list_according_to_groups(self.groupsList)
            
            print("END DICT AND LIST ACC TO GROUPS")
            # group = self.configuration.get_group_selection(self.group)
            # dependent_parameters = self.configuration.get_dependent_parameters()
            
            for name in paramsList:
                tempList=[]
                tempListForOutliers=[]
                for doc in self.main_db.find({'ship_imo': self.ship_imo}, {'processed_daily_data.'+name: 1, 'Logs': 1}).sort('final_rep_dt', ASCENDING):
                    # newdate = doc['processed_daily_data'][name]['processed'].strftime("%Y-%m-%d")
                    if name != 'rep_dt':
                        if self.anomalies == 'true':
                            if self.noonorlogs == 'noon':
                                if doc['Logs'] == False:
                                    if 'spe_messages' in doc['processed_daily_data'][name].keys() and new_duration in doc['processed_daily_data'][name]['spe_messages'].keys():
                                        # spe_messages_dict[name] = doc['processed_daily_data'][name]['spe_messages'][new_duration][2]
                                        if doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][2] == False:
                                            tempList.append(doc['processed_daily_data'][name]['spe_messages'][new_duration][2])
                                        elif doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][1] == False:
                                            tempList.append(doc['processed_daily_data'][name]['spe_messages'][new_duration][1])
                                        elif doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][0] == False:
                                            tempList.append(doc['processed_daily_data'][name]['spe_messages'][new_duration][0])
                                        else:
                                            tempList.append(None)
                                    else:
                                        # spe_messages_dict[name] = None
                                        tempList.append(None)
                            elif self.noonorlogs == 'logs':
                                if doc['Logs'] == True:
                                    if 'spe_messages' in doc['processed_daily_data'][name].keys() and new_duration in doc['processed_daily_data'][name]['spe_messages'].keys():
                                        # spe_messages_dict[name] = doc['processed_daily_data'][name]['spe_messages'][new_duration][2]
                                        if doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][2] == False:
                                            tempList.append(doc['processed_daily_data'][name]['spe_messages'][new_duration][2])
                                        elif doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][1] == False:
                                            tempList.append(doc['processed_daily_data'][name]['spe_messages'][new_duration][1])
                                        elif doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][0] == False:
                                            tempList.append(doc['processed_daily_data'][name]['spe_messages'][new_duration][0])
                                        else:
                                            tempList.append(None)
                                    else:
                                        # spe_messages_dict[name] = None
                                        tempList.append(None)
                        if self.include_outliers == 'true':
                            if self.noonorlogs == 'noon':
                                if doc['Logs'] == False:
                                    if 'within_outlier_limits' in doc['processed_daily_data'][name] and new_duration in doc['processed_daily_data'][name]['within_outlier_limits'] and 'outlier_limit_msg' in doc['processed_daily_data'][name] and new_duration in doc['processed_daily_data'][name]['outlier_limit_msg']:
                                        if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                            tempListForOutliers.append(doc['processed_daily_data'][name]['outlier_limit_msg'][new_duration])
                                        else:
                                            tempListForOutliers.append(None)
                                    else:
                                        tempListForOutliers.append(None)
                            elif self.noonorlogs == 'logs':
                                if doc['Logs'] == True:
                                    if 'within_outlier_limits' in doc['processed_daily_data'][name] and new_duration in doc['processed_daily_data'][name]['within_outlier_limits'] and 'outlier_limit_msg' in doc['processed_daily_data'][name] and new_duration in doc['processed_daily_data'][name]['outlier_limit_msg']:
                                        if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                            tempListForOutliers.append(doc['processed_daily_data'][name]['outlier_limit_msg'][new_duration])
                                        else:
                                            tempListForOutliers.append(None)
                                    else:
                                        tempListForOutliers.append(None)
                spe_messages_dict[name] = tempList
                outlier_messages_dict[name] = tempListForOutliers
            for name in indicesList:
                print("INDICES NAME!!!!!!!!!!!", name)
                tempList = []
                for doc in self.main_db.find({'ship_imo': self.ship_imo}, {'independent_indices.'+name: 1, 'Logs': 1}).sort('final_rep_dt', ASCENDING):
                    if name != 'rep_dt':
                        if self.anomalies == 'true':
                            if self.noonorlogs == 'noon':
                                if doc['Logs'] == False:
                                    try:
                                        if name in doc['independent_indices'] and 'spe_messages' in doc['independent_indices'][name].keys() and new_duration in doc['independent_indices'][name]['spe_messages'].keys():
                                            # spe_messages_dict[name] = doc['independent_indices'][name]['spe_messages'][new_duration][2]
                                            if doc['independent_indices'][name]['is_not_spe_anamolous'][durationActual][2] == False:
                                                tempList.append(doc['independent_indices'][name]['spe_messages'][new_duration][2])
                                            elif doc['independent_indices'][name]['is_not_spe_anamolous'][durationActual][1] == False:
                                                tempList.append(doc['independent_indices'][name]['spe_messages'][new_duration][1])
                                            elif doc['independent_indices'][name]['is_not_spe_anamolous'][durationActual][0] == False:
                                                tempList.append(doc['independent_indices'][name]['spe_messages'][new_duration][0])
                                            else:
                                                tempList.append(None)
                                        else:
                                            # spe_messages_dict[name] = None
                                            tempList.append(None)
                                    except:
                                        tempList.append(None)
                            elif self.noonorlogs == 'logs':
                                if doc['Logs'] == True:
                                    try:
                                        if name in doc['independent_indices'] and 'spe_messages' in doc['independent_indices'][name].keys() and new_duration in doc['independent_indices'][name]['spe_messages'].keys():
                                            # spe_messages_dict[name] = doc['independent_indices'][name]['spe_messages'][new_duration][2]
                                            if doc['independent_indices'][name]['is_not_spe_anamolous'][durationActual][2] == False:
                                                tempList.append(doc['independent_indices'][name]['spe_messages'][new_duration][2])
                                            elif doc['independent_indices'][name]['is_not_spe_anamolous'][durationActual][1] == False:
                                                tempList.append(doc['independent_indices'][name]['spe_messages'][new_duration][1])
                                            elif doc['independent_indices'][name]['is_not_spe_anamolous'][durationActual][0] == False:
                                                tempList.append(doc['independent_indices'][name]['spe_messages'][new_duration][0])
                                            else:
                                                tempList.append(None)
                                        else:
                                            # spe_messages_dict[name] = None
                                            tempList.append(None)
                                    except:
                                        tempList.append(None)
                spe_messages_dict[name] = tempList
            groups = self.groupsList
            print("START LIST")
            for name in paramsList:
                if name != 'rep_dt':
                    try:
                        cursor_temp_spe = self.ship_configs.find({'ship_imo': self.ship_imo}, {'spe_limits.'+name: 1, 't2_limits.'+name+'.zero_two': 1, 'ewma_limits.'+name: 1})
                        temp_spe = loads(dumps(cursor_temp_spe))
                        print("TEMP SPE", temp_spe)
                        spe_number_dict[name] = temp_spe[0]['spe_limits'][name][durationActual]['zero_zero_five']
                        t2_number_dict[name] = temp_spe[0]['t2_limits'][name]['zero_two']
                        ewma_number_dict[name] = temp_spe[0]['ewma_limits'][name][durationActual][2][2]
                    except KeyError:
                        continue
                    except TypeError:
                        continue
                # except TypeError:
                #     continue
            for name in indicesList:
                try:
                    cursor_temp_spe_indices = self.ship_configs.find({'ship_imo': self.ship_imo}, {'spe_limits_indices.'+name: 1, 't2_limits_indices.'+name+'.zero_two': 1, 'mewma_limits.'+name: 1})
                    temp_spe_indices = loads(dumps(cursor_temp_spe_indices))
                    spe_number_dict[name] = temp_spe_indices[0]['spe_limits_indices'][name][durationActual]['zero_zero_five']
                    t2_number_dict[name] = temp_spe_indices[0]['t2_limits_indices'][name]['zero_two']
                    ewma_number_dict[name] = temp_spe_indices[0]['mewma_limits'][name][durationActual][2]
                except KeyError:
                    continue
                except TypeError:
                    continue
            print("SPE & T2", spe_number_dict, t2_number_dict, ewma_number_dict)

            # INDEPENDENT INDICES------------------------------------------
            for name in indicesList:
                newlist=[]
                # explist=[]
                # upperlist=[]
                # lowerlist=[]
                # outlierlist=[]
                # lyexplist=[]
                # lylowerlist=[]
                # lyupperlist=[]
                # spe_list=[]
                outlierspe_list = []
                spe_limit_list = []
                outlierspe_limit_list = []
                t2_list = []
                ewma_list = []
                outliert2_list = []
                t2_limit_list = []
                ewma_limit_list = []
                outliert2_limit_list = []    
                # try:
                for doc in self.main_db.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'historical': 1, 'Logs': 1, 'independent_indices': 1, 'spe_limits_indices': 1, 't2_limits_indices': 1, 'mewma_limits': 1, '_id': 0}).sort('final_rep_dt', ASCENDING):
                    if name in doc['independent_indices'].keys():
                        if self.anomalies == 'false':
                            try:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_spe = self.configuration.spe_divide(doc['independent_indices'][name]['SPEy'][durationActual], spe_number_dict[name])
                                            if doc['independent_indices'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                newlist.append(self.configuration.makeDecimal(new_spe))
                                                # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                # spe_list.append(spe_number_dict[name])
                                            else:
                                                newlist.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_spe = self.configuration.spe_divide(doc['independent_indices'][name]['SPEy'][durationActual], spe_number_dict[name])
                                            if doc['independent_indices'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                newlist.append(self.configuration.makeDecimal(new_spe))
                                                # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                # spe_list.append(spe_number_dict[name])
                                            else:
                                                newlist.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_spe = self.configuration.spe_divide(doc['independent_indices'][name]['SPEy'][durationActual], doc['spe_limits_indices'][name][durationActual]['zero_zero_five'])
                                            if doc['independent_indices'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                newlist.append(self.configuration.makeDecimal(new_spe))
                                                # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                # spe_list.append(spe_number_dict[name])
                                            else:
                                                newlist.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_spe = self.configuration.spe_divide(doc['independent_indices'][name]['SPEy'][durationActual], doc['spe_limits_indices'][name][durationActual]['zero_zero_five'])
                                            if doc['independent_indices'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                newlist.append(self.configuration.makeDecimal(new_spe))
                                                # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                # spe_list.append(spe_number_dict[name])
                                            else:
                                                newlist.append(None)
                            except KeyError:
                                newlist.append(None)
                            except TypeError:
                                newlist.append(None)
                            except IndexError:
                                newlist.append(None)
                            
                            try:
                                # if doc['historical'] == True:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['independent_indices'][name]['is_not_spe_anamolous'][durationActual][2] == True:
                                            spe_limit_list.append(self.configuration.makeDecimal(spe_limit_global))
                                            # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                            # spe_limit_list.append(spe_number_dict[name])
                                        else:
                                            spe_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['independent_indices'][name]['is_not_spe_anamolous'][durationActual][2] == True:
                                            spe_limit_list.append(self.configuration.makeDecimal(spe_limit_global))
                                            # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                            # spe_limit_list.append(spe_number_dict[name])
                                        else:
                                            spe_limit_list.append(None)
                            except KeyError:
                                spe_limit_list.append(None)
                            except TypeError:
                                spe_limit_list.append(None)
                            except IndexError:
                                spe_limit_list.append(None)
                            
                            #T2----------------------------------------------------------------------
                            try:
                                # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual][0] == True:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_t2 = self.configuration.t2_divide(doc['independent_indices'][name]['t2_initial'][durationActual], t2_number_dict[name])
                                            if doc['independent_indices'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                t2_list.append(self.configuration.makeDecimal(new_t2))
                                                # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                            else:
                                                t2_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_t2 = self.configuration.t2_divide(doc['independent_indices'][name]['t2_initial'][durationActual], t2_number_dict[name])
                                            if doc['independent_indices'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                t2_list.append(self.configuration.makeDecimal(new_t2))
                                                # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                            else:
                                                t2_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_t2 = self.configuration.t2_divide(doc['independent_indices'][name]['t2_initial'][durationActual], doc['t2_limits_indices'][name]['zero_two'])
                                            if doc['independent_indices'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                t2_list.append(self.configuration.makeDecimal(new_t2))
                                                # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                            else:
                                                t2_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_t2 = self.configuration.t2_divide(doc['independent_indices'][name]['t2_initial'][durationActual], doc['t2_limits_indices'][name]['zero_two'])
                                            if doc['independent_indices'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                t2_list.append(self.configuration.makeDecimal(new_t2))
                                                # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                            else:
                                                t2_list.append(None)
                            except KeyError:
                                t2_list.append(None)
                            except TypeError:
                                t2_list.append(None)
                            except IndexError:
                                t2_list.append(None)
                            
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['independent_indices'][name]['is_not_t2_anamolous'][durationActual][0] == True:
                                            t2_limit_list.append(self.configuration.makeDecimal(t2_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # t2_limit_list.append(t2_number_dict[name])
                                        else:
                                            t2_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['independent_indices'][name]['is_not_t2_anamolous'][durationActual][0] == True:
                                            t2_limit_list.append(self.configuration.makeDecimal(t2_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # t2_limit_list.append(t2_number_dict[name])
                                        else:
                                            t2_limit_list.append(None)
                            except KeyError:
                                t2_limit_list.append(None)
                            except TypeError:
                                t2_limit_list.append(None)
                            except IndexError:
                                t2_limit_list.append(None)
                            #EWMA-----------------------------------------------------------------------
                            try:
                                # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual][0] == True:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_ewma = self.configuration.ewma_divide(doc['independent_indices'][name]['mewma_val'][durationActual], ewma_number_dict[name])
                                            if doc['independent_indices'][name]['mewma_val'][durationActual] < ewma_number_dict[name]:
                                                ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                            else:
                                                ewma_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_ewma = self.configuration.ewma_divide(doc['independent_indices'][name]['mewma_val'][durationActual], ewma_number_dict[name])
                                            if doc['independent_indices'][name]['mewma_val'][durationActual] < ewma_number_dict[name]:
                                                ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                            else:
                                                ewma_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_ewma = self.configuration.ewma_divide(doc['independent_indices'][name]['mewma_val'][durationActual], doc['mewma_limits'][name][durationActual][2])
                                            if doc['independent_indices'][name]['mewma_val'][durationActual] < ewma_number_dict[name]:
                                                ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                            else:
                                                ewma_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_ewma = self.configuration.ewma_divide(doc['independent_indices'][name]['mewma_val'][durationActual], doc['mewma_limits'][name][durationActual][2])
                                            if doc['independent_indices'][name]['mewma_val'][durationActual] < ewma_number_dict[name]:
                                                ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                            else:
                                                ewma_list.append(None)
                            except KeyError:
                                ewma_list.append(None)
                            except TypeError:
                                ewma_list.append(None)
                            except IndexError:
                                ewma_list.append(None)
                            
                            try:
                                # if doc['historical'] == True:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['independent_indices'][name]['is_not_mewma_anamolous'][durationActual][2] == True:
                                            ewma_limit_list.append(self.configuration.makeDecimal(ewma_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # ewma_limit_list.append(ewma_number_dict[name])
                                        else:
                                            ewma_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['independent_indices'][name]['is_not_mewma_anamolous'][durationActual][2] == True:
                                            ewma_limit_list.append(self.configuration.makeDecimal(ewma_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # ewma_limit_list.append(ewma_number_dict[name])
                                        else:
                                            ewma_limit_list.append(None)
                            except KeyError:
                                ewma_limit_list.append(None)
                            except TypeError:
                                ewma_limit_list.append(None)
                            except IndexError:
                                ewma_limit_list.append(None)
                        
                        if self.anomalies == 'true':
                            try:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_spe = self.configuration.spe_divide(doc['independent_indices'][name]['SPEy'][durationActual], spe_number_dict[name])
                                            # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                            newlist.append(self.configuration.makeDecimal(new_spe))
                                                # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                # spe_list.append(spe_number_dict[name])
                                            # else:
                                            #     spe_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_spe = self.configuration.spe_divide(doc['independent_indices'][name]['SPEy'][durationActual], spe_number_dict[name])
                                            # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                            newlist.append(self.configuration.makeDecimal(new_spe))
                                                # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                # spe_list.append(spe_number_dict[name])
                                            # else:
                                            #     spe_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_spe = self.configuration.spe_divide(doc['independent_indices'][name]['SPEy'][durationActual], doc['spe_limits_indices'][name][durationActual]['zero_zero_five'])
                                            # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                            newlist.append(self.configuration.makeDecimal(new_spe))
                                                # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                # spe_list.append(spe_number_dict[name])
                                            # else:
                                            #     spe_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_spe = self.configuration.spe_divide(doc['independent_indices'][name]['SPEy'][durationActual], doc['spe_limits_indices'][name][durationActual]['zero_zero_five'])
                                            # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                            newlist.append(self.configuration.makeDecimal(new_spe))
                                                # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                # spe_list.append(spe_number_dict[name])
                                            # else:
                                            #     spe_list.append(None)
                            except KeyError:
                                newlist.append(None)
                            except TypeError:
                                newlist.append(None)
                            except IndexError:
                                newlist.append(None)
                            
                            try:
                                # if doc['historical'] == True:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        # if doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][2] == True:
                                        spe_limit_list.append(self.configuration.makeDecimal(spe_limit_global))
                                            # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                            # spe_limit_list.append(spe_number_dict[name])
                                        # else:
                                        #     spe_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        # if doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][2] == True:
                                        spe_limit_list.append(self.configuration.makeDecimal(spe_limit_global))
                                            # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                            # spe_limit_list.append(spe_number_dict[name])
                                        # else:
                                        #     spe_limit_list.append(None)
                            except KeyError:
                                spe_limit_list.append(None)
                            except TypeError:
                                spe_limit_list.append(None)
                            except IndexError:
                                spe_limit_list.append(None)
                            
                            #T2----------------------------------------------------------------------
                            try:
                                # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual][0] == True:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_t2 = self.configuration.t2_divide(doc['independent_indices'][name]['t2_initial'][durationActual], t2_number_dict[name])
                                            # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                            t2_list.append(self.configuration.makeDecimal(new_t2))
                                                # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                            # else:
                                            #     t2_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_t2 = self.configuration.t2_divide(doc['independent_indices'][name]['t2_initial'][durationActual], t2_number_dict[name])
                                            # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                            t2_list.append(self.configuration.makeDecimal(new_t2))
                                                # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                            # else:
                                            #     t2_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_t2 = self.configuration.t2_divide(doc['independent_indices'][name]['t2_initial'][durationActual], doc['t2_limits_indices'][name]['zero_two'])
                                            # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                            t2_list.append(self.configuration.makeDecimal(new_t2))
                                                # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                            # else:
                                            #     t2_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_t2 = self.configuration.t2_divide(doc['independent_indices'][name]['t2_initial'][durationActual], doc['t2_limits_indices'][name]['zero_two'])
                                            # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                            t2_list.append(self.configuration.makeDecimal(new_t2))
                                                # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                            # else:
                                            #     t2_list.append(None)
                            except KeyError:
                                t2_list.append(None)
                            except TypeError:
                                t2_list.append(None)
                            except IndexError:
                                t2_list.append(None)
                            
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        # if doc['processed_daily_data'][name]['is_not_t2_anamolous'][durationActual][0] == True:
                                        t2_limit_list.append(self.configuration.makeDecimal(t2_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # t2_limit_list.append(t2_number_dict[name])
                                        # else:
                                        #     t2_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        # if doc['processed_daily_data'][name]['is_not_t2_anamolous'][durationActual][0] == True:
                                        t2_limit_list.append(self.configuration.makeDecimal(t2_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # t2_limit_list.append(t2_number_dict[name])
                                        # else:
                                        #     t2_limit_list.append(None)
                            except KeyError:
                                t2_limit_list.append(None)
                            except TypeError:
                                t2_limit_list.append(None)
                            except IndexError:
                                t2_limit_list.append(None)
                            #EWMA-----------------------------------------------------------------------
                            try:
                                # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual][0] == True:
                                # print("MEWMA!!!!!!!", doc['independent_indices'][name]['mewma_val'][durationActual], ewma_number_dict[name])
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_ewma = self.configuration.ewma_divide(doc['independent_indices'][name]['mewma_val'][durationActual], ewma_number_dict[name])
                                            # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                            ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                            # else:
                                            #     ewma_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_ewma = self.configuration.ewma_divide(doc['independent_indices'][name]['mewma_val'][durationActual], ewma_number_dict[name])
                                            # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                            ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                            # else:
                                            #     ewma_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_ewma = self.configuration.ewma_divide(doc['independent_indices'][name]['mewma_val'][durationActual], doc['mewma_limits'][name][durationActual][2])
                                            # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                            ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                            # else:
                                            #     ewma_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_ewma = self.configuration.ewma_divide(doc['independent_indices'][name]['mewma_val'][durationActual], doc['mewma_limits'][name][durationActual][2])
                                            # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                            ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                            # else:
                                            #     ewma_list.append(None)
                            except KeyError:
                                ewma_list.append(None)
                            except TypeError:
                                ewma_list.append(None)
                            except IndexError:
                                ewma_list.append(None)
                            
                            try:
                                # if doc['historical'] == True:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        # if doc['processed_daily_data'][name]['is_not_ewma_anamolous'][durationActual][2] == True:
                                        ewma_limit_list.append(self.configuration.makeDecimal(ewma_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # ewma_limit_list.append(ewma_number_dict[name])
                                        # else:
                                        #     ewma_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        # if doc['processed_daily_data'][name]['is_not_ewma_anamolous'][durationActual][2] == True:
                                        ewma_limit_list.append(self.configuration.makeDecimal(ewma_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # ewma_limit_list.append(ewma_number_dict[name])
                                        # else:
                                        #     ewma_limit_list.append(None)
                            except KeyError:
                                ewma_limit_list.append(None)
                            except TypeError:
                                ewma_limit_list.append(None)
                            except IndexError:
                                ewma_limit_list.append(None)
                # except KeyError:
                #     continue
                # print("MEWMA!!!!!!!", name, ewma_list)
                datadict[name] = newlist
                # expecteddict['expected_'+name] = explist
                # lyexpecteddict['lyexpected_'+name] = lyexplist
                # ewmadict['ewma_'+name] = ewma_list
                spelimitdict[name] = spe_limit_list
                ewmadict[name] = ewma_list
                ewmalimitdict[name] = ewma_limit_list
                t2dict[name] = t2_list
                t2limitdict[name] = t2_limit_list
                # lowerdict[name] = lowerlist
                # upperdict[name] = upperlist
                # lylowerdict[name] = lylowerlist
                # lyupperdict[name] = lyupperlist
                # outlierdict[name] = outlierlist
            #PARAMETERS-----------------------------------------------
            for name in paramsList:
                newlist=[]
                explist=[]
                upperlist=[]
                lowerlist=[]
                outlierlist=[]
                lyexplist=[]
                lylowerlist=[]
                lyupperlist=[]
                spe_list=[]
                outlierspe_list = []
                spe_limit_list = []
                outlierspe_limit_list = []
                t2_list = []
                ewma_list = []
                outliert2_list = []
                t2_limit_list = []
                ewma_limit_list = []
                outliert2_limit_list = []
                # outlierColorList=[]
                # opacityList=[]
                # if name != 'rep_dt':
                # print(name)
                # spe_limit_number = self.ship_configs.find({'ship_imo': self.ship_imo}, {''})
                # try:
                for doc in self.main_db.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'historical': 1, 'Logs': 1, 'final_rep_dt': 1, 'processed_daily_data.'+name: 1, 'spe_limits': 1, 't2_limits': 1, 'ewma_limits': 1, '_id': 0}).sort('final_rep_dt', ASCENDING):
                    # try:
                    if name != 'rep_dt' and name in doc['processed_daily_data'].keys():
                        try:
                            # if doc['historical'] == True:
                            if self.noonorlogs == "noon":
                                if doc['Logs'] == False:
                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                        if pd.isnull(doc['processed_daily_data'][name]['processed']) == False:
                                            outlierlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['processed']))
                                        else:
                                            outlierlist.append(None)
                                        
                                    else:
                                        outlierlist.append(None)
                                        # outlierspe_list.append(None)
                                        # outlierspe_limit_list.append(None)
                                        # outliert2_list.append(None)
                                        # outliert2_limit_list.append(None)
                            elif self.noonorlogs == "logs":
                                if doc['Logs'] == True:
                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                        if pd.isnull(doc['processed_daily_data'][name]['processed']) == False:
                                            outlierlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['processed']))
                                        else:
                                            outlierlist.append(None)
                                        
                                    else:
                                        outlierlist.append(None)
                                        # outlierspe_list.append(None)
                                        # outlierspe_limit_list.append(None)
                                        # outliert2_list.append(None)
                                        # outliert2_limit_list.append(None)
                        except TypeError:
                            outlierlist.append(None)
                            # outlierspe_list.append(None)
                            # outlierspe_limit_list.append(None)
                            # outliert2_list.append(None)
                            # outliert2_limit_list.append(None)
                            # outlierColorList.append(None)
                            # opacityList.append(0)
                        try:
                            # if doc['historical'] == True:
                            if self.noonorlogs == "noon":
                                if doc['Logs'] == False:
                                    if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                        if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                            if pd.isnull(doc['processed_daily_data'][name]['processed']) == False:
                                                newlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['processed']))
                                            else:
                                                newlist.append(None)
                                            
                                            
                                        else:
                                            newlist.append(None)
                                    else:
                                        if pd.isnull(doc['processed_daily_data'][name]['processed']) == False:
                                                newlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['processed']))
                                        else:
                                            newlist.append(None)
                                        # spe_list.append(None)
                                        # spe_limit_list.append(None)
                                        # t2_list.append(None)
                                        # t2_limit_list.append(None)
                            elif self.noonorlogs == "logs":
                                if doc['Logs'] == True:
                                    if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                        if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                            if pd.isnull(doc['processed_daily_data'][name]['processed']) == False:
                                                newlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['processed']))
                                            else:
                                                newlist.append(None)
                                            
                                            
                                        else:
                                            newlist.append(None)
                                    else:
                                        if pd.isnull(doc['processed_daily_data'][name]['processed']) == False:
                                                newlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['processed']))
                                        else:
                                            newlist.append(None)
                                        # spe_list.append(None)
                                        # spe_limit_list.append(None)
                                        # t2_list.append(None)
                                        # t2_limit_list.append(None)
                        except TypeError:
                            newlist.append(None)
                            # spe_list.append(None)
                            # spe_limit_list.append(None)
                            # t2_list.append(None)
                            # t2_limit_list.append(None)
                        
                        if self.anomalies == 'false':
                            try:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_spe = self.configuration.spe_divide(doc['processed_daily_data'][name]['SPEy'][durationActual], spe_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                    spe_list.append(self.configuration.makeDecimal(new_spe))
                                                    # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                    # spe_list.append(spe_number_dict[name])
                                                else:
                                                    spe_list.append(None)
                                                #     else:
                                                #         spe_list.append(None)
                                                # else:
                                                #     spe_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                            spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        else:
                                                            spe_list.append(None)
                                                    else:
                                                        spe_list.append(None)
                                                else:
                                                    spe_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_spe = self.configuration.spe_divide(doc['processed_daily_data'][name]['SPEy'][durationActual], spe_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                    spe_list.append(self.configuration.makeDecimal(new_spe))
                                                    # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                    # spe_list.append(spe_number_dict[name])
                                                else:
                                                    spe_list.append(None)
                                                #     else:
                                                #         spe_list.append(None)
                                                # else:
                                                #     spe_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                            spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        else:
                                                            spe_list.append(None)
                                                    else:
                                                        spe_list.append(None)
                                                else:
                                                    spe_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_spe = self.configuration.spe_divide(doc['processed_daily_data'][name]['SPEy'][durationActual], doc['spe_limits'][name][durationActual]['zero_zero_five'])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                    spe_list.append(self.configuration.makeDecimal(new_spe))
                                                    # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                    # spe_list.append(spe_number_dict[name])
                                                else:
                                                    spe_list.append(None)
                                                #     else:
                                                #         spe_list.append(None)
                                                # else:
                                                #     spe_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                            spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        else:
                                                            spe_list.append(None)
                                                    else:
                                                        spe_list.append(None)
                                                else:
                                                    spe_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_spe = self.configuration.spe_divide(doc['processed_daily_data'][name]['SPEy'][durationActual], doc['spe_limits'][name][durationActual]['zero_zero_five'])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                    spe_list.append(self.configuration.makeDecimal(new_spe))
                                                    # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                    # spe_list.append(spe_number_dict[name])
                                                else:
                                                    spe_list.append(None)
                                                #     else:
                                                #         spe_list.append(None)
                                                # else:
                                                #     spe_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                            spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        else:
                                                            spe_list.append(None)
                                                    else:
                                                        spe_list.append(None)
                                                else:
                                                    spe_list.append(None)
                            except KeyError:
                                spe_list.append(None)
                            except TypeError:
                                spe_list.append(None)
                            except IndexError:
                                spe_list.append(None)
                            
                            try:
                                # if doc['historical'] == True:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][2] == True:
                                            spe_limit_list.append(self.configuration.makeDecimal(spe_limit_global))
                                            # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                            # spe_limit_list.append(spe_number_dict[name])
                                        else:
                                            spe_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][2] == True:
                                            spe_limit_list.append(self.configuration.makeDecimal(spe_limit_global))
                                            # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                            # spe_limit_list.append(spe_number_dict[name])
                                        else:
                                            spe_limit_list.append(None)
                            except KeyError:
                                spe_limit_list.append(None)
                            except TypeError:
                                spe_limit_list.append(None)
                            except IndexError:
                                spe_limit_list.append(None)
                            
                            #T2----------------------------------------------------------------------
                            try:
                                # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual][0] == True:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_t2 = self.configuration.t2_divide(doc['processed_daily_data'][name]['t2_initial'][durationActual], t2_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                    t2_list.append(self.configuration.makeDecimal(new_t2))
                                                    # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                else:
                                                    t2_list.append(None)
                                                #     else:
                                                #         t2_list.append(None)
                                                # else:
                                                #     t2_list.append(None)

                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                            t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        else:
                                                            t2_list.append(None)
                                                    else:
                                                        t2_list.append(None)
                                                else:
                                                    t2_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_t2 = self.configuration.t2_divide(doc['processed_daily_data'][name]['t2_initial'][durationActual], t2_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                    t2_list.append(self.configuration.makeDecimal(new_t2))
                                                    # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                else:
                                                    t2_list.append(None)
                                                #     else:
                                                #         t2_list.append(None)
                                                # else:
                                                #     t2_list.append(None)

                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                            t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        else:
                                                            t2_list.append(None)
                                                    else:
                                                        t2_list.append(None)
                                                else:
                                                    t2_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_t2 = self.configuration.t2_divide(doc['processed_daily_data'][name]['t2_initial'][durationActual], doc['t2_limits'][name]['zero_two'])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                    t2_list.append(self.configuration.makeDecimal(new_t2))
                                                    # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                else:
                                                    t2_list.append(None)
                                                #     else:
                                                #         t2_list.append(None)
                                                # else:
                                                #     t2_list.append(None)

                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                            t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        else:
                                                            t2_list.append(None)
                                                    else:
                                                        t2_list.append(None)
                                                else:
                                                    t2_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_t2 = self.configuration.t2_divide(doc['processed_daily_data'][name]['t2_initial'][durationActual], doc['t2_limits'][name]['zero_two'])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                    t2_list.append(self.configuration.makeDecimal(new_t2))
                                                    # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                else:
                                                    t2_list.append(None)
                                                #     else:
                                                #         t2_list.append(None)
                                                # else:
                                                #     t2_list.append(None)

                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                            t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        else:
                                                            t2_list.append(None)
                                                    else:
                                                        t2_list.append(None)
                                                else:
                                                    t2_list.append(None)
                            except KeyError:
                                t2_list.append(None)
                            except TypeError:
                                t2_list.append(None)
                            except IndexError:
                                t2_list.append(None)
                            
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['processed_daily_data'][name]['is_not_t2_anamolous'][durationActual][0] == True:
                                            t2_limit_list.append(self.configuration.makeDecimal(t2_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # t2_limit_list.append(t2_number_dict[name])
                                        else:
                                            t2_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['processed_daily_data'][name]['is_not_t2_anamolous'][durationActual][0] == True:
                                            t2_limit_list.append(self.configuration.makeDecimal(t2_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # t2_limit_list.append(t2_number_dict[name])
                                        else:
                                            t2_limit_list.append(None)
                            except KeyError:
                                t2_limit_list.append(None)
                            except TypeError:
                                t2_limit_list.append(None)
                            except IndexError:
                                t2_limit_list.append(None)
                            #EWMA-----------------------------------------------------------------------
                            try:
                                # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual][0] == True:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_ewma = self.configuration.ewma_divide(doc['processed_daily_data'][name]['ewma'][durationActual][2], ewma_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                    ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                    # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                else:
                                                    ewma_list.append(None)
                                                #     else:
                                                #         ewma_list.append(None)
                                                # else:
                                                #     ewma_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                            ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        else:
                                                            ewma_list.append(None)
                                                    else:
                                                        ewma_list.append(None)
                                                else:
                                                    ewma_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_ewma = self.configuration.ewma_divide(doc['processed_daily_data'][name]['ewma'][durationActual][2], ewma_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                    ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                    # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                else:
                                                    ewma_list.append(None)
                                                #     else:
                                                #         ewma_list.append(None)
                                                # else:
                                                #     ewma_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                            ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        else:
                                                            ewma_list.append(None)
                                                    else:
                                                        ewma_list.append(None)
                                                else:
                                                    ewma_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_ewma = self.configuration.ewma_divide(doc['processed_daily_data'][name]['ewma'][durationActual][2], doc['ewma_limits'][name][durationActual][2])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                    ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                    # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                else:
                                                    ewma_list.append(None)
                                                #     else:
                                                #         ewma_list.append(None)
                                                # else:
                                                #     ewma_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                            ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        else:
                                                            ewma_list.append(None)
                                                    else:
                                                        ewma_list.append(None)
                                                else:
                                                    ewma_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_ewma = self.configuration.ewma_divide(doc['processed_daily_data'][name]['ewma'][durationActual][2], doc['ewma_limits'][name][durationActual][2])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                    ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                    # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                else:
                                                    ewma_list.append(None)
                                                #     else:
                                                #         ewma_list.append(None)
                                                # else:
                                                #     ewma_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                            ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        else:
                                                            ewma_list.append(None)
                                                    else:
                                                        ewma_list.append(None)
                                                else:
                                                    ewma_list.append(None)
                            except KeyError:
                                ewma_list.append(None)
                            except TypeError:
                                ewma_list.append(None)
                            except IndexError:
                                ewma_list.append(None)
                            
                            try:
                                # if doc['historical'] == True:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['processed_daily_data'][name]['is_not_ewma_anamolous'][durationActual][2] == True:
                                            ewma_limit_list.append(self.configuration.makeDecimal(ewma_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # ewma_limit_list.append(ewma_number_dict[name])
                                        else:
                                            ewma_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['processed_daily_data'][name]['is_not_ewma_anamolous'][durationActual][2] == True:
                                            ewma_limit_list.append(self.configuration.makeDecimal(ewma_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # ewma_limit_list.append(ewma_number_dict[name])
                                        else:
                                            ewma_limit_list.append(None)
                            except KeyError:
                                ewma_limit_list.append(None)
                            except TypeError:
                                ewma_limit_list.append(None)
                            except IndexError:
                                ewma_limit_list.append(None)
                        
                        if self.anomalies == 'true':
                            try:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_spe = self.configuration.spe_divide(doc['processed_daily_data'][name]['SPEy'][durationActual], spe_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        # else:
                                                        #     spe_list.append(None)
                                                #     else:
                                                #         spe_list.append(None)
                                                # else:
                                                #     spe_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                        spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        # else:
                                                        #     spe_list.append(None)
                                                    else:
                                                        spe_list.append(None)
                                                else:
                                                    spe_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_spe = self.configuration.spe_divide(doc['processed_daily_data'][name]['SPEy'][durationActual], spe_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        # else:
                                                        #     spe_list.append(None)
                                                #     else:
                                                #         spe_list.append(None)
                                                # else:
                                                #     spe_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                        spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        # else:
                                                        #     spe_list.append(None)
                                                    else:
                                                        spe_list.append(None)
                                                else:
                                                    spe_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_spe = self.configuration.spe_divide(doc['processed_daily_data'][name]['SPEy'][durationActual], doc['spe_limits'][name][durationActual]['zero_zero_five'])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        # else:
                                                        #     spe_list.append(None)
                                                #     else:
                                                #         spe_list.append(None)
                                                # else:
                                                #     spe_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                        spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        # else:
                                                        #     spe_list.append(None)
                                                    else:
                                                        spe_list.append(None)
                                                else:
                                                    spe_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_spe = self.configuration.spe_divide(doc['processed_daily_data'][name]['SPEy'][durationActual], doc['spe_limits'][name][durationActual]['zero_zero_five'])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        # else:
                                                        #     spe_list.append(None)
                                                #     else:
                                                #         spe_list.append(None)
                                                # else:
                                                #     spe_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                                        spe_list.append(self.configuration.makeDecimal(new_spe))
                                                            # spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                                            # spe_list.append(spe_number_dict[name])
                                                        # else:
                                                        #     spe_list.append(None)
                                                    else:
                                                        spe_list.append(None)
                                                else:
                                                    spe_list.append(None)
                            except KeyError:
                                spe_list.append(None)
                            except TypeError:
                                spe_list.append(None)
                            except IndexError:
                                spe_list.append(None)
                            
                            try:
                                # if doc['historical'] == True:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        # if doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][2] == True:
                                        spe_limit_list.append(self.configuration.makeDecimal(spe_limit_global))
                                            # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                            # spe_limit_list.append(spe_number_dict[name])
                                        # else:
                                        #     spe_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        # if doc['processed_daily_data'][name]['is_not_spe_anamolous'][durationActual][2] == True:
                                        spe_limit_list.append(self.configuration.makeDecimal(spe_limit_global))
                                            # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                            # spe_limit_list.append(spe_number_dict[name])
                                        # else:
                                        #     spe_limit_list.append(None)
                            except KeyError:
                                spe_limit_list.append(None)
                            except TypeError:
                                spe_limit_list.append(None)
                            except IndexError:
                                spe_limit_list.append(None)
                            
                            #T2----------------------------------------------------------------------
                            try:
                                # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual][0] == True:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_t2 = self.configuration.t2_divide(doc['processed_daily_data'][name]['t2_initial'][durationActual], t2_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        # else:
                                                        #     t2_list.append(None)
                                                #     else:
                                                #         t2_list.append(None)
                                                # else:
                                                #     t2_list.append(None)
                                            

                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                        t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        # else:
                                                        #     t2_list.append(None)
                                                    else:
                                                        t2_list.append(None)
                                                else:
                                                    t2_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_t2 = self.configuration.t2_divide(doc['processed_daily_data'][name]['t2_initial'][durationActual], t2_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        # else:
                                                        #     t2_list.append(None)
                                                #     else:
                                                #         t2_list.append(None)
                                                # else:
                                                #     t2_list.append(None)
                                            

                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                        t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        # else:
                                                        #     t2_list.append(None)
                                                    else:
                                                        t2_list.append(None)
                                                else:
                                                    t2_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_t2 = self.configuration.t2_divide(doc['processed_daily_data'][name]['t2_initial'][durationActual], doc['t2_limits'][name]['zero_two'])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        # else:
                                                        #     t2_list.append(None)
                                                #     else:
                                                #         t2_list.append(None)
                                                # else:
                                                #     t2_list.append(None)
                                            

                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                        t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        # else:
                                                        #     t2_list.append(None)
                                                    else:
                                                        t2_list.append(None)
                                                else:
                                                    t2_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_t2 = self.configuration.t2_divide(doc['processed_daily_data'][name]['t2_initial'][durationActual], doc['t2_limits'][name]['zero_two'])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        # else:
                                                        #     t2_list.append(None)
                                                #     else:
                                                #         t2_list.append(None)
                                                # else:
                                                #     t2_list.append(None)
                                            

                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                                        t2_list.append(self.configuration.makeDecimal(new_t2))
                                                            # t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                                        # else:
                                                        #     t2_list.append(None)
                                                    else:
                                                        t2_list.append(None)
                                                else:
                                                    t2_list.append(None)
                            except KeyError:
                                t2_list.append(None)
                            except TypeError:
                                t2_list.append(None)
                            except IndexError:
                                t2_list.append(None)
                            
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        # if doc['processed_daily_data'][name]['is_not_t2_anamolous'][durationActual][0] == True:
                                        t2_limit_list.append(self.configuration.makeDecimal(t2_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # t2_limit_list.append(t2_number_dict[name])
                                        # else:
                                        #     t2_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        # if doc['processed_daily_data'][name]['is_not_t2_anamolous'][durationActual][0] == True:
                                        t2_limit_list.append(self.configuration.makeDecimal(t2_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # t2_limit_list.append(t2_number_dict[name])
                                        # else:
                                        #     t2_limit_list.append(None)
                            except KeyError:
                                t2_limit_list.append(None)
                            except TypeError:
                                t2_limit_list.append(None)
                            except IndexError:
                                t2_limit_list.append(None)
                            #EWMA-----------------------------------------------------------------------
                            try:
                                # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual][0] == True:
                                if doc['historical'] == True:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_ewma = self.configuration.ewma_divide(doc['processed_daily_data'][name]['ewma'][durationActual][2], ewma_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        # else:
                                                        #     ewma_list.append(None)
                                                #     else:
                                                #         ewma_list.append(None)
                                                # else:
                                                #     ewma_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                        ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        # else:
                                                        #     ewma_list.append(None)
                                                    else:
                                                        ewma_list.append(None)
                                                else:
                                                    ewma_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_ewma = self.configuration.ewma_divide(doc['processed_daily_data'][name]['ewma'][durationActual][2], ewma_number_dict[name])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        # else:
                                                        #     ewma_list.append(None)
                                                #     else:
                                                #         ewma_list.append(None)
                                                # else:
                                                #     ewma_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                        ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        # else:
                                                        #     ewma_list.append(None)
                                                    else:
                                                        ewma_list.append(None)
                                                else:
                                                    ewma_list.append(None)
                                else:
                                    if self.noonorlogs == "noon":
                                        if doc['Logs'] == False:
                                            new_ewma = self.configuration.ewma_divide(doc['processed_daily_data'][name]['ewma'][durationActual][2], doc['ewma_limits'][name][durationActual][2])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        # else:
                                                        #     ewma_list.append(None)
                                                #     else:
                                                #         ewma_list.append(None)
                                                # else:
                                                #     ewma_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                        ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        # else:
                                                        #     ewma_list.append(None)
                                                    else:
                                                        ewma_list.append(None)
                                                else:
                                                    ewma_list.append(None)
                                    elif self.noonorlogs == "logs":
                                        if doc['Logs'] == True:
                                            new_ewma = self.configuration.ewma_divide(doc['processed_daily_data'][name]['ewma'][durationActual][2], doc['ewma_limits'][name][durationActual][2])
                                            if self.include_outliers == 'true':
                                                # if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                #     if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                                        # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        # else:
                                                        #     ewma_list.append(None)
                                                #     else:
                                                #         ewma_list.append(None)
                                                # else:
                                                #     ewma_list.append(None)
                                            
                                            if self.include_outliers == 'false':
                                                if pd.isnull(doc['processed_daily_data'][name]['within_outlier_limits'][new_duration]) == False:
                                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                                        # if doc['processed_daily_data'][name]['ewma'][durationActual][2] < ewma_number_dict[name]:
                                                        ewma_list.append(self.configuration.makeDecimal(new_ewma))
                                                            # ewma_list.append(doc['processed_daily_data'][name]['ewma'][durationActual][2])
                                                        # else:
                                                        #     ewma_list.append(None)
                                                    else:
                                                        ewma_list.append(None)
                                                else:
                                                    ewma_list.append(None)
                            except KeyError:
                                ewma_list.append(None)
                            except TypeError:
                                ewma_list.append(None)
                            except IndexError:
                                ewma_list.append(None)
                            
                            try:
                                # if doc['historical'] == True:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        # if doc['processed_daily_data'][name]['is_not_ewma_anamolous'][durationActual][2] == True:
                                        ewma_limit_list.append(self.configuration.makeDecimal(ewma_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # ewma_limit_list.append(ewma_number_dict[name])
                                        # else:
                                        #     ewma_limit_list.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        # if doc['processed_daily_data'][name]['is_not_ewma_anamolous'][durationActual][2] == True:
                                        ewma_limit_list.append(self.configuration.makeDecimal(ewma_limit_global))
                                            # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                            # ewma_limit_list.append(ewma_number_dict[name])
                                        # else:
                                        #     ewma_limit_list.append(None)
                            except KeyError:
                                ewma_limit_list.append(None)
                            except TypeError:
                                ewma_limit_list.append(None)
                            except IndexError:
                                ewma_limit_list.append(None)

                        print("SPE!!!!!", name)

                        if 'ly_' in durationActual:
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][1]) == False:
                                            explist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][1]))
                                        else:
                                            explist.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][1]) == False:
                                            explist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][1]))
                                        else:
                                            explist.append(None)
                            except TypeError:
                                explist.append(None)
                            except KeyError:
                                explist.append(None)
                            except IndexError:
                                explist.append(None)
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if pd.isnull(doc['processed_daily_data'][name]['predictions'][durationActual][1]) == False:
                                            lyexplist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][durationActual][1]))
                                        else:
                                            lyexplist.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if pd.isnull(doc['processed_daily_data'][name]['predictions'][durationActual][1]) == False:
                                            lyexplist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][durationActual][1]))
                                        else:
                                            lyexplist.append(None)
                            except TypeError:
                                lyexplist.append(None)
                            except KeyError:
                                lyexplist.append(None)
                            except IndexError:
                                lyexplist.append(None)
                            
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['processed_daily_data'][name]['predictions'][new_duration][0] != -np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][0]) == False:
                                                lowerlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][0]))
                                            else:
                                                lowerlist.append(None)
                                        else:
                                            lowerlist.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['processed_daily_data'][name]['predictions'][new_duration][0] != -np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][0]) == False:
                                                lowerlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][0]))
                                            else:
                                                lowerlist.append(None)
                                        else:
                                            lowerlist.append(None)
                            except TypeError:
                                lowerlist.append(None)
                            except KeyError:
                                lowerlist.append(None)
                            except IndexError:
                                lowerlist.append(None)
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['processed_daily_data'][name]['predictions'][durationActual][0] != -np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][durationActual][0]) == False:
                                                lylowerlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][durationActual][0]))
                                            else:
                                                lylowerlist.append(None)
                                        else:
                                            lylowerlist.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['processed_daily_data'][name]['predictions'][durationActual][0] != -np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][durationActual][0]) == False:
                                                lylowerlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][durationActual][0]))
                                            else:
                                                lylowerlist.append(None)
                                        else:
                                            lylowerlist.append(None)
                            except TypeError:
                                lylowerlist.append(None)
                            except KeyError:
                                lylowerlist.append(None)
                            except IndexError:
                                lylowerlist.append(None)
                            
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['processed_daily_data'][name]['predictions'][new_duration][2] != np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][2]) == False:
                                                upperlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][2]))
                                            else:
                                                upperlist.append(None)
                                        else:
                                            upperlist.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['processed_daily_data'][name]['predictions'][new_duration][2] != np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][2]) == False:
                                                upperlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][2]))
                                            else:
                                                upperlist.append(None)
                                        else:
                                            upperlist.append(None)
                            except TypeError:
                                upperlist.append(None)
                            except KeyError:
                                upperlist.append(None)
                            except IndexError:
                                upperlist.append(None)
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['processed_daily_data'][name]['predictions'][durationActual][2] != np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][durationActual][2]) == False:
                                                lyupperlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][durationActual][2]))
                                            else:
                                                lyupperlist.append(None)
                                        else:
                                            lyupperlist.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['processed_daily_data'][name]['predictions'][durationActual][2] != np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][durationActual][2]) == False:
                                                lyupperlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][durationActual][2]))
                                            else:
                                                lyupperlist.append(None)
                                        else:
                                            lyupperlist.append(None)
                            except TypeError:
                                lyupperlist.append(None)
                            except KeyError:
                                lyupperlist.append(None)
                            except IndexError:
                                lyupperlist.append(None)
                        else:
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][1]) == False:
                                            explist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][1]))
                                        else:
                                            explist.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][1]) == False:
                                            explist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][1]))
                                        else:
                                            explist.append(None)
                            except TypeError:
                                explist.append(None)
                            except KeyError:
                                explist.append(None)
                            except IndexError:
                                explist.append(None)

                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['processed_daily_data'][name]['predictions'][new_duration][0] != -np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][0]) == False:
                                                lowerlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][0]))
                                            else:
                                                lowerlist.append(None)
                                        else:
                                            lowerlist.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['processed_daily_data'][name]['predictions'][new_duration][0] != -np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][0]) == False:
                                                lowerlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][0]))
                                            else:
                                                lowerlist.append(None)
                                        else:
                                            lowerlist.append(None)
                            except TypeError:
                                lowerlist.append(None)
                            except KeyError:
                                lowerlist.append(None)
                            except IndexError:
                                lowerlist.append(None)
                            
                            try:
                                if self.noonorlogs == "noon":
                                    if doc['Logs'] == False:
                                        if doc['processed_daily_data'][name]['predictions'][new_duration][2] != np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][2]) == False:
                                                upperlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][2]))
                                            else:
                                                upperlist.append(None)
                                        else:
                                            upperlist.append(None)
                                elif self.noonorlogs == "logs":
                                    if doc['Logs'] == True:
                                        if doc['processed_daily_data'][name]['predictions'][new_duration][2] != np.inf:
                                            if pd.isnull(doc['processed_daily_data'][name]['predictions'][new_duration][2]) == False:
                                                upperlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['predictions'][new_duration][2]))
                                            else:
                                                upperlist.append(None)
                                        else:
                                            upperlist.append(None)
                            except TypeError:
                                upperlist.append(None)
                            except KeyError:
                                upperlist.append(None)
                            except IndexError:
                                upperlist.append(None)
                    # datadict[name] = newlist
                    # expecteddict['expected_'+name] = explist
                    # lyexpecteddict['lyexpected_'+name] = lyexplist
                    # spedict['spe_'+name] = spe_list
                    # ewmadict['ewma_'+name] = ewma_list
                    # spelimitdict['spe_limit_'+name] = spe_limit_list
                    # ewmalimitdict['ewma_limit_'+name] = ewma_limit_list
                    # t2dict['t2_'+name] = t2_list
                    # t2limitdict['t2_limit_'+name] = t2_limit_list
                    # lowerdict[name] = lowerlist
                    # upperdict[name] = upperlist
                    # lylowerdict[name] = lylowerlist
                    # lyupperdict[name] = lyupperlist
                    # outlierdict[name] = outlierlist
                    if name == 'rep_dt' and name in doc['processed_daily_data'].keys():
                        if self.noonorlogs == "noon":
                            if doc['Logs'] == False:
                                # newdate = doc['processed_daily_data'][name]['processed'].strftime("%Y-%m-%d")
                                newdate = doc['final_rep_dt'].strftime("%Y-%m-%d")
                                newlist.append(newdate)
                                explist.append(newdate)
                                lyexplist.append(newdate)
                        elif self.noonorlogs == "logs":
                            if doc['Logs'] == True:
                                # newdate = doc['processed_daily_data'][name]['processed'].strftime("%Y-%m-%d")
                                newdate = doc['final_rep_dt'].strftime("%Y-%m-%d %H:%M:%S")
                                newlist.append(newdate)
                                explist.append(newdate)
                                lyexplist.append(newdate)
                # except KeyError:
                #     continue
                datadict[name] = newlist
                expecteddict['expected_'+name] = explist
                lyexpecteddict['lyexpected_'+name] = lyexplist
                spedict['spe_'+name] = spe_list
                ewmadict['ewma_'+name] = ewma_list
                spelimitdict['spe_limit_'+name] = spe_limit_list
                ewmalimitdict['ewma_limit_'+name] = ewma_limit_list
                t2dict['t2_'+name] = t2_list
                t2limitdict['t2_limit_'+name] = t2_limit_list
                lowerdict[name] = lowerlist
                upperdict[name] = upperlist
                lylowerdict[name] = lylowerlist
                lyupperdict[name] = lyupperlist
                outlierdict[name] = outlierlist
            print("DATADICT!!!!!!", datadict)
            
            ''' Deleting all the variables that do not have expected, lower, and upper values'''
            for k in datadict.copy():
                if datadict[k] == []:
                    del datadict[k]
                try:
                    if all(x is None for x in datadict[k]):
                        del datadict[k]
                except KeyError:
                    continue
            for k in spedict.copy():
                if spedict[k] == []:
                    del spedict[k]
                try:
                    if all(x is None for x in spedict[k]):
                        del spedict[k]
                except KeyError:
                    continue
            for k in spelimitdict.copy():
                if spelimitdict[k] == []:
                    del spelimitdict[k]
                try:
                    if all(x is None for x in spelimitdict[k]):
                        del spelimitdict[k]
                except KeyError:
                    continue
            for k in ewmadict.copy():
                if ewmadict[k] == []:
                    del ewmadict[k]
                try:
                    if all(x is None for x in ewmadict[k]):
                        del ewmadict[k]
                except KeyError:
                    continue
            for k in ewmalimitdict.copy():
                if ewmalimitdict[k] == []:
                    del ewmalimitdict[k]
                try:
                    if all(x is None for x in ewmalimitdict[k]):
                        del ewmalimitdict[k]
                except KeyError:
                    continue
            for k in t2dict.copy():
                if t2dict[k] == []:
                    del t2dict[k]
                try:
                    if all(x is None for x in t2dict[k]):
                        del t2dict[k]
                except KeyError:
                    continue
            for k in t2limitdict.copy():
                if t2limitdict[k] == []:
                    del t2limitdict[k]
                try:
                    if all(x is None for x in t2limitdict[k]):
                        del t2limitdict[k]
                except KeyError:
                    continue
            for k in expecteddict.copy():
                if expecteddict[k] == []:
                    del expecteddict[k]
                try:
                    if all(x is None for x in expecteddict[k]):
                        del expecteddict[k]
                except KeyError:
                    continue
            for k in lowerdict.copy():
                if lowerdict[k] == []:
                    del lowerdict[k]
                try:
                    if all(x is None for x in lowerdict[k]):
                        del lowerdict[k]
                except KeyError:
                    continue
            for k in upperdict.copy():
                if upperdict[k] == []:
                    del upperdict[k]
                try:
                    if all(x is None for x in upperdict[k]):
                        del upperdict[k]
                except KeyError:
                    continue
            for k in lyexpecteddict.copy():
                if lyexpecteddict[k] == []:
                    del lyexpecteddict[k]
                try:
                    if all(x is None for x in lyexpecteddict[k]):
                        del lyexpecteddict[k]
                except KeyError:
                    continue
            for k in lylowerdict.copy():
                if lylowerdict[k] == []:
                    del lylowerdict[k]
                try:
                    if all(x is None for x in lylowerdict[k]):
                        del lylowerdict[k]
                except KeyError:
                    continue
            for k in lyupperdict.copy():
                if lyupperdict[k] == []:
                    del lyupperdict[k]
                try:
                    if all(x is None for x in lyupperdict[k]):
                        del lyupperdict[k]
                except KeyError:
                    continue
            for k in outlierdict.copy():
                if outlierdict[k] == []:
                    del outlierdict[k]
                try:
                    if all(x is None for x in outlierdict[k]):
                        del outlierdict[k]
                except KeyError:
                    continue
            for k in spe_messages_dict.copy():
                if spe_messages_dict[k] == []:
                    del spe_messages_dict[k]
                try:
                    if all(x is None for x in spe_messages_dict[k]):
                        del spe_messages_dict[k]
                except KeyError:
                    continue          
            print("END LIST")
            # processed_values = pd.DataFrame(datadict)
            # processed_values.to_csv("Processed_Values.csv")
            # expected_values = pd.DataFrame(expecteddict)
            # expected_values.to_csv("Expected_Values.csv")
            # spe_values = pd.DataFrame(spedict)
            # spe_values.to_csv("SPE_Values.csv")
            # ewma_values = pd.DataFrame(ewmadict)
            # ewma_values.to_csv("EWMA_Values.csv")
            # t2_values = pd.DataFrame(t2dict)
            # t2_values.to_csv("T2_Values.csv")
            # outlier_values = pd.DataFrame(outlierdict)
            # outlier_values.to_csv("Outlier_Values.csv")
            # newgroup = self.configuration.get_corrected_group_selection(datadict, group, spe=False)
            print("START SPE")
            newgroup_spe = self.configuration.get_group_selection_including_spe(groups, datadict, spedict)
            print("END SPE")
            print("START CORRECTED GRP")
            corrected_group = self.configuration.get_corrected_group_selection(newgroup_spe)
            self.corrected = corrected_group
            subgroup_dict_new = self.configuration.get_subgroup_names_for_custom_groups(subgroup_dict, "", corrected_group) if len(self.individual_params) > 1 else subgroup_dict
            weather_data = self.get_weather_parameters()
            short_names = self.short_names_for_weather_params(list(weather_data.keys()))
            print("END CORRECTED GRP")
            
            if self.include_outliers == 'true':
                if 'ly_' in durationActual:
                    return corrected_group, datadict, expecteddict, lowerdict, upperdict, lyexpecteddict, lylowerdict, lyupperdict, outlierdict, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict, ewmadict, ewmalimitdict, spe_messages_dict, subgroup_dict_new, outlier_messages_dict, weather_data, short_names
                else:
                    return corrected_group, datadict, expecteddict, lowerdict, upperdict, outlierdict, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict, ewmadict, ewmalimitdict, spe_messages_dict, subgroup_dict_new, outlier_messages_dict, weather_data, short_names
            else:
                if 'ly_' in durationActual:
                    return corrected_group, datadict, expecteddict, lowerdict, upperdict, lyexpecteddict, lylowerdict, lyupperdict, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict, ewmadict, ewmalimitdict, spe_messages_dict, subgroup_dict_new, weather_data, short_names
                else:
                    return corrected_group, datadict, expecteddict, lowerdict, upperdict, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict, ewmadict, ewmalimitdict, spe_messages_dict, subgroup_dict_new, weather_data, short_names
    
    def process_fuel_consumption(self):
        ''' Returns the dictionary of all the variables for Fuel Consumption sorted by date(final_rep_dt)'''
        fuelSelection = self.configuration.get_selection_for_fuel_consumption()
        durationActual = self.configuration.create_current_duration(self.duration)
        charter_party_static_data = self.configuration.get_static_data_for_charter_party()
        charter_party_static_data_list = list(charter_party_static_data.keys())
        # dateres = self.read_for_fuel_consumption()
        # fuelres = loads(dumps(fuelcursor))
        # print(loads(dumps(fuelres)))
        fuelresList=[]
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(fuelres)
        # print(fuelres)
        print("START PROCESS FUEL CONSUMPTION")
        # for i in range(0, len(fuelres[0])):
        # for index, item in enumerate(dateres):
        #     newdate = item['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
        #     temp = {'Report Date': newdate}
        #     fuelresList.append(temp)

        for datedoc in self.main_db.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'Logs': 1, '_id': 0}).sort('final_rep_dt', ASCENDING):
            # newdate = datedoc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
            if self.noonorlogs == "noon":
                if datedoc['Logs'] == False:
                    newdate = datedoc['final_rep_dt'].strftime("%Y-%m-%d")
                    temp = {'Report Date': newdate}
                    fuelresList.append(temp)
            elif self.noonorlogs == "logs":
                if datedoc['Logs'] == True:
                    newdate = datedoc['final_rep_dt'].strftime("%Y-%m-%d %H:%M:%S")
                    temp = {'Report Date': newdate}
                    fuelresList.append(temp)

        # print(fuelSelection)
        for var in fuelSelection:
            if var != 'rep_dt':
                for doc in self.main_db.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'Logs': 1, "processed_daily_data."+var: 1, "_id": 0}).sort('final_rep_dt', ASCENDING):
                    for i in range(len(fuelresList)):
                        # if fuelresList[i]['Report Date'] == doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d'):
                        if self.noonorlogs == "noon":
                            if datedoc['Logs'] == False:
                                if fuelresList[i]['Report Date'] == doc['final_rep_dt'].strftime("%Y-%m-%d"):
                                    short_name = doc['processed_daily_data'][var]['name'].strip() if var in doc['processed_daily_data'] else 'null'
                                    temp = []
                                    try:
                                        new_processed = self.configuration.makeDecimal(doc['processed_daily_data'][var]['processed']) if pd.isnull(doc['processed_daily_data'][var]['processed']) == False else None
                                        temp.append(new_processed)

                                    except TypeError:
                                        new_processed = None
                                        temp.append(new_processed)
                                    except KeyError:
                                        new_processed = None
                                        temp.append(new_processed)
                                    except IndexError:
                                        new_processed = None
                                        temp.append(new_processed)
                                    
                                    if var == 'main_fuel':
                                        for key in charter_party_static_data_list:
                                            if var == key:
                                                new_predictions = charter_party_static_data[key] if pd.isnull(charter_party_static_data[key]) == False else None
                                                temp.append(new_predictions)
                                    else:
                                        new_predictions = None
                                        temp.append(new_predictions)
                                    # try:
                                    #     new_predictions = self.configuration.makeDecimal(doc['processed_daily_data'][var]['predictions'][durationActual][1])
                                    #     temp.append(new_predictions)
                                    # except TypeError:
                                    #     new_predictions = None
                                    #     temp.append(new_predictions)
                                    # except KeyError:
                                    #     new_predictions = None
                                    #     temp.append(new_predictions)
                                    # except IndexError:
                                    #     new_predictions = None
                                    #     temp.append(new_predictions)
                                    fuelresList[i][short_name] = temp
                        elif self.noonorlogs == "logs":
                            if datedoc['Logs'] == True:
                                if fuelresList[i]['Report Date'] == doc['final_rep_dt'].strftime("%Y-%m-%d %H:%M:%S"):
                                    short_name = doc['processed_daily_data'][var]['name'].strip() if var in doc['processed_daily_data'] else 'null'
                                    temp = []
                                    try:
                                        new_processed = self.configuration.makeDecimal(doc['processed_daily_data'][var]['processed']) if pd.isnull(doc['processed_daily_data'][var]['processed']) == False else None
                                        temp.append(new_processed)

                                    except TypeError:
                                        new_processed = None
                                        temp.append(new_processed)
                                    except KeyError:
                                        new_processed = None
                                        temp.append(new_processed)
                                    except IndexError:
                                        new_processed = None
                                        temp.append(new_processed)
                                    
                                    if var == 'main_fuel':
                                        for key in charter_party_static_data_list:
                                            if var == key:
                                                new_predictions = charter_party_static_data[key] if pd.isnull(charter_party_static_data[key]) == False else None
                                                temp.append(new_predictions)
                                    else:
                                        new_predictions = None
                                        temp.append(new_predictions)
                                    # try:
                                    #     new_predictions = self.configuration.makeDecimal(doc['processed_daily_data'][var]['predictions'][durationActual][1])
                                    #     temp.append(new_predictions)
                                    # except TypeError:
                                    #     new_predictions = None
                                    #     temp.append(new_predictions)
                                    # except KeyError:
                                    #     new_predictions = None
                                    #     temp.append(new_predictions)
                                    # except IndexError:
                                    #     new_predictions = None
                                    #     temp.append(new_predictions)
                                    fuelresList[i][short_name] = temp



        
        print("END PROCESS FUEL CONSUMPTION")

        
        return fuelresList
    
    def get_weather_parameters(self):
        '''
            Gets weather data
        '''

        weather_parameters = ['sea_st', 'w_force', 'w_dir_rel', 'curknots', 'swelldir', 'current_dir_rel']
        weather_data = {}

        for param in weather_parameters:
            tempList=[]
            for doc in self.main_db.find({'ship_imo': self.ship_imo}, {"processed_daily_data."+param: 1}).sort('final_rep_dt', ASCENDING):
                try:
                    tempList.append(self.configuration.makeDecimal(doc['processed_daily_data'][param]['processed']))
                except TypeError:
                    tempList.append(None)
            weather_data[param] = tempList
        
        return weather_data

    # print("START GENERIC GROUP SELECTION")
    # def get_generic_group_list(self):
    #     groupList = self.configuration.get_generic_group_selection()
    #     return groupList
    # print("END GENERIC GROUP SELECTION")

    def short_names_for_weather_params(self, params):
        '''
            Get Short Names for weather parameters
        '''
        all_short_names = self.configuration.create_short_names_dictionary()
        short_names = {}

        for param in params:
            short_names[param] = all_short_names[param]
        
        return short_names

    def get_groups_selection_for_table_component(self):
        # self.configuration = Configurator(self.ship_imo)
        # self.ship_configs = self.configuration.get_ship_configs()
        # self.main_db = self.configuration.get_main_data()
        # if self.group == "" and len(self.individual_params) > 1:
        #     self.groupsList = self.configuration.get_group_selection_for_individual_parameters(self.individual_params)
        # if self.group != "" and len(self.individual_params) == 1:
        #     self.groupsList = self.configuration.get_group_selection(self.group)
        # if self.group != "" and len(self.individual_params) > 1:
        #     self.groupsList = self.configuration.get_group_selection_for_individual_parameters(self.individual_params, self.group)
        print("CORRECTED GROUP",self.corrected)
        return self.corrected
    
    print("START CHART HEIGHT")
    def get_chart_height(self, groupsList):
        height = self.configuration.get_height_of_chart(groupsList)
        return height
    print("END CHART HEIGHT")

    print("START CHART HEIGHT AFTER 2x CLICK")
    def get_height_of_chart_after_double_click(self, groupsList):
        height_after_double_click = self.configuration.get_height_of_chart_after_double_click(groupsList)
        return height_after_double_click
    print("End CHART HEIGHT AFTER 2x CLICK")
    
    # def get_short_names_list_for_multi_axis(self):
    #     namesList = self.configuration.get_short_names_list_for_multi_axis(self.group)
    #     return namesList
    
    print("START Y POSITION")
    def get_Y_position(self):
        y_position = self.configuration.get_Y_position_for_legend(self.groupsList)
        return y_position
    print("END Y POSITION")
    
    print("START Y POSITION AFTER 2x CLICK")
    def get_Y_position_after_double_click(self):
        y_position_after_double_click = self.configuration.get_Y_position_for_legend_after_double_click(self.groupsList)
        return y_position_after_double_click
    print("END Y POSITION AFTER 2x CLICK")

    print("START VARIABLES LIST")
    def variable_list_according_to_order_blocks(self, datadict, groupsList):
        # print(datadict.keys())
        new_variables_list = []
        spe_variables_list = []
        variables_list = self.configuration.get_variables_list_in_order_of_blocks(groupsList)

        for i in variables_list:
            if 'spe_' in i:
                spe_variables_list.append(i)
        
        for i in range(len(variables_list)):
            new_variables_list.append(variables_list[i])
        
        for i in spe_variables_list:
            name1 = i.replace('spe_', 'spe_limit_')
            name2 = i.replace('spe_', 't2_')
            name3 = i.replace('spe_', 't2_limit_')
            name4 = i.replace('spe_', 'ewma_')
            name5 = i.replace('spe_', 'ewma_limit_')
            new_variables_list.append(name1)
            new_variables_list.append(name2)
            new_variables_list.append(name3)
            new_variables_list.append(name4)
            new_variables_list.append(name5)
        # datadictList = list(datadict)
        # for var in variables_list:
        #     if var not in datadictList:
        #         print("INSIDE VARIABLES LIST IF")
        #         variables_list.remove(var)
        return new_variables_list
    print("END VARIABLES LIST")
    
    # print("START INDIVIDUAL PARAMETERS")
    # def individual_parameters(self):
    #     individual_params = self.configuration.get_individual_parameters()
    #     return individual_params
    # print("END INDIVIDUAL PARAMETERS")
            
        


# Remove later
# res = TrendsExtractor(9591301, 'BASIC', '30Days')
# res.connect()
# res.process_fuel_consumption()
# res.do_steps()
# print(grpresult)
# print(mainresult)