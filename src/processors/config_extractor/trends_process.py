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




class TrendsExtractor():
    def __init__(self, ship_imo, group, duration, individual_params, include_outliers='false', compare='None', anomalies='true'):
        self.ship_imo = int(ship_imo)
        self.group = group
        self.duration = duration
        self.include_outliers = include_outliers
        self.individual_params = individual_params
        self.compare = compare
        self.anomalies = anomalies
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
        if self.include_outliers == 'true':
            if 'Lastyear' in self.duration:
                a, b, c, d, e, f, g, h, i, j, k, l, m, n = self.process_data()
                return a, b, c, d, e, f, g, h, i, j, k, l, m, n
            else:
                a, b, c, d, e, f, g, h, i, j, k = self.process_data()
                return a, b, c, d, e, f, g, h, i, j, k
        else:
            if 'Lastyear' in self.duration:
                a, b, c, d, e, g, h, i, j, k, l, m, n = self.process_data()
                return a, b, c, d, e, g, h, i, j, k, l, m, n
            else:
                a, b, c, d, e, g, h, i, j, k = self.process_data()
                return a, b, c, d, e, g, h, i, j, k
    
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
        spe_number_dict = {}
        t2_number_dict = {}
        if('Multi Axis' in self.group):
            ''' TEMPORARY PURPOSES'''
            nameslist = self.configuration.temporary_dict_and_list_according_to_groups(self.group)
            group = self.configuration.temporary_group_selection_for_11(self.group)
        else:
            print("START DICT AND LIST ACC TO GROUPS")
            if self.group == "" and self.individual_params != "":
                nameslist = self.individual_params
            if self.group != "" and self.individual_params == "":
                nameslist = self.configuration.get_dict_and_list_according_to_groups(self.groupsList)
            if self.group != "" and self.individual_params != "":
                nameslist = self.configuration.get_dict_and_list_according_to_groups(self.groupsList)
            
            print("END DICT AND LIST ACC TO GROUPS")
            # group = self.configuration.get_group_selection(self.group)
            # dependent_parameters = self.configuration.get_dependent_parameters()
            new_duration = durationActual.replace('ly_','') if 'ly_' in durationActual else durationActual
            groups = self.groupsList
            print("START LIST")
            for name in nameslist:
                try:
                    cursor_temp_spe = self.ship_configs.find({'ship_imo': self.ship_imo}, {'spe_limits.'+name+'.zero_zero_five': 1, 't2_limits.'+name+'.zero_two': 1})
                    temp_spe = loads(dumps(cursor_temp_spe))
                    print("TEMP SPE", temp_spe)
                    spe_number_dict[name] = temp_spe[0]['spe_limits'][name]['zero_zero_five']
                    t2_number_dict[name] = temp_spe[0]['t2_limits'][name]['zero_two']
                except KeyError:
                    continue
            print("SPE & T2", spe_number_dict, t2_number_dict)
            for name in nameslist:
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
                outliert2_list = []
                t2_limit_list = []
                outliert2_limit_list = []
                # outlierColorList=[]
                # opacityList=[]
                # if name != 'rep_dt':
                # print(name)
                # spe_limit_number = self.ship_configs.find({'ship_imo': self.ship_imo}, {''})
                try:
                    for doc in self.main_db.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'processed_daily_data.'+name: 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                        # try:
                            if name != 'rep_dt':
                                try:
                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == False:
                                        if pd.isnull(doc['processed_daily_data'][name]['processed']) == False:
                                            outlierlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['processed']))
                                        else:
                                            outlierlist.append(None)
                                        
                                        # try:
                                        #     if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][0] == True:
                                        #         outlierspe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                        #     else:
                                        #         outlierspe_list.append(None)
                                        # except KeyError:
                                        #     outlierspe_list.append(None)
                                        # except TypeError:
                                        #     outlierspe_list.append(None)
                                        # except IndexError:
                                        #     outlierspe_list.append(None)
                                        
                                        # try:
                                        #     if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][0] == True:
                                        #         outlierspe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                        #     else:
                                        #         outlierspe_limit_list.append(None)
                                        # except KeyError:
                                        #     outlierspe_limit_list.append(None)
                                        # except TypeError:
                                        #     outlierspe_limit_list.append(None)
                                        # except IndexError:
                                        #     outlierspe_limit_list.append(None)
                                        
                                        # try:
                                        #     if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][0] == True:
                                        #         outliert2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                        #     else:
                                        #         outliert2_list.append(None)
                                        # except KeyError:
                                        #     outliert2_list.append(None)
                                        # except TypeError:
                                        #     outliert2_list.append(None)
                                        # except IndexError:
                                        #     outliert2_list.append(None)

                                        # try:
                                        #     if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][0] == True:
                                        #         outliert2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                        #     else:
                                        #         outliert2_limit_list.append(None)
                                        # except KeyError:
                                        #     outliert2_limit_list.append(None)
                                        # except TypeError:
                                        #     outliert2_limit_list.append(None)
                                        # except IndexError:
                                        #     outliert2_limit_list.append(None)
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
                                    if doc['processed_daily_data'][name]['within_outlier_limits'][new_duration] == True:
                                        if pd.isnull(doc['processed_daily_data'][name]['processed']) == False:
                                            newlist.append(self.configuration.makeDecimal(doc['processed_daily_data'][name]['processed']))
                                        else:
                                            newlist.append(None)
                                        
                                        
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
                                
                                # if self.anomalies == 'false':
                                try:
                                    # if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][2] == True:
                                    if doc['processed_daily_data'][name]['SPEy'][durationActual] < spe_number_dict[name]:
                                        spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                        # spe_list.append(spe_number_dict[name])
                                    else:
                                        spe_list.append(None)
                                except KeyError:
                                    spe_list.append(None)
                                except TypeError:
                                    spe_list.append(None)
                                except IndexError:
                                    spe_list.append(None)
                                
                                try:
                                    if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][2] == True:
                                        # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                        spe_limit_list.append(spe_number_dict[name])
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
                                    if doc['processed_daily_data'][name]['t2_initial'][durationActual] < t2_number_dict[name]:
                                        t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                    else:
                                        t2_list.append(None)
                                except KeyError:
                                    t2_list.append(None)
                                except TypeError:
                                    t2_list.append(None)
                                except IndexError:
                                    t2_list.append(None)
                                
                                try:
                                    if doc['processed_daily_data'][name]['t2_anamoly'][durationActual][0] == True:
                                        # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                        t2_limit_list.append(t2_number_dict[name])
                                    else:
                                        t2_limit_list.append(None)
                                except KeyError:
                                    t2_limit_list.append(None)
                                except TypeError:
                                    t2_limit_list.append(None)
                                except IndexError:
                                    t2_limit_list.append(None)

                                # if self.anomalies == 'true':
                                #     try:
                                #         # if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][1] == True:
                                #         spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                #         # else:
                                #         #     spe_list.append(None)
                                #     except KeyError:
                                #         spe_list.append(None)
                                #     except TypeError:
                                #         spe_list.append(None)
                                #     except IndexError:
                                #         spe_list.append(None)
                                    
                                #     try:
                                #         # if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][1] == True:
                                #             # spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                #         spe_limit_list.append(spe_number_dict[name])
                                #         # else:
                                #         #     spe_limit_list.append(None)
                                #     except KeyError:
                                #         spe_limit_list.append(None)
                                #     except TypeError:
                                #         spe_limit_list.append(None)
                                #     except IndexError:
                                #         spe_limit_list.append(None)
                                    
                                #     #T2----------------------------------------------------------------------
                                #     try:
                                #         # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual] == True:
                                #         t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                #         # else:
                                #         #     t2_list.append(None)
                                #     except KeyError:
                                #         t2_list.append(None)
                                #     except TypeError:
                                #         t2_list.append(None)
                                #     except IndexError:
                                #         t2_list.append(None)
                                    
                                #     try:
                                #         # if doc['processed_daily_data'][name]['t2_anamoly'][durationActual] == True:
                                #             # t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                #         t2_limit_list.append(t2_number_dict[name])
                                #         # else:
                                #         #     t2_limit_list.append(None)
                                #     except KeyError:
                                #         t2_limit_list.append(None)
                                #     except TypeError:
                                #         t2_limit_list.append(None)
                                #     except IndexError:
                                #         t2_limit_list.append(None)
                                print("SPE!!!!!", name)
                                # try:
                                #     if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][0] == False:
                                #         spe_list.append(doc['processed_daily_data'][name]['SPEy'][durationActual])
                                #     else:
                                #         spe_list.append(None)
                                # except KeyError:
                                #     spe_list.append(None)
                                # except TypeError:
                                #     spe_list.append(None)
                                # except IndexError:
                                #     spe_list.append(None)
                                
                                # try:
                                #     if doc['processed_daily_data'][name]['spe_anamoly'][durationActual][0] == False:
                                #         spe_limit_list.append(doc['processed_daily_data'][name]['Q_y'][durationActual][1])
                                #     else:
                                #         spe_limit_list.append(None)
                                # except KeyError:
                                #     spe_limit_list.append(None)
                                # except TypeError:
                                #     spe_limit_list.append(None)
                                # except IndexError:
                                #     spe_limit_list.append(None)
                                
                                # #T2----------------------------------------------------------------------
                                # if 't2_initial' in doc['processed_daily_data'][name]:
                                #     t2_list.append(doc['processed_daily_data'][name]['t2_initial'][durationActual])
                                # else:
                                #     t2_list.append(None)
                                # if 'ucl_crit_beta' in doc['processed_daily_data'][name]:
                                #     t2_limit_list.append(doc['processed_daily_data'][name]['ucl_crit_beta'][durationActual])
                                # else:
                                #     t2_limit_list.append(None)

                                if 'ly_' in durationActual:
                                    try:
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
                                
                            else:
                                newdate = doc['processed_daily_data'][name]['processed'].strftime("%Y-%m-%d")
                                newlist.append(newdate)
                                explist.append(newdate)
                                lyexplist.append(newdate)
                except KeyError:
                    for doc in self.main_db.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'independent_indices.'+name: 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                        try:
                            if doc['independent_indices'][name]['spe_anamoly'][durationActual][1] == True:
                            # spe_list.append(doc['independent_indices'][name]['SPE'][durationActual])
                                print('SPE!!!!', name)
                                newlist.append(self.configuration.makeDecimal(doc['independent_indices'][name]['SPEy'][durationActual]))
                            else:
                                newlist.append(None)
                        except KeyError:
                            newlist.append(None)
                        except TypeError:
                            newlist.append(None)
                        except IndexError:
                            newlist.append(None)
                        
                        try:
                            if doc['independent_indices'][name]['spe_anamoly'][durationActual][1] == True:
                            # spe_list.append(doc['independent_indices'][name]['SPE'][durationActual])
                                print('SPE!!!!', name)
                                spe_limit_list.append(self.configuration.makeDecimal(doc['independent_indices'][name]['Q_y'][durationActual][1]))
                            else:
                                spe_limit_list.append(None)
                        except KeyError:
                            spe_limit_list.append(None)
                        except TypeError:
                            spe_limit_list.append(None)
                        except IndexError:
                            spe_limit_list.append(None)
                        
                        try:
                            if 't2_initial' in doc['independent_indices'][name]:
                                t2_list.append(doc['independent_indices'][name]['t2_initial'][durationActual])
                            else:
                                t2_list.append(None)
                        except KeyError:
                            t2_list.append(None)
                        
                        try:
                            if 'ucl_crit_beta' in doc['independent_indices'][name]:
                                t2_limit_list.append(doc['independent_indices'][name]['ucl_crit_beta'][durationActual])
                            else:
                                t2_limit_list.append(None)
                        except KeyError:
                            t2_limit_list.append(None)

                datadict[name] = newlist
                expecteddict['expected_'+name] = explist
                lyexpecteddict['lyexpected_'+name] = lyexplist
                spedict['spe_'+name] = spe_list
                if 'index' in name:
                    spelimitdict[name] = spe_limit_list
                    t2dict[name] = t2_list
                    t2limitdict[name] = t2_limit_list
                else:
                    spelimitdict['spe_limit_'+name] = spe_limit_list
                    t2dict['t2_'+name] = t2_list
                    t2limitdict['t2_limit_'+name] = t2_limit_list
                lowerdict[name] = lowerlist
                upperdict[name] = upperlist
                lylowerdict[name] = lylowerlist
                lyupperdict[name] = lyupperlist
                outlierdict[name] = outlierlist
                # outlierdict['spe_'+name] = outlierspe_list
                # outlierdict['spe_limit_'+name] = outlierspe_limit_list
                # outlierdict['t2_'+name] = outliert2_limit_list
                # outlierdict['t2_limit_'+name] = outlierlist
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
                if not expecteddict[k]:
                    del expecteddict[k]
            # for k in spedict.copy():
            #     if not spedict[k]:
            #         del spedict[k]
            for k in lowerdict.copy():
                if not lowerdict[k]:
                    del lowerdict[k]
            for k in upperdict.copy():
                if not upperdict[k]:
                    del upperdict[k]
            for k in lyexpecteddict.copy():
                if not lyexpecteddict[k]:
                    del lyexpecteddict[k]
            for k in lylowerdict.copy():
                if not lylowerdict[k]:
                    del lylowerdict[k]
            for k in lyupperdict.copy():
                if not lyupperdict[k]:
                    del lyupperdict[k]
            # for k in outlierdict.copy():
            #     if not outlierdict[k]:
            #         del outlierdict[k]
            for k in outlierdict.copy():
                if outlierdict[k] == []:
                    del outlierdict[k]
                try:
                    if all(x is None for x in outlierdict[k]):
                        del outlierdict[k]
                except KeyError:
                    continue
            # for k in outlierColorDict.copy():
            #     if not outlierColorDict[k]:
            #         del outlierColorDict[k]  
            # for k in outlierOpacityDict.copy():
            #     if not outlierOpacityDict[k]:
            #         del outlierOpacityDict[k]          
        print("END LIST")
        # newgroup = self.configuration.get_corrected_group_selection(datadict, group, spe=False)
        print("START SPE")
        newgroup_spe = self.configuration.get_group_selection_including_spe(groups, datadict, spedict)
        print("END SPE")
        print("START CORRECTED GRP")
        corrected_group = self.configuration.get_corrected_group_selection(newgroup_spe)
        self.corrected = corrected_group
        print("END CORRECTED GRP")
        loaded_ballast_list = self.configuration.get_shapes_for_loaded_ballast_data()
        if self.include_outliers == 'true':
            if 'ly_' in durationActual:
                return corrected_group, datadict, expecteddict, lowerdict, upperdict, lyexpecteddict, lylowerdict, lyupperdict, outlierdict, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict
            else:
                return corrected_group, datadict, expecteddict, lowerdict, upperdict, outlierdict, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict
        else:
            if 'ly_' in durationActual:
                return corrected_group, datadict, expecteddict, lowerdict, upperdict, lyexpecteddict, lylowerdict, lyupperdict, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict
            else:
                return corrected_group, datadict, expecteddict, lowerdict, upperdict, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict
    
    def process_fuel_consumption(self):
        ''' Returns the dictionary of all the variables for Fuel Consumption sorted by date(rep_dt)'''
        fuelSelection = self.configuration.get_selection_for_fuel_consumption()
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

        for datedoc in self.main_db.find({'ship_imo': self.ship_imo}, {'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
            newdate = datedoc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
            temp = {'Report Date': newdate}
            fuelresList.append(temp)

        # print(fuelSelection)
        for var in fuelSelection:
            if var != 'rep_dt':
                for doc in self.main_db.find({'ship_imo': self.ship_imo}, {"processed_daily_data.rep_dt.processed": 1, "processed_daily_data."+var: 1, "_id": 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                    for i in range(len(fuelresList)):
                        if fuelresList[i]['Report Date'] == doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d'):
                            short_name = doc['processed_daily_data'][var]['name'].strip() if var in doc['processed_daily_data'] else 'null'
                            try:
                                new_processed = self.configuration.makeDecimal(doc['processed_daily_data'][var]['processed'])
                            except TypeError:
                                new_processed = 'null'
                            except KeyError:
                                continue
                            fuelresList[i][short_name] = new_processed



        
        print("END PROCESS FUEL CONSUMPTION")

        
        return fuelresList

    # print("START GENERIC GROUP SELECTION")
    # def get_generic_group_list(self):
    #     groupList = self.configuration.get_generic_group_selection()
    #     return groupList
    # print("END GENERIC GROUP SELECTION")

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
    
    def get_short_names_list_for_multi_axis(self):
        namesList = self.configuration.get_short_names_list_for_multi_axis(self.group)
        return namesList
    
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
        variables_list = self.configuration.get_variables_list_in_order_of_blocks(groupsList)
        for i in range(len(variables_list)):
            new_variables_list.append(variables_list[i])
            if 'spe_' in variables_list[i]:
                name1 = variables_list[i].replace('spe_', 'spe_limit_')
                name2 = variables_list[i].replace('spe_', 't2_')
                name3 = variables_list[i].replace('spe_', 't2_limit_')
                new_variables_list.append(name1)
                new_variables_list.append(name2)
                new_variables_list.append(name3)
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