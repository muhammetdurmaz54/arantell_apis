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
# import pandas as pd

log = CommonLogger(__name__,debug=True).setup_logger()
# df = pd.read_excel("D:/Internship/Helper Files/ConfiguratorRev_04 A.xlsx", sheet_name="Groups", engine="openpyxl", header=[0, 1], index_col=[0])
# a = df.columns.get_level_values(0).to_series()
# b = a.mask(a.str.startswith('Unnamed')).ffill().fillna('')
# df.columns = [b, df.columns.get_level_values(1)]




class TrendsExtractor():
    def __init__(self, ship_imo, group, duration):
        self.ship_imo = int(ship_imo)
        self.group = group
        self.duration = duration
        self.db_aranti = None
        self.db_test = None
        self.error = False
        self.traceback_msg = None

    def do_steps(self):
        self.connect()
        self.read_data()
        a, b, c, d, e, f, g, h = self.process_data()
        return a, b, c, d, e, f, g, h
    
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
    
  
    def read_data(self):
        ''' Initializing the Configurator instance and running get_ship_configs 
            to connect to the database
        '''
        self.configuration = Configurator(self.ship_imo)
        self.ship_configs = self.configuration.get_ship_configs()
        self.main_db = self.configuration.get_main_data()

        mainres = []
        singledata = self.ship_configs.find_one()['data']

        self.groupsList = self.configuration.get_group_selection(self.group)
        
        '''For getting the data of all the variables in the group list'''
        res = self.main_db.find(
            {
                'ship_imo': self.ship_imo
            },
            {
                "processed_daily_data.rep_dt.processed": 1,
                "_id": 0
            }
        ).sort("processed_daily_data.rep_dt.processed", ASCENDING)
        mainres.append(res)
        for vars in singledata.keys():
            grps = singledata[vars]['group_selection']
            for grp in grps:
                if grp['groupname'] == self.group:                    
                    res = self.main_db.find(
                        {
                            'ship_imo': self.ship_imo
                        },
                        {
                            "ship_imo": 1,
                            "processed_daily_data."+grp['name']: 1,
                            "_id": 0
                        }
                    ).sort("processed_daily_data.rep_dt.processed", ASCENDING) # Sort dates in the ascending order
                    mainres.append(res)
                ''' FOR TEMPORARY PURPOSES'''
                if 'Multi Axis' in self.group:
                    if grp['groupname'] == 'COMBUSTION PROCESS':
                        if 'Combst' in singledata[vars]['short_names'] and '1' in singledata[vars]['short_names']:
                            res = self.main_db.find(
                                {
                                    'ship_imo': self.ship_imo
                                },
                                {
                                    "ship_imo": 1,
                                    "processed_daily_data."+grp['name']: 1,
                                    "_id": 0
                                }
                            ).sort("processed_daily_data.rep_dt.processed", ASCENDING) # Sort dates in the ascending order
                            mainres.append(res)
                        if 'ME Exh Temp' in singledata[vars]['short_names'] and '1' in singledata[vars]['short_names']:
                            res = self.main_db.find(
                                {
                                    'ship_imo': self.ship_imo
                                },
                                {
                                    "ship_imo": 1,
                                    "processed_daily_data."+grp['name']: 1,
                                    "_id": 0
                                }
                            ).sort("processed_daily_data.rep_dt.processed", ASCENDING) # Sort dates in the ascending order
                            mainres.append(res)

        self.maindb_res = mainres
    
    def process_data(self):

        if('Multi Axis' in self.group):
            ''' TEMPORARY PURPOSES'''
            datadict, expecteddict, nameslist = self.configuration.temporary_dict_and_list_according_to_groups(self.group)
            group = self.configuration.temporary_group_selection_for_11(self.group)
        else:
            datadict, expecteddict, nameslist = self.configuration.get_dict_and_list_according_to_groups(self.group)
            group = self.configuration.get_group_selection(self.group)
        mainres = loads(dumps(self.maindb_res))
        lowerdict = {}
        upperdict = {}
        outlierdict = {}
        for i in range(len(mainres)):
            newlist = []
            explist = []
            upperlist = []
            lowerlist = []
            outlierlist = []
            expoutlierlist = []
            for j in range(0, len(mainres[i])):
                try:
                    if nameslist[i] == 'rep_dt':
                        newdate = mainres[i][j]['processed_daily_data'][nameslist[i]]['processed'].strftime("%Y-%m-%d")
                        newlist.append(newdate)
                        explist.append(newdate)
                    else:
                        # tempNum = self.get_precise_decimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['processed'])
                        # print("DECIMAL",tempNum)
                        # if mainres[i][j]['processed_daily_data'][nameslist[i]]['within_outlier_limits'] == False:
                        #     outlierlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['processed']))
                            # if self.duration == '30Days':
                            #     expoutlierlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][1]))
                            # if self.duration == '90Days':
                            #     expoutlierlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][1]))
                            # if self.duration == '1Year':
                            #     expoutlierlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][1]))
                        # else:
                        newlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['processed']))
                        if self.duration == '30Days':
                            if mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][1] > 0 or mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][1] < 0:
                                explist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][1]))
                            #Lower interval
                            if mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][0] > 0 or mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][0] < 0:
                                lowerlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][0]))
                            if -np.inf in lowerlist:
                                lowerlist.remove(-np.inf)
                            #Upper interval
                            if mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][2] > 0 or mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][2] < 0:
                                upperlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m3'][2]))
                            if np.inf in upperlist:
                                upperlist.remove(np.inf)
                        if self.duration == '90Days':
                            if mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][1] > 0 or mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][1] < 0:
                                explist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][1]))
                            #Lower interval
                            if mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][0] > 0 or mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][0] < 0:
                                lowerlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][0]))
                            if -np.inf in lowerlist:
                                lowerlist.remove(-np.inf)
                            #Upper interval
                            if mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][2] > 0 or mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][2] < 0:
                                upperlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m6'][2]))
                            if np.inf in upperlist:
                                upperlist.remove(np.inf)
                        if self.duration == '1Year':
                            if mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][1] > 0 or mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][1] < 0:
                                explist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][1]))
                            #Lower interval
                            if mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][0] > 0 or mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][0] < 0:
                                lowerlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][0]))
                            if -np.inf in lowerlist:
                                lowerlist.remove(-np.inf)
                            #Upper interval
                            if mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][2] > 0 or mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][2] < 0:
                                upperlist.append(self.configuration.makeDecimal(mainres[i][j]['processed_daily_data'][nameslist[i]]['predictions']['m12'][2]))
                            if np.inf in upperlist:
                                upperlist.remove(np.inf)
                except KeyError:
                    continue
                except TypeError:
                    continue
            datadict[nameslist[i]] = newlist
            expecteddict['expected_'+nameslist[i]] = explist
            lowerdict[nameslist[i]] = lowerlist
            upperdict[nameslist[i]] = upperlist
            # outlierdict[nameslist[i]] = outlierlist
        ''' Deleting all the variables that do not have expected, lower, and upper values'''
        for k in expecteddict.copy():
            if not expecteddict[k]:
                del expecteddict[k]
        for k in lowerdict.copy():
            if not lowerdict[k]:
                del lowerdict[k]
        for k in upperdict.copy():
            if not upperdict[k]:
                del upperdict[k]
        for k in outlierdict.copy():
            if not outlierdict[k]:
                del outlierdict[k]

        result, min_max_dict = self.configuration.get_loaded_and_ballast_data(datadict, self.group)
        return group, datadict, expecteddict, lowerdict, upperdict, outlierdict, result, min_max_dict
    
    def read_for_fuel_consumption(self):
        mainres = []
        singledata = self.ship_configs.find_one()['data']
        fuelSelection = self.configuration.get_selection_for_fuel_consumption()

        # res = self.db_aranti.Main_db.find(
        #     {},
        #     {
        #         "processed_daily_data.rep_dt.processed": 1,
        #         "_id": 0
        #     }
        # ).sort("processed_daily_data.rep_dt.processed", ASCENDING)
        # mainres.append(res)

        for vars in fuelSelection:
            key = list(vars.keys())[0]
            res = self.main_db.find(
                {
                    'ship_imo': self.ship_imo
                },
                {
                    "processed_daily_data.rep_dt.processed": 1,
                    "processed_daily_data."+key+".processed": 1,
                    "_id": 0
                }
            ).sort("processed_daily_data.rep_dt.processed", ASCENDING)
            mainres.append(res)
        return mainres
    
    def process_fuel_consumption(self):
        ''' Returns the dictionary of all the variables for Fuel Consumption sorted by date(rep_dt)'''
        fuelSelection = self.configuration.get_selection_for_fuel_consumption()
        fuelcursor = self.read_for_fuel_consumption()
        fuelres = loads(dumps(fuelcursor))
        # print(len(fuelres))
        fuelresList=[]
        # pp = pprint.PrettyPrinter(indent=4)
        # pp.pprint(fuelres)
        
        for i in range(0, len(fuelres[0])):
            newdate = fuelres[0][i]['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
            dateKey = list(fuelres[0][i]['processed_daily_data'].keys())[0]
            temp = {}
            for j in range(1, len(fuelres)):
                for k in range(0, len(fuelres[j])):
                    try:
                        if newdate == fuelres[j][k]['processed_daily_data'][dateKey]['processed'].strftime('%Y-%m-%d'):
                            varKey = list(fuelres[j][k]['processed_daily_data'].keys())[1]
                            temp[fuelSelection[0][dateKey]] = newdate
                            temp[fuelSelection[j][varKey]] = fuelres[j][k]['processed_daily_data'][varKey]['processed']
                    except IndexError:
                        continue
                    except KeyError:
                        continue
            fuelresList.append(temp)

        
        return fuelresList


    def get_generic_group_list(self):
        groupList = self.configuration.get_generic_group_selection()
        return groupList
    
    def get_chart_height(self):
        height = self.configuration.get_height_of_chart(self.group)
        return height
    
    def get_height_of_chart_after_double_click(self):
        height_after_double_click = self.configuration.get_height_of_chart_after_double_click(self.group)
        return height_after_double_click
    
    def get_short_names_list_for_multi_axis(self):
        namesList = self.configuration.get_short_names_list_for_multi_axis(self.group)
        return namesList
    
    def get_Y_position(self):
        y_position = self.configuration.get_Y_position_for_legend(self.group)
        return y_position
    
    def get_Y_position_after_double_click(self):
        y_position_after_double_click = self.configuration.get_Y_position_for_legend_after_double_click(self.group)
        return y_position_after_double_click

    def variable_list_according_to_order_blocks(self):
        variables_list = self.configuration.get_variables_list_in_order_of_blocks(self.group)
        return variables_list
    
    def individual_parameters(self):
        individual_params = self.configuration.get_individual_parameters()
        return individual_params
            
        


# Remove later
# res = TrendsExtractor(9591301, 'BASIC', '30Days')
# res.connect()
# res.process_fuel_consumption()
# res.do_steps()
# print(grpresult)
# print(mainresult)