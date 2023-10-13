# all main function of react application backend, all these functions are called in application.py. this file inherits/calls other functions from ither fiels in config_extractor_folder

from decimal import DivisionByZero
# from math import isfinite, isnan
import sys
import os
from dotenv import load_dotenv
# import re
# from numpy.core.numeric import NaN
import pandas as pd
sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
from src.db.setup_mongo import connect_db
# from src.configurations.logging_config import CommonLogger
from datetime import datetime, timedelta
# from dateutil.relativedelta import relativedelta
import numpy as np
from pymongo import MongoClient
from bson.json_util import dumps, loads
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.preprocessing import PolynomialFeatures
# from src.processors.config_extractor.prediction_interval import LRPI
from pymongo import ASCENDING, DESCENDING
import math
import random
import scipy.stats as st
import re
# import base64
# from pyearth import Earth

load_dotenv()

# client = MongoClient("mongodb://localhost:27017/")
# db=client.get_database("aranti")
client = MongoClient(os.getenv('MONGODB_ATLAS'))
db=client.get_database("aranti")
# db=client.get_database("aranti_copy")
database = db

# Constants
height_per_block = 300
height_per_block_after_double_click = height_per_block * 2.5
default_height = 650
default_Y_position = -0.05
common_header = ['Name', "Unit", 'Reported', 'Expected', 'Statement', 'Cause', 'P']
daily_report_column_headers = {
    'VESSEL PARTICULARS': ["Name", "Constant"],
    'VESSEL STATUS': ["Name", "Unit", "Reported", "Expected", "Charter Pty", "Statement"],
    'CHANGE IN SPEED': ["Name", "Unit", "Reported", "Expected", "Statement"],
    'WEATHER PARAMETERS': ["Name", "Unit", "Reported", "AIS/MET", "Statement", "API_DATA"],
    'DISTANCE AND TIME': ["Name", "Unit", "Reported", "AIS", "Expected", "Statement", "API_DATA"],
    'VESSEL POSITION': ["Name", "Reported", "AIS", "Expected"],
    'FUEL OIL CONSUMPTION': ['Name', "Unit", 'Reported', 'Expected', 'Statement', 'Cause', 'P'],
    'MAIN ENGINE': ['Name', "Unit", 'Reported', 'Expected', 'Statement', 'Cause', 'P'],
    'GENERATOR': ['Name', "Unit", 'Reported', 'Expected', 'Statement', 'Cause', 'P'],
    'AUXILLIARIES': ['Name', "Unit", 'Reported', 'Expected', 'Statement', 'Cause', 'P'],
    'INDICES': ['Name', "Unit", 'Calculated', 'Expected', 'Threshold', 'Statement', 'Cause', 'Feedback']
}
daily_issues_column_headers = {
    'VESSEL PARTICULARS': ["Name", "Constant"],
    'VESSEL STATUS': ["Name", "Unit", "Reported", "Occurrence","Expected", "Charter Pty", "Statement"],
    'CHANGE IN SPEED': ["Name", "Unit", "Reported", "Occurrence","Expected", "Statement"],
    'WEATHER PARAMETERS': ["Name", "Unit", "Reported","Occurrence", "AIS/MET", "Statement", "API_DATA"],
    'DISTANCE AND TIME': ["Name", "Unit", "Reported","Occurrence", "AIS", "Expected", "Statement", "API_DATA"],
    'VESSEL POSITION': ["Name", "Reported","Occurrence", "AIS", "Expected"],
    'FUEL OIL CONSUMPTION': ['Name', "Unit", 'Reported',"Occurrence", 'Expected', 'Statement', 'P'],
    'MAIN ENGINE': ['Name', "Unit", 'Reported',"Occurrence", 'Expected', 'Statement', 'P'],
    'GENERATOR': ['Name', "Unit", 'Reported',"Occurrence", 'Expected', 'Statement', 'P'],
    'AUXILLIARIES': ['Name', "Unit", 'Reported',"Occurrence", 'Expected', 'Statement', 'P'],
    'INDICES': ['Name', "Unit", 'Calculated', 'Expected', 'Threshold', 'Statement', 'Feedback']
}
position_of_collapsible_category = {
    'VESSEL PARTICULARS': 0,
    'VESSEL STATUS': 1,
    'CHANGE IN SPEED': 2,
    'WEATHER PARAMETERS': 3,
    'DISTANCE AND TIME': 4,
    'VESSEL POSITION': 5,
    'FUEL OIL CONSUMPTION': 6,
    'MAIN ENGINE': 7,
    'GENERATOR': 8,
    'AUXILLIARIES': 9,
    'INDICES': 10
}
duration_mapping = {
    # '30Days': 90,
    '180Days': 180,
    '1Year': 365,
    'Lastyear180': 545,
    'Lastyear': 730
}

performance_analysis_identifier_mapping = {
    'Date (yyyy-mm-dd)': 'final_rep_dt',
    'Observed Distance (nautical miles)': 'odist',
    'Steaming Time (h)': 'stm_hrs',
    'Engine Distance (nautical miles)': 'edist',
    'Slip (%)': 'slip_o',
    "M/E (rpm)": 'rpm',
    "M/E LUB Cons (l)": '', #doubtful Lub OIl COns
    "F/W Cons (t)": '' #doubtful Fresh Water cons
}

weather_data_identifier_mapping = {
    "Date (yyyy-mm-dd)": 'final_rep_dt',
    "Significant Wave Height (m)": 'swell',
    "Wave Direction (deg)": 'swelldir_deg',
    "Wind Speed (knots)": 'w_force', #doubtful
    "Wind Direction (deg)": 'w_dir_deg',
    "Current Speed (m/s)": 'curknots', #doubtful
    "Current Direction (deg)": 'current_dir_rel' #doubtful
}

FOC_identifier_mapping = {
    "Date (yyyy-mm-dd)": 'final_rep_dt',
    "24 h Corrected F.O.C (t)": 'main_fuel', #doubtful
    "Weather corrected Baseline (t)": '',
    "Torque Rich Index": '',
    "Difference (%)": '',
    "Displacement (t)": 'displ',
    "Draft Aft (m)": 'draft_aft',
    "Draft Fwd (m)": 'draft_fwd',
    "Trim (m)": 'trim'
}
# TEMPORARY UNTIL m3 ADDED IN DB
# temporaryDurationActual = 'm6'

class Configurator():

    def __init__(self,ship_imo):
        self.ship_imo=ship_imo
        self.ship_configs={}
        
    def connect_db(self):
        self.database = connect_db()   #to be used later

    def get_ship_configs(self):  
        ship_configs_collection = database.get_collection("ship")
        self.ship_configs = ship_configs_collection.find_one({"ship_imo": self.ship_imo})
        return ship_configs_collection # Use this variable in any file configurator object exists.

    def get_main_data(self):  
        maindb_collection = database.get_collection("Main_db")
        self.maindb = maindb_collection.find({"ship_imo": self.ship_imo}).sort('processed_daily_data.rep_dt.processed', ASCENDING)
        return maindb_collection # Use this variable in any file configurator object exists.
    
    def get_login_data(self):
        login_info = database.get_collection("login_info")
        return login_info
    
    def get_daily_data(self):
        '''
            Returns the daily_data collection
        '''
        daily_collection = database.get_collection("daily_data")
        return daily_collection
    
    def get_ais_collection(self):
        '''
            Returns the AIS collection
        '''
        ais_collection = database.get_collection("Ais_collection")
        return ais_collection

    def static_lists(self):
        self.stat_var_list=[]
        for i,j in self.ship_configs['static_data'].items():
            self.stat_var_list.append(str(i))
        return self.stat_var_list
    
    def get_static_data(self):
        ship_config_collection = self.get_ship_configs()
        result = None

        for doc in ship_config_collection.find({'ship_imo': self.ship_imo}).sort('ship_imo', ASCENDING):
            result = doc['static_data']
        return result
    
    def get_identifier_mapping(self):
        ship_collection = self.get_ship_configs()
        result = {}
        for doc in ship_collection.find({'ship_imo': self.ship_imo}).sort('ship_imo', ASCENDING):
            result = doc['identifier_mapping']
        
        return result

    def engine_var_list(self):
        self.engine_var_list=[]
        for i in range (0,len(self.ship_configs['data_available_engine'])):
            if type(self.ship_configs['data_available_engine'][i])==str:
                self.engine_var_list.append(self.ship_configs['data_available_engine'][i])
        return self.engine_var_list

    def nav_var_list(self):
        self.nav_var_list=[]
        for i in range (0,len(self.ship_configs['data_available_nav'])):
            if type(self.ship_configs['data_available_nav'][i])==str:
                self.nav_var_list.append(self.ship_configs['data_available_nav'][i])
        return self.nav_var_list
    
    def ais_api_var_list(self):
        self.ais_api_var_list=[]
        for i in range (0,len(self.ship_configs['ais_api_data'])):
            if type(self.ship_configs['ais_api_data'][i])==str:
                self.ais_api_var_list.append(self.ship_configs['ais_api_data'][i])
        return self.ais_api_var_list

    def calculated_var_list(self):
        self.calculated_var_list=[]
        for i in range (0,len(self.ship_configs['calculated_var'])):
            if type(self.ship_configs['calculated_var'][i])==str:
                self.calculated_var_list.append(self.ship_configs['calculated_var'][i])
        return self.calculated_var_list
    
    def ewma_divide(self, ewma_val,ewma_limit):
        if pd.isnull(ewma_val)==False and pd.isnull(ewma_limit)==False:
            try:
                new_ewma_val=ewma_val/ewma_limit
            except DivisionByZero:
                new_ewma_val = ewma_val
            return new_ewma_val
        else:
            return None


    def spe_divide(self, spe_val,spe_limit):
        if pd.isnull(spe_val)==False and pd.isnull(spe_limit)==False:
            try:
                new_spe_val=spe_val/(spe_limit*0.8)
            except DivisionByZero:
                new_spe_val = spe_val
            return new_spe_val
        else:
            return None

    def t2_divide(self, t2_val,t2_limit):
        if pd.isnull(t2_val)==False and pd.isnull(t2_limit)==False:
            try:
                new_t2_val=t2_val/(t2_limit*0.65)
            except DivisionByZero:
                new_t2_val = t2_val
            return new_t2_val
        else:
            return None

    def makeDecimal(self, number, sliderRange=False):
        ''' Returns the number rounded to required decimal places. Also used to create slider marks numbers.'''
        if sliderRange == True:
            if (number > 0 and number <= 9) or (number < 0 and number >= -9):
                return round(number, 2)
            if number > 9 and number < 100:
                return round(number, 1)

            elif number >= 100 and number < 1000:
                return round(number)
            else:
                return round(number)
        else:
            if (number > 0 and number <= 9) or (number < 0 and number >= -9):
                return round(number, 3)
            if number > 9 and number < 100:
                return round(number, 2)

            elif number >= 100 and number < 1000:
                return round(number, 1)
            else:
                return round(number, 1)
    
    def count_words_in_a_string(self, string):
        word_count = 1
        for i in range(len(string)):
            if(string[i] == ' ' or string == '\n' or string == '\t'):
                word_count += 1
        return word_count

    def text_wrapping(self, string):
        word_count = self.count_words_in_a_string(string)
        stringList = string.split(' ')
        if word_count > 3:
            stringList.insert(3, '<br />')
        
        new_string = ' '.join(stringList)
        return new_string
    
    def create_current_duration(self, duration):
        ''' Returns the actual duration as in database.'''
        if duration == '90Days':
            # Currently m3 unavailable. Change back when it is.
            durationActual = 'm3'
        elif duration == '180Days':
            durationActual = 'm6'
        elif duration == '1Year':
            durationActual = 'm12'
        elif duration == 'Lastyear30':
            durationActual = 'ly_m3'
        elif duration == 'Lastyear180':
            durationActual = 'ly_m6'
        else:
            durationActual = 'ly_m12'
        return durationActual
    
    def get_dict_for_ships(self, id):
        ''' Returns list of dictionaries with the ship imo as key and the ship name as values'''
        ship_collection = self.get_ship_configs()
        result=[]
        if id == "":
            for doc in ship_collection.find().sort('ship_imo', ASCENDING):
                ship_imo = doc['ship_imo']
                ship_name = doc['static_data']['ship_name']['value'].strip()
                temp = {'value': ship_imo, 'label': ship_name}
                result.append(temp)
        else:
            for doc in ship_collection.find({'organization_id': {"$in": [id, 'default']}}).sort('ship_imo', ASCENDING):
                ship_imo = doc['ship_imo']
                ship_name = doc['static_data']['ship_name']['value'].strip()
                temp = {'value': ship_imo, 'label': ship_name}
                result.append(temp)
        
        return result
    
    def get_dependent_parameters(self, id):
        ''' Returns the list of dependent parameters.'''
        result = []
        ship_collection = self.get_ship_configs()

        for doc in ship_collection.find({'ship_imo': self.ship_imo}, {'data': 1}):
            for key in doc['data'].keys():
                if doc['data'][key]['dependent'] == True:
                    if pd.isnull(doc['data'][key]['source_idetifier']) == False:
                        temp = {'id': id, 'value': key, 'label': doc['data'][key]['short_names']}
                        result.append(temp)
                    elif pd.isnull(doc['data'][key]['static_data']) == False:
                        temp = {'id': id, 'value': key, 'label': doc['data'][key]['short_names']}
                        result.append(temp)
        result.append({'id': id, 'value': 'speed_sog_calc', 'label': 'Speed SOG'})
        result.append({'id': id, 'value': 'speed_stw_calc', 'label': 'Speed STW (Calc)'})
        result.append({'id': id, 'value': 'None', 'label': 'None'})
        return result
    
    def get_independent_parameters(self, id):
        ''' Returns the list of independent parameters.'''
        result = []
        ship_collection = self.get_ship_configs()

        for doc in ship_collection.find({'ship_imo': self.ship_imo}, {'data': 1}):
            for key in doc['data'].keys():
                if key != 'rep_dt' and key != 'rep_time' and key != 'vsl_load_bal':
                    if doc['data'][key]['dependent'] == False:
                        temp = {'id': id, 'value': key, 'label': doc['data'][key]['short_names']}
                        result.append(temp)
        result.append({'id': id, 'value': 'None', 'label': 'None'})
        return result
    
    def get_params_and_their_unit(self):
        ''' Returns dictionary for unit of each parameter as value and the parameter name as key.'''
        result = {}
        ship_collection = self.get_ship_configs()
        for doc in ship_collection.find({}).sort('ship_imo', ASCENDING):
            tempDict = {}
            singledata = doc['data']
            for var in singledata.keys():
                tempDict[var] = singledata[var]['unit']
            result[doc['ship_imo']] = tempDict
        
        return result
    
    def create_dict_for_params_and_their_blocknos(self, group):
        ''' Returns a dictionary where the param name is key and the block number is the value.'''
        result = {}

        for i in range(len(group)):
            result[group[i]['name']] = int(group[i]['block_number'])
        
        return result
    
    def check_if_param_in_spe_subgroup(self, group, spekeylist, param):
        ''' Returns if the "param" belongs to the subgroup which has a spe parameter.'''
        block_number_of_param=None
        param_in_spe_subgroup = False

        if 'spe_' in param:
            param_in_spe_subgroup = False
        else:
            for i in range(len(group)):
                if group[i]['name'] == param:
                    block_number_of_param = group[i]['block_number']
            for i in range(len(group)):
                if 'spe_' + group[i]['name'] in spekeylist and group[i]['block_number'] == block_number_of_param:
                    param_in_spe_subgroup = True
        return param_in_spe_subgroup

        
    
    def get_sister_vessel(self):
        ''' Returns the list of all the sister vessels of a particular ship.'''
        ship_collection = self.get_ship_configs()
        result = []

        for doc in ship_collection.find({'ship_imo': self.ship_imo}).sort('ship_imo', ASCENDING):
            for vessel in range(len(doc['sister_vessel_list'])):
                temp = {'label': doc['sister_vessel_list_names'][vessel], 'value': doc['sister_vessel_list'][vessel]}
                result.append(temp)
        
        result.append({'label': 'None', 'value': 'None'})
        return result
    
    def get_similar_vessel(self):
        ''' Returns the list of all the vessels similar to a particular ship.'''
        ship_collection = self.get_ship_configs()
        result = []

        for doc in ship_collection.find({'ship_imo': self.ship_imo}).sort('ship_imo', ASCENDING):
            for vessel in range(len(doc['similar_vessel_list'])):
                temp = {'label': doc['similar_vessel_list_names'][vessel], 'value': doc['similar_vessel_list'][vessel]}
                result.append(temp)
        
        result.append({'label': 'None', 'value': 'None'})
        return result



    ''' Functions for Trends processing '''
    def get_group_selection(self,groupname):
        '''For getting the groups list for particular group'''
        result = []
        singledata = self.ship_configs['data']
        for var in singledata.keys():            
            groups = singledata[var]['group_selection']
            for group in groups:
                # if group['groupname'] == groupname:
                if group['groupnumber'] == groupname:
                    tempName = self.text_wrapping(var) if type(singledata[var]['short_names']) == float else self.text_wrapping(singledata[var]['short_names'])
                    temp = {'short_names': tempName}
                    group.update(temp)
                    result.append(group)
        return result
    
    def get_group_selection_for_individual_parameters(self, individual_params_list, groupname=""):
        ''' Returns the groups list for individual parameters similar what get_group_selection() returns'''
        individual_group_selection={}
        shortNameDict = self.create_short_names_dictionary()
        # unitDict = self.get_params_and_their_unit()
        groupsList=[]
        if groupname != '':
            groups = self.get_group_selection(groupname)
            blocks = self.get_number_of_blocks(groupname)
            maxBlock = max(blocks)

            if type(individual_params_list) == str:
                shortName = self.text_wrapping(shortNameDict[individual_params_list])
                individual_group_selection['name'] = individual_params_list
                individual_group_selection['group_availability_code'] = 1
                individual_group_selection['block_number'] = maxBlock + 1
                individual_group_selection['short_names'] = shortName
                groups.append(individual_group_selection)
                return groups
            else:
                newMaxBlock = maxBlock + 1
                for i in range(len(individual_params_list)):
                    if i > 1:
                        newMaxBlock = newMaxBlock + 1
                    if individual_params_list[i] != 'rep_dt':
                        individual_group_selection={}
                        shortName = self.text_wrapping(shortNameDict[individual_params_list[i]])
                        individual_group_selection['name'] = individual_params_list[i]
                        individual_group_selection['group_availability_code'] = 1
                        individual_group_selection['block_number'] = newMaxBlock
                        individual_group_selection['short_names'] = shortName
                        groups.append(individual_group_selection)
                return groups
        else:
            for i in range(len(individual_params_list)):
                if individual_params_list[i] != 'rep_dt':
                    individual_group_selection={}
                    shortName = self.text_wrapping(shortNameDict[individual_params_list[i]])
                    individual_group_selection['name'] = individual_params_list[i]
                    individual_group_selection['group_availability_code'] = 1
                    individual_group_selection['block_number'] = i + 1
                    individual_group_selection['short_names'] = shortName
                    groupsList.append(individual_group_selection)
            return groupsList
    
    def get_corrected_group_selection(self, oldgroup):
        ''' Returns the corrected groups list by excluding the parameters that don't have values. Also corrects the block numbers'''
        block = self.get_number_of_blocks('', oldgroup) if oldgroup[0]['groupname'] == 'COMBUSTION PROCESS' else []
        # name_and_block_dict = self.create_dict_for_params_and_their_blocknos(oldgroup)
        # legendNames = ['ME Scav Press & Combst-Compr Press', 'SPE ME Scav Press', 'Combst-Peak Press', 'ME Exh Temp', 'SPE ME Exh Temp', 'Air Temp B4 Cooler & ME Scav Temp', 'SPE Air Temp B4 Cooler & SPE ME Scav Temp', 'Air Cooler Pres Drop', 'SPE Air Cooler Pres Drop', 'Air Cooler SW In/Out Temp & Air Cooler Water Sep']
        #print("BLOCK WITH NEWGROUP", block)
        # newgroup = []
        if len(block) > 0:
            for i in range(len(block)):#1, 2, 9, 10, 11, 12, 13
                for j in range(len(oldgroup)):
                    if oldgroup[j]['block_number'] == block[i]:
                        oldgroup[j]['block_number'] = i + 1

        return oldgroup
    
    def get_group_selection_including_spe(self, group, datadict, spedict):
        ''' Returns the groups list by including the spe for dependent parameters, and correcting the block numbers.'''
        #print("START DEPENDENT PARAMS")
        # spe_list = self.get_dependent_parameters()
        #print("END DEPENDENT PARAMS")
        group_of_only_params_with_values = []
        spekeylist = list(spedict.keys())
        datakeylist = list(datadict.keys())
        name_and_block_dict = self.create_dict_for_params_and_their_blocknos(group)
        sorted_group = []
        newgroup = []
        block_num = []
        # block_num = 0
        for i in range(len(group)):
            tempname = 'spe_' + group[i]['name']
            if group[i]['name'] in datakeylist or tempname in spekeylist:
                group_of_only_params_with_values.append(group[i])
        block = self.get_number_of_blocks('', group_of_only_params_with_values)
        #print("START SORT GROUP")
        for i in block:
            for j in range(len(group_of_only_params_with_values)):
                if group_of_only_params_with_values[j]['block_number'] == i:
                    sorted_group.append(group_of_only_params_with_values[j])
        #print("SORTED GROUP", sorted_group)
        #print("END SORT GROUP")
        # param_in_spe_subgroup_dict = self.check_if_param_in_spe_subgroup(sorted_group, spekeylist, '')
        # #print("SPE SUBGROUP DICT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", param_in_spe_subgroup_dict)
        #print("START SPE INSIDE")
        check_list=[]
        for i in range(len(sorted_group)):
            tempName = 'spe_' + sorted_group[i]['name']
            print(tempName)

            if tempName in spekeylist:
                if sorted_group[i]['block_number'] == 1:
                    newgroup.append(sorted_group[i])
                    temp = {}
                    temp['name'] = 'spe_' + sorted_group[i]['name']
                    temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                    temp['block_number'] = sorted_group[i]['block_number'] + 1
                    temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                    newgroup.append(temp)
                else:
                    print(newgroup)
                    if 'spe_' in newgroup[-1]['name']:
                        if name_and_block_dict[sorted_group[i]['name']] == name_and_block_dict[newgroup[-2]['name']]:
                            sorted_group[i]['block_number'] = newgroup[-2]['block_number']
                            newgroup.append(sorted_group[i])
                            temp = {}
                            temp['name'] = 'spe_' + sorted_group[i]['name']
                            temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                            # temp['block_number'] = sorted_group[i]['block_number'] + 1
                            temp['block_number'] = newgroup[-1]['block_number'] + 1
                            temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                            newgroup.append(temp)
                        else:
                            sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                            newgroup.append(sorted_group[i])
                            temp = {}
                            temp['name'] = 'spe_' + sorted_group[i]['name']
                            temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                            # temp['block_number'] = sorted_group[i]['block_number'] + 1
                            temp['block_number'] = newgroup[-1]['block_number'] + 1
                            temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                            newgroup.append(temp)
                    else:
                        spe_subgroup_check = self.check_if_param_in_spe_subgroup(sorted_group, spekeylist, newgroup[-1]['name'])
                        if name_and_block_dict[sorted_group[i]['name']] == name_and_block_dict[newgroup[-1]['name']]:
                            sorted_group[i]['block_number'] = newgroup[-1]['block_number']
                            newgroup.append(sorted_group[i])
                            temp = {}
                            temp['name'] = 'spe_' + sorted_group[i]['name']
                            temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                            # temp['block_number'] = sorted_group[i]['block_number'] + 1
                            temp['block_number'] = newgroup[-1]['block_number'] + 1
                            temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                            newgroup.append(temp)
                        elif spe_subgroup_check == True:
                        # elif newgroup[-1]['name'] in param_in_spe_subgroup_dict.keys() and param_in_spe_subgroup_dict[newgroup[-1]['name']] == True:
                            sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 2
                            newgroup.append(sorted_group[i])
                            temp = {}
                            temp['name'] = 'spe_' + sorted_group[i]['name']
                            temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                            # temp['block_number'] = sorted_group[i]['block_number'] + 1
                            temp['block_number'] = newgroup[-1]['block_number'] + 1
                            temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                            newgroup.append(temp)
                        else:
                            sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                            newgroup.append(sorted_group[i])
                            temp = {}
                            temp['name'] = 'spe_' + sorted_group[i]['name']
                            temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                            # temp['block_number'] = sorted_group[i]['block_number'] + 1
                            temp['block_number'] = newgroup[-1]['block_number'] + 1
                            temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                            newgroup.append(temp)
            else:
                if sorted_group[i]['block_number'] == 1:
                    newgroup.append(sorted_group[i])
                else:
                    try:
                        if 'spe_' in newgroup[-1]['name']:
                            if name_and_block_dict[sorted_group[i]['name']] == name_and_block_dict[newgroup[-2]['name']]:
                                sorted_group[i]['block_number'] = newgroup[-2]['block_number']
                                newgroup.append(sorted_group[i])
                            else:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                                newgroup.append(sorted_group[i])
                        else:
                            spe_subgroup_check = self.check_if_param_in_spe_subgroup(sorted_group, spekeylist, newgroup[-1]['name'])
                            if name_and_block_dict[sorted_group[i]['name']] == name_and_block_dict[newgroup[-1]['name']]:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number']
                                newgroup.append(sorted_group[i])
                            elif spe_subgroup_check == True:
                            # elif newgroup[-1]['name'] in param_in_spe_subgroup_dict.keys() and param_in_spe_subgroup_dict[newgroup[-1]['name']] == True:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 2
                                newgroup.append(sorted_group[i])
                            else:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                                newgroup.append(sorted_group[i])
                    except IndexError:
                        newgroup.append(sorted_group[i])
        #     if tempName in spekeylist:
        #         #print("SPE PARAMETERS NAME!!!!!!!!", sorted_group[i]['name'])
        #         if sorted_group[i]['block_number'] == 1:
        #             newgroup.append(sorted_group[i])
        #             check_list.append(sorted_group[i]['name'])
        #             temp = {}
        #             temp['name'] = 'spe_' + sorted_group[i]['name']
        #             temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #             temp['block_number'] = sorted_group[i]['block_number'] + 1
        #             temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #             newgroup.append(temp)
        #             check_list.append('spe_' + sorted_group[i]['name'])
        #         else:
        #             try:
        #                 if newgroup[-1]['name'] in spekeylist:
        #                     try:
        #                         if name_and_block_dict[newgroup[-2]['name']] == name_and_block_dict[sorted_group[i]['name']]:
        #                             sorted_group[i]['block_number'] = newgroup[-2]['block_number']
        #                             newgroup.insert(-1, sorted_group[i])
        #                             check_list.insert(-1, sorted_group[i]['name'])
        #                             temp = {}
        #                             temp['name'] = 'spe_' + sorted_group[i]['name']
        #                             temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                             temp['block_number'] = newgroup[-1]['block_number']
        #                             temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                             newgroup.append(temp)
        #                             check_list.append('spe_' + sorted_group[i]['name'])
        #                         elif name_and_block_dict[newgroup[-3]['name']] == name_and_block_dict[sorted_group[i]['name']]:
        #                             sorted_group[i]['block_number'] = newgroup[-3]['block_number']
        #                             newgroup.insert(-1, sorted_group[i])
        #                             check_list.insert(-1, sorted_group[i]['name'])
        #                             temp = {}
        #                             temp['name'] = 'spe_' + sorted_group[i]['name']
        #                             temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                             temp['block_number'] = newgroup[-2]['block_number']
        #                             temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                             newgroup.append(temp)
        #                             check_list.append('spe_' + sorted_group[i]['name'])
        #                         else:
        #                             sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
        #                             newgroup.append(sorted_group[i])
        #                             check_list.append(sorted_group[i]['name'])
        #                             temp = {}
        #                             temp['name'] = 'spe_' + sorted_group[i]['name']
        #                             temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                             temp['block_number'] = newgroup[-1]['block_number'] + 1#
        #                             temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                             newgroup.append(temp)
        #                             check_list.append('spe_' + sorted_group[i]['name'])
        #                     except KeyError:
        #                         # print("IN CASE OF KEY ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        #                         if name_and_block_dict[newgroup[-1]['name'].replace('spe_', '')] == name_and_block_dict[sorted_group[i]['name']]:
        #                             sorted_group[i]['block_number'] = newgroup[-3]['block_number']
        #                             newgroup.insert(-1, sorted_group[i])
        #                             check_list.insert(-1, sorted_group[i]['name'])
        #                             temp = {}
        #                             temp['name'] = 'spe_' + sorted_group[i]['name']
        #                             temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                             temp['block_number'] = newgroup[-3]['block_number'] + 1
        #                             temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                             newgroup.append(temp)
        #                             check_list.append('spe_' + sorted_group[i]['name'])
        #                         elif name_and_block_dict[newgroup[-2]['name'].replace('spe_', '')] == name_and_block_dict[sorted_group[i]['name']]:
        #                             sorted_group[i]['block_number'] = newgroup[-2]['block_number']
        #                             newgroup.insert(-1, sorted_group[i])
        #                             check_list.insert(-1, sorted_group[i]['name'])
        #                             temp = {}
        #                             temp['name'] = 'spe_' + sorted_group[i]['name']
        #                             temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                             temp['block_number'] = newgroup[-1]['block_number']
        #                             temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                             newgroup.append(temp)
        #                         else:
        #                             sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
        #                             newgroup.append(sorted_group[i])
        #                             check_list.append(sorted_group[i]['name'])
        #                             temp = {}
        #                             temp['name'] = 'spe_' + sorted_group[i]['name']
        #                             temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                             temp['block_number'] = newgroup[-1]['block_number'] + 1#
        #                             temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                             newgroup.append(temp)
        #                             check_list.append('spe_' + sorted_group[i]['name'])
        #                 else:
        #                     try:
        #                         if name_and_block_dict[newgroup[-1]['name']] == name_and_block_dict[sorted_group[i]['name']]:
        #                             sorted_group[i]['block_number'] = newgroup[-1]['block_number']
        #                             newgroup.insert(-1, sorted_group[i])
        #                             check_list.insert(-1, sorted_group[i]['name'])
        #                             temp = {}
        #                             temp['name'] = 'spe_' + sorted_group[i]['name']
        #                             temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                             temp['block_number'] = newgroup[-1]['block_number'] + 1#
        #                             temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                             newgroup.append(temp)
        #                             check_list.append('spe_' + sorted_group[i]['name'])
        #                         else:
        #                             sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
        #                             newgroup.append(sorted_group[i])
        #                             check_list.append(sorted_group[i]['name'])
        #                             temp = {}
        #                             temp['name'] = 'spe_' + sorted_group[i]['name']
        #                             temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                             temp['block_number'] = newgroup[-1]['block_number'] + 1#
        #                             temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                             newgroup.append(temp)
        #                             check_list.append('spe_' + sorted_group[i]['name'])
        #                     except KeyError:
        #                         sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
        #                         newgroup.append(sorted_group[i])
        #                         # print("IN CASE OF KEY ERROR PART 2!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        #                         check_list.append(sorted_group[i]['name'])
        #                         temp = {}
        #                         temp['name'] = 'spe_' + sorted_group[i]['name']
        #                         temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                         temp['block_number'] = newgroup[-1]['block_number'] + 1#
        #                         temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                         newgroup.append(temp)
        #                         check_list.append('spe_' + sorted_group[i]['name'])
        #             except IndexError:
        #                 newgroup.append(sorted_group[i])
        #                 # print("IN CASE OF INDEX ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        #                 check_list.append(sorted_group[i]['name'])
        #                 temp = {}
        #                 temp['name'] = 'spe_' + sorted_group[i]['name']
        #                 temp['group_availability_code'] = sorted_group[i]['group_availability_code']
        #                 temp['block_number'] = newgroup[-1]['block_number'] + 1
        #                 temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
        #                 newgroup.append(temp)
        #                 check_list.append('spe_' + sorted_group[i]['name'])
        #     else:
        #         print("NON SPE PARAMETERS NAME!!!!!!!!", sorted_group[i]['name'])
        #         if sorted_group[i]['block_number'] == 1:
        #             newgroup.append(sorted_group[i])
        #             check_list.append(sorted_group[i]['name'])
        #         else:
        #             try:
        #                 if newgroup[-1]['name'] in spekeylist:
        #                     if name_and_block_dict[newgroup[-2]['name']] == name_and_block_dict[sorted_group[i]['name']]:
        #                         sorted_group[i]['block_number'] = newgroup[-2]['block_number']
        #                         newgroup.insert(-1,sorted_group[i])
        #                         check_list.insert(-1, sorted_group[i]['name'])
        #                     else:
        #                         sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
        #                         newgroup.append(sorted_group[i])
        #                         check_list.append(sorted_group[i]['name'])
        #                 else:
        #                     # print("PREVIOUS NON SPE PARAMETER!!!!", newgroup[-1]['name'])
        #                     # spe_subgroup_check = self.check_if_param_in_spe_subgroup(sorted_group, newgroup[-1]['name'])
        #                     # print("SPE SUBGROUP CHECK!!!!", spe_subgroup_check)
        #                     if name_and_block_dict[newgroup[-1]['name']] == name_and_block_dict[sorted_group[i]['name']]:
        #                         sorted_group[i]['block_number'] = newgroup[-1]['block_number']
        #                         newgroup.append(sorted_group[i])
        #                         check_list.append(sorted_group[i]['name'])
        #                     # elif name_and_block_dict[newgroup[-1]['name']] != name_and_block_dict[sorted_group[i]['name']] and spe_subgroup_check == True:
        #                     #     print("ENTERED THE IF CONDITION OF NON SPE PARAMETERS!!!!!!!!!!!!!!")
        #                     #     sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 2
        #                     #     newgroup.append(sorted_group[i])
        #                     else:
        #                         sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
        #                         newgroup.append(sorted_group[i])
        #                         check_list.append(sorted_group[i]['name'])
        #             except IndexError:
        #                 newgroup.append(sorted_group[i])
        #                 # print("IN CASE OF INDEX ERROR PART 2 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        #                 check_list.append(sorted_group[i]['name'])
        # print("CHECK LIST OF ALL THE PARAMETERS BY THE ORDER OF APPEND!!!!!!!!!!!!!!!!!!!!!!", check_list)
        # print("END SPE INSIDE")
        ''' Completely works for BASIC'''
        ''' List of block numbers of parameters with substring SPE in their names.'''

        # print("NEWGROUP!!!!",newgroup)
        
        return newgroup




    def temporary_group_selection_for_multi_axis(self, unit_number):
        ''' Temporary group selection for multi-axis'''
        result = []
        singledata = self.ship_configs['data']
        for var in singledata.keys():            
            groups = singledata[var]['group_selection']
            for group in groups:
                if pd.isnull(singledata[var]['short_names']) == False:
                    if 'Compr' in singledata[var]['short_names'] and unit_number in singledata[var]['short_names']:
                        temp = {'short_names': singledata[var]['short_names'], 'unit': singledata[var]['unit']}
                        group.update(temp)
                        result.append(group)
                    if 'Peak' in singledata[var]['short_names'] and unit_number in singledata[var]['short_names']:
                        temp = {'short_names': singledata[var]['short_names'], 'unit': singledata[var]['unit']}
                        group.update(temp)
                        result.append(group)
                    if 'ME Exh Temp' in singledata[var]['short_names'] and unit_number in singledata[var]['short_names']:
                        temp = {'short_names': singledata[var]['short_names'], 'unit': singledata[var]['unit']}
                        group.update(temp)
                        result.append(group)
        # if len(result) >= 3:
        #     result.pop(0)
        #     result.pop(2)
        return result
    
    def get_selection_for_fuel_consumption(self):
        result=[]
        singledata = self.ship_configs['data']
        for vars in singledata.keys():
            if singledata[vars]['category'] == 'FUEL OIL CONSUMPTION':
                if 'Main Engine' in singledata[vars]['subcategory'] or 'Generator' in singledata[vars]['subcategory'] or 'Boiler' in singledata[vars]['subcategory']:
                    # temp = {vars: singledata[vars]['short_names']}
                    result.append(vars)
                if vars == 'sfoc':
                    # temp={vars: singledata[vars]['short_names']}
                    result.append(vars)
        result.insert(0, 'rep_dt')
        return result
    
    def get_short_names_list_for_multi_axis(self, unit_number):
        ''' Returns the list of short names from the group selection of multi axis'''
        groupList = self.temporary_group_selection_for_multi_axis(unit_number) # Only passing hard set groupname because
        # currently only the temporary function exists. Replace with the "groupname" argument later.
        namesList = []
        for i in groupList:
            if i['short_names'] not in namesList:
                namesList.append(i['short_names'])
        
        return namesList

    def get_height_of_chart(self, groupsList):
        ''' Returns the height of the chart to be set according to the group'''
        height=0
        # blocks=None
        # if 'Multi Axis' not in groupname:
        blocks = self.get_number_of_blocks('',groupsList)
        if(len(blocks) != 0):
            block = len(blocks)
            if(block <= 3):
                # height = default_height
                return default_height
            else:
                height = 200 + height_per_block*block
                return height
        # else:
        #     height = default_height
        #     return height

    def get_height_of_chart_after_double_click(self, groupsList):
        ''' Returns the height of the chart to be set according to the group after double click on the chart'''
        height=0
        # blocks=None
        # if 'Multi Axis' not in groupname:
        blocks = self.get_number_of_blocks('',groupsList)
        if(len(blocks) != 0):
            block = len(blocks)
            if(block <= 3):
                # height = default_height
                return default_height
            else:
                height = 200 + height_per_block_after_double_click*block
                return height
        # else:
        #     height = default_height
        #     return height
    
    def get_Y_position_for_legend(self, groupsList):
        ''' Returns the Y position for setting the legend on the chart'''
        blocks = self.get_number_of_blocks('',groupsList)
        if len(blocks) != 0:
            block = max(blocks)
        if block < 8:
            y_position = -((block - 1)/100) - height_per_block/10000
            return y_position
        if block >= 8:
            y_position = -((block - 1)/100) + height_per_block/10000
            return y_position

    def get_Y_position_for_legend_after_double_click(self, groupsList):
        ''' Returns the Y position for setting the legend on the chart after double click on the chart'''
        blocks = self.get_number_of_blocks('',groupsList)
        prev_height = self.get_height_of_chart_after_double_click(groupsList)
        if len(blocks) != 0:
            block = max(blocks)
        if block < 8:
            y_position = -(prev_height * 0.0006)/100
            return y_position
        if block >= 8:
            y_position = -(prev_height * 0.0003)/100
            return y_position


    
    def get_generic_group_selection(self):
        ''' For getting the groupname and the list of included parameters'''
        singledata = self.ship_configs['data']

        grpnames=self.get_list_of_groupnumbers()
        generic=self.get_dict_groupnames()
                
        
        for i in grpnames:
            block = self.get_number_of_blocks(i)
            temp_dict={}
            blocks=[]
            # print("GROUPNUMBER", type(i))
            for j in range(len(block)):
                temp_list=[]
                # print("BLOCK NUMBER", type(block[j]))
                for var in singledata.keys():
                    groups = singledata[var]['group_selection']
                    # print("VARS", var)
                    for group in groups:
                        if group['block_number'] == block[j] and group['groupnumber'] == str(i):
                            # print("INSIDE IF")
                            temp_list.append(singledata[var]['short_names'])
                temp_dict['Sub-Group '+ str(int(j))] = temp_list
                generic[i] = temp_dict
            # blocks.append(temp_dict)
            # if i in generic.keys():
            #     generic[i] = temp_dict

        return generic

    def get_dict_groupnames(self):
        ''' Returns a dictionary with keys as the names of individual groups
            and values as empty lists.
        '''
        grpnames=self.get_list_of_groupnumbers()
        ship_collection = self.get_ship_configs()

        generic_dict={}
        for i in grpnames:
            generic_dict[i] = {}
            # for doc in ship_collection.find({'ship_imo': self.ship_imo}).sort('ship_imo', ASCENDING):
            #     for key in doc['data'].keys():
            #         for group in doc['data'][key]['group_selection']:
            #             if group['groupnumber'] == i:
            #                 generic_dict[group['groupname']] = []

        return generic_dict
    
    def get_list_of_groupnumbers(self):
        ''' Returns list of all the groupnumbers'''
        singledata = self.ship_configs['data']
        grpnames=[]
        for var in singledata.keys():
            groups = singledata[var]['group_selection']
            for group in groups:
                if int(group['groupnumber']) not in grpnames:
                    grpnames.append(int(group['groupnumber']))
        grpnames.sort()
        return grpnames
    
    def get_list_of_groupnames(self):
        ''' Returns list of all the groupnames'''
        singledata = self.ship_configs['data']
        groupnumbers = self.get_list_of_groupnumbers()
        groupnames = []
        sorted_groupnames = []

        for var in singledata.keys():
            groups = singledata[var]['group_selection']  
            for group in groups:
                if group['groupname'] not in groupnames:
                    # groupnames.append(group['groupname'])
                    groupnames.insert(int(group['groupnumber'])-1, group['groupname'])
        # groupnumbers.sort()
        # for i in range(0, len(groupnames)):
        #     for j in range(0, len(groupnumbers)):
        #         if groupnumbers[j] == i + 1:
        #             sorted_groupnames.append(groupnames[i])

        return groupnames
        
    
    def get_number_of_blocks(self, groupname='', groupsList=[]):
        ''' Returns the list of block numbers in a particular group'''
        block_number = []
        if groupname != '':
            singledata = self.ship_configs['data']
            for var in singledata.keys():
                groups = singledata[var]['group_selection']
                for group in groups:
                    # if group['groupname'] == groupname:
                    if group['groupnumber'] == str(groupname):
                        if group['block_number'] not in block_number:
                            block_number.append(group['block_number'])
        else:
            for i in range(len(groupsList)):
                if groupsList[i]['block_number'] not in block_number:
                    block_number.append(groupsList[i]['block_number'])
        block_number.sort()
        # print("BLOCK", block_number)
        return block_number
    
    def get_variables_list_in_order_of_blocks(self, groupsList):
        ''' Returns the list of variables in the ascending order of the blocks w.r.t. groupname.'''
        block_list = self.get_number_of_blocks('', groupsList)
        # print(block_list)
        # singledata = self.ship_configs['data']
        variable_list=[]

        # for block in reversed(block_list):
            # for var in singledata.keys():
            #     groups = singledata[var]['group_selection']
            #     for group in groups:
            #         if group['groupname'] == groupname and group['block_number'] == block:
            #             variable_list.append(var)
        for i in range(len(groupsList)-1, -1, -1):
            if 'groupname' in groupsList[i]:
                # if groupsList[i]['block_number'] == block:
                variable_list.append(groupsList[i]['name'])
                if 'index' in groupsList[i]['name']:
                    new_spe = 'spe_' + 'main_fuel_index'
                    variable_list.append(new_spe)
            else:
                # if groupsList[i]['block_number'] == block:
                variable_list.append(groupsList[i]['name'])
                if 'index' in groupsList[i]['name']:
                    new_spe = 'spe_' + 'main_fuel_index'
                    variable_list.append(new_spe)
        return variable_list
    
    def get_number_of_group_members(self, groupname):
        ''' Returns count of members in a particular group'''
        count = 0
        singledata = self.ship_configs['data']
        for var in singledata.keys():
            groups = singledata[var]['group_selection']
            for group in groups:
                if group['groupname'] == groupname:
                    count = count + 1
        return count

    def temporary_dict_and_list_according_to_groups(self, groupsList):
        ''' TEMPORARY Returns a dictionary with the name of the parameters (as per the group selection)
            as keys and empty lists as values.
            Also returns a list of parameter names.'''
        res1 = {}
        res2 = {}
        names=[]
        # group = self.temporary_group_selection_for_11(groupname)
        count = 2
        # for i in range(0, count):
        #     temp = {group[i]['name']: []}
        #     res1.update(temp)
        #     res2.update(temp)
        # res1.update(rep_dt = [])
        # res2.update(rep_dt = [])
        for i in range(len(groupsList)):
            names.append(groupsList[i]['name'])
        names.insert(0,'rep_dt')
        return names
    
    def get_dict_and_list_according_to_groups(self, groupsList):
        ''' Returns a dictionary with the name of the parameters (as per the group selection)
            as keys and empty lists as values.
            Also returns a list of parameter names.
        '''
        maindb_collection = self.get_main_data()
        names=[]
        paramList=[]
        indicesList=[]
        for i in range(len(groupsList)):
            names.append(groupsList[i]['name'])
        names.insert(0,'rep_dt')
        for doc in maindb_collection.find({'ship_imo': self.ship_imo}).sort('final_rep_dt', DESCENDING).limit(1):
            for name in names:
                if name in doc['processed_daily_data'].keys():
                    paramList.append(name)
                elif name in doc['independent_indices'].keys():
                    indicesList.append(name)
        return paramList, indicesList
    
    def get_overall_min_and_max(self, groupsList, datadict):
        ''' Returns a dictionary with the block numbers as keys and a dictionary containing
            overall minimum and maximum of all variables according to blocks, as values.
        '''
        # groups = self.get_group_selection(groupname)
        # groupName = groupsList[0]['groupname'] if 'groupname' in groupsList[0] else ''
        blocks = self.get_number_of_blocks('', groupsList)
        block_wise_datadict={}

        #print("START PROCESS OVERALL 1st FOR LOOP")
        for block in blocks:
            minNum=None
            maxNum=None
            #print('OVERALL 2nd FOR LOOP')
            for group in range(len(groupsList)):
                #print("OVERALL 3rd FOR LOOP")
                # for key in datadict.keys():
                    # tempList = datadict[key]
                    # if None in tempList:
                    #     tempList.remove(None)
                # if key == groupsList[group]['name'] and block == groupsList[group]['block_number']:
                id_name = groupsList[group]['name']
                if block == groupsList[group]['block_number']:
                    try:
                        tempMin = min(x for x in datadict[id_name] if x != None)
                        tempMax = max(x for x in datadict[id_name] if x != None)
                        if minNum == None:
                            minNum = tempMin
                        else:
                            if tempMin < minNum:
                                minNum = tempMin
                        if maxNum == None:
                            maxNum = tempMax
                        else:
                            if tempMax > maxNum:
                                maxNum = tempMax
                        exactMax = maxNum - minNum
                        block_wise_datadict[id_name] = {'Min': minNum, 'Max': exactMax}    
                    except TypeError:
                        continue
                    except ValueError:
                        continue
                    except KeyError:
                        continue
        #print("END PROCESS OVERALL 1st FOR LOOP")
        
        return block_wise_datadict
    
    def get_shapes_for_loaded_ballast_data(self, noonorlogs='noon'):
        maindbcollection = self.get_main_data()
        loaded_ballast_list=[]
        res_data={'Ballast': [], 'Loaded': [], 'Engine': [], 'Nav': []}
        
        if noonorlogs == 'noon':
            tempList=[]
            for doc in maindbcollection.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'Logs': 1, 'vessel_loaded_check': 1, 'data_available_engine': 1, 'data_available_nav': 1, 'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                newdate = doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
                if doc['Logs'] == False:
                    if doc['vessel_loaded_check'] == "Ballast":
                        tempList.append(newdate)
                        # if doc['data_available_engine'] == True and doc['data_available_nav'] == True:
                        #     if tempList not in res_data['Ballast']:
                        #         res_data['Ballast'].append(tempList)
                        # else:
                        #     tempList = []
                    else:
                        tempList=[]
                    if doc['data_available_engine'] == True and doc['data_available_nav'] == True:
                        if tempList not in res_data['Ballast']:
                            res_data['Ballast'].append(tempList)
                    else:
                        tempList = []
            tempList = []
            for doc in maindbcollection.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'Logs': 1, 'vessel_loaded_check': 1, 'data_available_engine': 1, 'data_available_nav': 1, 'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                newdate = doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
                if doc['Logs'] == False:
                    if doc['vessel_loaded_check'] == "Loaded":
                        tempList.append(newdate)
                        # if doc['data_available_engine'] == True and doc['data_available_nav'] == True:
                        #     if tempList not in res_data['Loaded']:
                        #         res_data['Loaded'].append(tempList)
                        # else:
                        #     tempList = []
                    else:
                        tempList=[]
                    if doc['data_available_engine'] == True and doc['data_available_nav'] == True:
                        if tempList not in res_data['Loaded']:
                            res_data['Loaded'].append(tempList)
                    else:
                        tempList = []
            tempList = []
            for doc in maindbcollection.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'Logs': 1, 'data_available_engine': 1, 'data_available_nav': 1, 'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                newdate = doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
                if doc['Logs'] == False:
                    if doc['data_available_engine'] == False and doc['data_available_nav'] == True:
                        if tempList not in res_data['Nav']:
                            tempList.append(newdate)
                            res_data['Nav'].append(tempList)
                    elif doc['data_available_engine'] == True and doc['data_available_nav'] == False:
                        tempList.append(newdate)
                        if tempList not in res_data['Engine']:
                            res_data['Engine'].append(tempList)
                    else:
                        tempList=[]
            # #print("RES NO DATA!!!!!!!!", res_data['No Data'])
        if noonorlogs == 'logs':
            tempList=[]
            for doc in maindbcollection.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'Logs': 1, 'vessel_loaded_check': 1, 'data_available_engine': 1, 'data_available_nav': 1, 'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                newdate = doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
                if doc['Logs'] == True:
                    if doc['vessel_loaded_check'] == "Ballast":
                        tempList.append(newdate)
                        # if doc['data_available_engine'] == True and doc['data_available_nav'] == True:
                        #     if tempList not in res_data['Ballast']:
                        #         res_data['Ballast'].append(tempList)
                        # else:
                        #     tempList = []
                    else:
                        tempList=[]
                    if doc['data_available_engine'] == True and doc['data_available_nav'] == True:
                        if tempList not in res_data['Ballast']:
                            res_data['Ballast'].append(tempList)
                    else:
                        tempList = []
            tempList = []
            for doc in maindbcollection.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'Logs': 1, 'vessel_loaded_check': 1, 'data_available_engine': 1, 'data_available_nav': 1, 'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                newdate = doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
                if doc['Logs'] == True:
                    if doc['vessel_loaded_check'] == "Loaded":
                        tempList.append(newdate)
                        # if doc['data_available_engine'] == True and doc['data_available_nav'] == True:
                        #     if tempList not in res_data['Loaded']:
                        #         res_data['Loaded'].append(tempList)
                        # else:
                        #     tempList = []
                    else:
                        tempList=[]
                    if doc['data_available_engine'] == True and doc['data_available_nav'] == True:
                        if tempList not in res_data['Loaded']:
                            res_data['Loaded'].append(tempList)
                    else:
                        tempList = []
            tempList = []
            for doc in maindbcollection.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'Logs': 1, 'data_available_engine': 1, 'data_available_nav': 1, 'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                newdate = doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
                if doc['Logs'] == True:
                    if doc['data_available_engine'] == False and doc['data_available_nav'] == True:
                        if tempList not in res_data['Nav']:
                            tempList.append(newdate)
                            res_data['Nav'].append(tempList)
                    elif doc['data_available_engine'] == True and doc['data_available_nav'] == False:
                        tempList.append(newdate)
                        if tempList not in res_data['Engine']:
                            res_data['Engine'].append(tempList)
                    else:
                        tempList=[]
        
        for i in res_data['Ballast']:
            # #print(i)
            #lightpink
            tempballast={'type': 'rect','xref': 'x','yref': 'paper','y0': 0,'y1': 1,'fillcolor': 'rgb(249, 226, 226)','opacity': 0.4,'line': {'width': 0}}
            try:
                # tempOne = datetime.strptime(i[0], '%Y-%m-%d')
                # tempTwo = datetime.strptime(i[-1], '%Y-%m-%d')
                # rel_date = relativedelta(tempOne, tempTwo)
                # if rel_date.years == 0 and rel_date.months <= 3:
                tempballast['x0'] = i[0]
                tempballast['x1'] = i[-1]
                loaded_ballast_list.append(tempballast)
                # if rel_date.months <= 3:
                #     #print(rel_date.months)
                #     tempballast['x0'] = i[0]
                #     tempballast['x1'] = i[-1]
                #     loaded_ballast_list.append(tempballast)
            except IndexError:
                continue
        for i in res_data['Loaded']:
            #gray
            temploaded={'type': 'rect','xref': 'x','yref': 'paper','y0': 0,'y1': 1,'fillcolor': 'rgb(196, 192, 192)','opacity': 0.4,'line': {'width': 0}}
            try:
                # tempOne = datetime.strptime(i[0], '%Y-%m-%d')
                # tempTwo = datetime.strptime(i[-1], '%Y-%m-%d')
                # rel_date = relativedelta(tempOne, tempTwo)
                # if rel_date.years == 0 and rel_date.months <= 3:
                temploaded['x0'] = i[0]
                temploaded['x1'] = i[-1]
                loaded_ballast_list.append(temploaded)
                # if rel_date.months <= 3:
                #     temploaded['x0'] = i[0]
                #     temploaded['x1'] = i[-1]
                #     loaded_ballast_list.append(temploaded)
            except IndexError:
                continue
        for i in res_data['Engine']:
            tempdata = {'type': 'rect','xref': 'x','yref': 'paper','y0': 0,'y1': 1,'fillcolor': 'rgb(255, 255, 51)','opacity': 0.1,'line': {'width': 0}}
            # tempOne = datetime.strptime(i[0], '%Y-%m-%d')
            # tempTwo = datetime.strptime(i[-1], '%Y-%m-%d')
            # rel_date = relativedelta(tempOne, tempTwo)
            # if rel_date.years == 0:
            tempdata['x0'] = i[0]
            tempdata['x1'] = i[-1]
            # if tempdata not in loaded_ballast_list:
            loaded_ballast_list.append(tempdata)
        for i in res_data['Nav']:
            tempdata = {'type': 'rect','xref': 'x','yref': 'paper','y0': 0,'y1': 1,'fillcolor': 'rgb(213, 250, 252)','opacity': 0.3,'line': {'width': 0}}
            # tempOne = datetime.strptime(i[0], '%Y-%m-%d')
            # tempTwo = datetime.strptime(i[-1], '%Y-%m-%d')
            # rel_date = relativedelta(tempOne, tempTwo)
            # if rel_date.years == 0:
            tempdata['x0'] = i[0]
            tempdata['x1'] = i[-1]
            # if tempdata not in loaded_ballast_list:
            loaded_ballast_list.append(tempdata)
        return loaded_ballast_list


    
    def get_loaded_and_ballast_data(self, datadict, groupsList):
        ''' Returns the loaded and ballast data along with the overall minimum and maximum of all the variables.'''
        #print("START GET MAIN DATA")
        maindbcollection = self.get_main_data()
        # maindata = maindbcollection.find(
        #     {
        #         'ship_imo': self.ship_imo
        #     },
        #     {
        #         'ship_imo': 1,
        #         'vessel_loaded_check': 1,
        #         '_id': 0,
        #         # 'date': 0,
        #         # 'historical': 0,
        #         # 'processed_daily_data': 0,
        #         # 'weather_api': 0,
        #         # 'position_api': 0,
        #         # 'faults': 0,
        #         # 'indices': 0,
        #         # 'health_status': 0,
        #         # 'Equipment': 0
        #     }
        #     )
        # main_data = loads(dumps(maindata))
        #print("END GET MAIN DATA")
        # groups = self.get_group_selection(groupname)
        #print("START BLOCK WISE DATADICT")
        block_wise_datadict = self.get_overall_min_and_max(groupsList, datadict)
        #print("END BLOCK WISE DATADICT")
        # #print(block_wise_datadict)

        result={}
        
        #print("START PROCESSING BALLAST LOADED 1st FOR LOOP")
        for key in datadict.keys():
            ballast_list=[]
            loaded_list=[]
            if key != 'rep_dt':
                # minNum = min(datadict[key]) if len(datadict[key]) > 0 else 0
                # ballast_list.append(str(minNum))
                # loaded_list.append(str(minNum))
                # maxNum = max(datadict[key])
                # minNum = min(datadict[key])
                try:
                    val = block_wise_datadict[key]['Max']
                except KeyError:
                    continue
                #print("2nd FOR LOOP")
                # for index, item in enumerate(maindata):
                #     if item['vessel_loaded_check'] == 'Ballast':
                #         ballast_list.append(val)
                #     else:
                #         ballast_list.append(0)
                #     if item['vessel_loaded_check'] == 'Loaded':
                #         loaded_list.append(val)
                #     else:
                #         loaded_list.append(0)
                # result[key] = {'Ballast': ballast_list, 'Loaded': loaded_list}
                for doc in maindbcollection.find({'ship_imo': self.ship_imo},{'ship_imo': 1,'vessel_loaded_check': 1,'_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
                    if doc['vessel_loaded_check'] == 'Ballast':
                        ballast_list.append(val)
                    else:
                        ballast_list.append(None)
                    if doc['vessel_loaded_check'] == 'Loaded':
                        loaded_list.append(val)
                    else:
                        loaded_list.append(None)
                result[key] = {'Ballast': ballast_list, 'Loaded': loaded_list}
        #print("END PROCESSING BALLAST LOADED 1st FOR LOOP")

        return result, block_wise_datadict
    
    def get_subgroup_names(self, groupname):
        ''' Returns a dictionary for sub-group names.'''
        ship_collection = self.get_ship_configs()
        subgroup_dict={}

        for doc in ship_collection.find({'ship_imo': self.ship_imo}, {'group_dict': 1}):
            try:
                subgroup_dict = doc['group_dict'][groupname]
            except KeyError:
                subgroup_dict = None
        
        return subgroup_dict
    
    def get_subgroup_names_for_custom_groups(self, subgroup_dict=None, groupname="", group_selection=None):
        ''' Returns a dictionary for sub-group names. Also includes the individual parameters added by user.
            If groupname is empty, iterates over the group_selection and creates the dictionary.
        '''
        if subgroup_dict != None:
            index_of_last_key = 0
            block_number_of_last_key = int(list(subgroup_dict.keys())[-1].replace('Sub-Group ', '').replace('_i', ''))
            name_of_last_value = list(subgroup_dict.values())[-1]
            for i in range(len(group_selection)):
                if block_number_of_last_key == group_selection[i]['block_number']:
                    index_of_last_key = i
            
            for i in range(index_of_last_key+1, len(group_selection)):
                temp_key = 'Sub-Group '
                if 'spe_' not in group_selection[i]['name']:
                    block_number_of_last_key = block_number_of_last_key + 1
                    key_for_subgroup_dict = temp_key + str(block_number_of_last_key)
                    subgroup_dict[key_for_subgroup_dict] = group_selection[i]['short_names']
                else:
                    block_number_of_last_key = block_number_of_last_key
                    key_for_subgroup_dict = temp_key + str(block_number_of_last_key) + '_i'
                    subgroup_dict[key_for_subgroup_dict] = group_selection[i]['short_names']
            return subgroup_dict
        else:
            list_of_block_numbers = []
            result={}
            for i in range(len(group_selection)):
                temp_key = 'Sub-Group '
                if group_selection[i]['block_number'] not in list_of_block_numbers:
                    if 'spe_' not in group_selection[i]['name']:
                        key_for_subgroup_dict = temp_key + str(group_selection[i]['block_number'])
                        result[key_for_subgroup_dict] = group_selection[i]['short_names']
                    else:
                        key_for_subgroup_dict = temp_key + str(group_selection[i]['block_number']) + '_i'
                        result[key_for_subgroup_dict] = group_selection[i]['short_names']
            return result




        
    
    ''' Functions for Daily Report processing '''
    def get_category_dict(self,callfrom):
        ''' Returns a dictionary containing category as the key and the list of subcategories in that category as the value'''
        # single = tempRes[0]
        #print(type(self.ship_configs))
        singledata = self.ship_configs['data']
        catDict={}
        catList=[]
        categoryDict={}
        if callfrom=='issues':
            column_headers = daily_issues_column_headers
        else:
            column_headers = daily_report_column_headers

        for var in singledata.keys():
            try:
                if singledata[var]['category'].strip() not in catList:
                    # categoryList.append(singledata[var]['category'].strip())
                    catList.insert(position_of_collapsible_category[singledata[var]['category'].strip()], singledata[var]['category'].strip())
            except AttributeError:
                continue
        catList.insert(0, 'VESSEL PARTICULARS')
        
        categoryDict['VESSEL PARTICULARS'] = ['Vessel Particulars']
        for i in catList:
            temp=[]
            for var in singledata.keys():
                try:
                    if singledata[var]['category'].strip() == i and singledata[var]['subcategory'].strip() not in temp:
                        temp.append(singledata[var]['subcategory'].strip())
                        # #print(temp)
                        categoryDict[singledata[var]['category'].strip()] = temp
                except AttributeError:
                    continue
        catDict['data'] = [categoryDict]
        return catList, catDict, column_headers
    
    def get_subcategory_dict(self):
        ''' Return the dictionary containing the parameters in every subcategory.
            Here, the subcategory name is the key and the list of parameters is the value.
            Takes care of null values if any.
        '''
        subcategoryList=[]
        subcategoryDict={}
        singledata = self.ship_configs['data']

        for var in singledata.keys():
            try:
                if singledata[var]['subcategory'].strip() not in subcategoryList:
                    subcategoryList.append(singledata[var]['subcategory'].strip())
                subcategoryList.append('Vessel Particulars')
            except AttributeError:
                continue
    
        for i in subcategoryList:
            tempList=[]
            for var in singledata.keys():
                try:
                    if singledata[var]['subcategory'].strip() == i:
                        if pd.isnull(singledata[var]['short_names']) == True:
                            tempName = ""
                            tempDict = {var: tempName}
                            tempList.append(tempDict)
                            subcategoryDict[singledata[var]['subcategory'].strip()] = tempList
                        else:
                            tempDict = {var: singledata[var]['short_names'].strip()}
                            tempList.append(tempDict)
                            subcategoryDict[singledata[var]['subcategory'].strip()] = tempList
                    subcategoryDict['Vessel Particulars'] = self.ship_configs['static_data']
                except AttributeError:
                    continue
        for key in subcategoryDict['Vessel Particulars'].keys():
            if pd.isnull(subcategoryDict['Vessel Particulars'][key]['name']) == True:
                subcategoryDict['Vessel Particulars'][key]['name'] = ""
            if pd.isnull(subcategoryDict['Vessel Particulars'][key]['value']) == True: 
                subcategoryDict['Vessel Particulars'][key]['value'] = ""
        return subcategoryDict
    
    def create_short_names_dictionary(self):
        ''' Returns a dictionary of short names of all the parameters.
            Here, identifier_new is the key and the Short Name is the value.
        '''
        shortNameDict={}

        try:
            for var in self.ship_configs['data']:
                if pd.isnull(self.ship_configs['data'][var]['short_names']) == True:
                    shortNameDict[var] = var
                else:
                    shortNameDict[var] = self.ship_configs['data'][var]['short_names']
        except KeyError:
            ship_configs_collection = self.get_ship_configs()
            for doc in ship_configs_collection.find({"ship_imo": self.ship_imo}):
                for var in doc['data']:
                    if pd.isnull(doc['data'][var]['short_names']) == True:
                        shortNameDict[var] = var
                    else:
                        shortNameDict[var] = doc['data'][var]['short_names']
        
        return shortNameDict
    
    def get_Equipment_and_Parameter_list(self):
        ''' Returns the list of parameters that are classified as equipments(E)
            and the list of parameters that are classified as parameters(P).
        '''
        singledata = self.ship_configs['data']
        equipment_list = []
        parameter_list = []

        for var in singledata.keys():
            if singledata[var]['override'] == 1:
                # if pd.isnull(singledata[var]['var_type']) == False:
                if 'E' in singledata[var]['var_type'] and '1' not in singledata[var]['var_type']:
                    equipment_list.append(var)
                if 'P' in singledata[var]['var_type']:
                    parameter_list.append(var)
            elif singledata[var]['override'] != -1 and singledata[var]['override'] != 1:
                if pd.isnull(singledata[var]['var_type']) == False:
                    if singledata[var]['var_type'] =='E' or singledata[var]['var_type'] == 'E1' and pd.isnull(singledata[var]['source_idetifier']) == False:
                        equipment_list.append(var)
                    if 'P' in singledata[var]['var_type'] and pd.isnull(singledata[var]['source_idetifier']) == False:
                        parameter_list.append(var)
            else:
                continue
        #print("EQUIPMENT LIST", equipment_list)
        
        return equipment_list, parameter_list
    
    def check_for_nan_and_replace(self, data):
        dupli_data = data
        for var in dupli_data['processed_daily_data'].keys():
            for subvar in dupli_data['processed_daily_data'][var]:
                if subvar == 'predictions':
                    for i in range(0, 3):
                        try:
                            try:
                                # UNCOMMENT WHEN m3 ADDED IN DB
                                if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m3'][i]) == True or dupli_data['processed_daily_data'][var][subvar]['m3'][i] == -np.inf or dupli_data['processed_daily_data'][var][subvar]['m3'][i] == np.inf:
                                    dupli_data['processed_daily_data'][var][subvar]['m3'].pop(i)
                                    dupli_data['processed_daily_data'][var][subvar]['m3'].insert(i, 'null')
                                if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m6'][i]) == True or dupli_data['processed_daily_data'][var][subvar]['m6'][i] == -np.inf or dupli_data['processed_daily_data'][var][subvar]['m6'][i] == np.inf:
                                    dupli_data['processed_daily_data'][var][subvar]['m6'].pop(i)
                                    dupli_data['processed_daily_data'][var][subvar]['m6'].insert(i, 'null')
                                if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m12'][i]) == True or dupli_data['processed_daily_data'][var][subvar]['m12'][i] == -np.inf or dupli_data['processed_daily_data'][var][subvar]['m12'][i] == np.inf:
                                    dupli_data['processed_daily_data'][var][subvar]['m12'].pop(i)
                                    dupli_data['processed_daily_data'][var][subvar]['m12'].insert(i, 'null')
                            except IndexError:
                                continue
                            # UNCOMMENT WHEN m3 ADDED IN DB
                            try:
                                if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m3'][i]) == True or dupli_data['processed_daily_data'][var][subvar]['ly_m3'][i] == -np.inf or dupli_data['processed_daily_data'][var][subvar]['ly_m3'][i] == np.inf:
                                    dupli_data['processed_daily_data'][var][subvar]['ly_m3'].pop(i)
                                    dupli_data['processed_daily_data'][var][subvar]['ly_m3'].insert(i, 'null')
                                if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m6'][i]) == True or dupli_data['processed_daily_data'][var][subvar]['ly_m6'][i] == -np.inf or dupli_data['processed_daily_data'][var][subvar]['ly_m6'][i] == np.inf:
                                    dupli_data['processed_daily_data'][var][subvar]['ly_m6'].pop(i)
                                    dupli_data['processed_daily_data'][var][subvar]['ly_m6'].insert(i, 'null')
                                if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m12'][i]) == True or dupli_data['processed_daily_data'][var][subvar]['ly_m12'][i] == -np.inf or dupli_data['processed_daily_data'][var][subvar]['ly_m12'][i] == np.inf:
                                    dupli_data['processed_daily_data'][var][subvar]['ly_m12'].pop(i)
                                    dupli_data['processed_daily_data'][var][subvar]['ly_m12'].insert(i, 'null')
                            except IndexError:
                                continue
                        except TypeError:
                            continue
                if subvar == 'z_score':
                    try:
                        # UNCOMMENT WHEN m3 ADDED IN DB
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m3']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m3'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m6']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m6'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m12']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m12'] = 'null'
                        # UNCOMMENT WHEN m3 ADDED IN DB
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m3']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['ly_m3'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m6']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['ly_m6'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m12']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['ly_m12'] = 'null'
                    except TypeError:
                        continue
                if subvar == 'crit_val_dynamic':
                    try:
                        # UNCOMMENT WHEN m3 ADDED IN DB
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m3']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m3'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m6']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m6'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m12']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m12'] = 'null'
                        # UNCOMMENT WHEN m3 ADDED IN DB
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m3']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['ly_m3'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m6']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['ly_m6'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m12']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['ly_m12'] = 'null'
                    except TypeError:
                        continue
                if subvar == 'ucl_crit_fcrit':
                    try:
                        # UNCOMMENT WHEN m3 ADDED IN DB
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m3']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m3'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m6']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m6'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m12']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m12'] = 'null'
                        # UNCOMMENT WHEN m3 ADDED IN DB
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m3']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['ly_m3'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m6']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['ly_m6'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m12']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['ly_m12'] = 'null'
                    except TypeError:
                        continue
                if pd.isnull(dupli_data['processed_daily_data'][var][subvar]) == True:
                    dupli_data['processed_daily_data'][var][subvar] = ""
        
        return dupli_data
    
    def get_decimal_control_on_daily_values(self, subcategoryDictData):
        ''' Returns categoryDictData with the correct decimal places'''
        for subcategory in subcategoryDictData.keys():
            for i in range(len(subcategoryDictData[subcategory])):
                try:
                    tempValue = self.makeDecimal(subcategoryDictData[subcategory][i]['processed'])
                    subcategoryDictData[subcategory][i]['processed'] = tempValue
                    
                    if subcategoryDictData[subcategory][i]['predictions']['m3']:
                        tempExptd = self.makeDecimal(subcategoryDictData[subcategory][i]['predictions']['m3'][1])
                        subcategoryDictData[subcategory][i]['predictions']['m3'][1] = tempExptd
                    if subcategoryDictData[subcategory][i]['predictions']['m6']:
                        tempExptd = self.makeDecimal(subcategoryDictData[subcategory][i]['predictions']['m6'][1])
                        subcategoryDictData[subcategory][i]['predictions']['m6'][1] = tempExptd
                    if subcategoryDictData[subcategory][i]['predictions']['m12']:
                        tempExptd = self.makeDecimal(subcategoryDictData[subcategory][i]['predictions']['m12'][1])
                        subcategoryDictData[subcategory][i]['predictions']['m12'][1] = tempExptd
                except TypeError:
                    continue
                except KeyError:
                    continue
        return subcategoryDictData
    
    def get_category_and_subcategory_with_issues(self, dateString, id, ship_imo):
        ''' Returns all the categories and the subcategories that have outlier parameters.'''
        result={}
        ship_collection = self.get_ship_configs()
        issues, issuesCount = self.create_dict_of_issues(id, dateString=dateString) if dateString != "" else self.create_dict_of_issues(id, "")
        for doc in ship_collection.find({'ship_imo': int(ship_imo)}):
            for key in doc['data'].keys():
                # if key in equipment_list or key in parameter_list:
                if str(ship_imo) in issues:
                    for var in issues[str(self.ship_imo)]:
                        if pd.isnull(doc['data'][key]['short_names']) == False and doc['data'][key]['short_names'].strip() == var:
                            tempList=[]
                            if pd.isnull(doc['data'][key]['subcategory']) == False and doc['data'][key]['subcategory'].strip() not in tempList:
                                tempList.append(doc['data'][key]['subcategory'].strip())
                                # #print(tempList)
                            if pd.isnull(doc['data'][key]['category']) == False and doc['data'][key]['category'].strip() not in result.keys():
                                result[doc['data'][key]['category'].strip()] = tempList
                                # #print(tempList)
                            else:
                                if pd.isnull(doc['data'][key]['category']) == False:
                                    result[doc['data'][key]['category'].strip()].extend(tempList)
                        if pd.isnull(doc['data'][key]['short_names']) == False and doc['data'][key]['short_names'].strip() == var.replace('_OP', ''):
                            tempList=[]
                            if pd.isnull(doc['data'][key]['subcategory']) == False and doc['data'][key]['subcategory'].strip() not in tempList:
                                tempList.append(doc['data'][key]['subcategory'].strip()+'_OP')
                                # #print(tempList)
                            if pd.isnull(doc['data'][key]['category']) == False and doc['data'][key]['category'].strip() not in result.keys():
                                result[doc['data'][key]['category'].strip()] = tempList
                                # #print(tempList)
                            else:
                                if pd.isnull(doc['data'][key]['category']) == False:
                                    result[doc['data'][key]['category'].strip()].extend(tempList)
                        if pd.isnull(doc['data'][key]['short_names']) == False and doc['data'][key]['short_names'].strip() == var.replace('_SPE', ''):
                            tempList=[]
                            if pd.isnull(doc['data'][key]['subcategory']) == False and doc['data'][key]['subcategory'].strip() not in tempList:
                                tempList.append(doc['data'][key]['subcategory'].strip()+'_SPE')
                                # #print(tempList)
                            if pd.isnull(doc['data'][key]['category']) == False and doc['data'][key]['category'].strip() not in result.keys():
                                result[doc['data'][key]['category'].strip()] = tempList
                                # #print(tempList)
                            else:
                                if pd.isnull(doc['data'][key]['category']) == False:
                                    result[doc['data'][key]['category'].strip()].extend(tempList)
                else:
                    result = {}
        #print(result)
        return result, issuesCount
    
    def get_static_data_for_charter_party(self):
        ''' Returns the dictionary with the static data of the charter party parameters. '''
        ship_collection = self.get_ship_configs()
        result = {}

        for doc in ship_collection.find({'ship_imo': self.ship_imo}):
            for key in doc['data'].keys():
                if doc['data'][key]['subcategory'] == 'Charter party':
                    result[doc['data'][key]['source_idetifier']] = doc['data'][key]['static_data'] if pd.isnull(doc['data'][key]['static_data']) == False else None
        
        return result
    
    def get_charter_party_list(self):
        ''' Returns the dict of source identifiers for parameters in the charter party subcategory.'''
        ship_collection = self.get_ship_configs()
        result = {}

        for doc in ship_collection.find({'ship_imo': self.ship_imo}):
            for key in doc['data'].keys():
                if doc['data'][key]['subcategory'] == 'Charter party':
                    if pd.isnull(doc['data'][key]['source_idetifier']) == False:
                        result[key] = doc['data'][key]['source_idetifier']

        return result
    
    def get_daily_charter_party_values(self, dateString=''):
        ''' Returns the values of the charter party parameters.'''
        maindb_collection = self.get_main_data()
        charter_party_dict = self.get_charter_party_list()
        result = {}
        prediction_result = {}

        if dateString != '':
            findDate = datetime.strptime(dateString,"%Y-%m-%d, %H:%M:%S")
            for cp in charter_party_dict.keys():
                for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'processed_daily_data.rep_dt.processed': findDate}):
                    for key in doc['processed_daily_data'].keys():
                        if key == charter_party_dict[cp]:
                            try:
                                result[cp] = self.makeDecimal(doc['processed_daily_data'][key]['processed'])
                            except TypeError:
                                result[cp] = None
                            try:
                                prediction_result[cp] = self.makeDecimal(doc['processed_daily_data'][key]['predictions']['m3'][1])
                            except TypeError:
                                prediction_result[cp] = None
                            except KeyError:
                                prediction_result[cp] = None
                            except IndexError:
                                prediction_result[cp] = None
            return result, prediction_result
        else:
            for cp in charter_party_dict.keys():
                for doc in maindb_collection.find({'ship_imo': self.ship_imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING).limit(1):
                    for key in doc['processed_daily_data'].keys():
                        if key == charter_party_dict[cp]:
                            try:
                                result[cp] = self.makeDecimal(doc['processed_daily_data'][key]['processed'])
                            except TypeError:
                                result[cp] = None
                            try:
                                prediction_result[cp] = self.makeDecimal(doc['processed_daily_data'][key]['predictions']['m3'][1])
                            except TypeError:
                                prediction_result[cp] = None
                            except KeyError:
                                prediction_result[cp] = None
                            except IndexError:
                                prediction_result[cp] = None
        return result, prediction_result



    ''' Functions for Interactive processing '''
    #print("START CREATE REGRESSION")
    def regress_for_constant_x(self,data,x1,y,z=None,**other_x):
        '''
            function for prediction by changing 1 x dimension and keeping other dimensios contant
            data=dataframe,x1=the x dimension to be changed,y= y dimension(to be predicted),otherx=other x dimensions constant values
        '''
        # data.to_csv("ActualValues.csv")
        clean_data = self.clean_data(data)
        data = clean_data.copy()
        data_copy = clean_data.copy()
        for col in data.columns:
            data=self.z_score_data(data,col)
            data_copy=self.z_score_data_3(data_copy,col)
        # data.to_csv("Zscore1CleanData.csv")
        # data_copy.to_csv("Zscore3CleanData.csv")
        clean_X_list = data_copy[x1]
        clean_Y_list = data_copy[y]
        clean_Z_list = data_copy[z] if z is not None else []
        for col in data.columns:
            data=self.z_score_data(data,col)
        # data.to_csv("Zscore2CleanData.csv")
        # clean_X_list = clean_data_copy[x1]
        # clean_Y_list = clean_data_copy[y]
        # clean_Z_list = clean_data_copy[z] if z is not None else []
        x_list=[]
        temp_data={}
        if z is not None:
            for column in data:
                if column!=y:
                    x_list.append(str(column))  
                    temp_data[column]=[]
            for key in other_x:
                if key in temp_data.keys():
                    temp_data[key]=[other_x[key]]
            # #print("DATAFRAME!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", data)
            # for i in range(len(data)):
            #     #print(data.loc[[i]])
            x1_min = data[x1].min()
            x1_max = data[x1].max()
            z_min = data[z].min()
            z_max = data[z].max()
            # x1_list=np.arange(x1_min, x1_max, 0.1)
            x1_list=np.linspace(x1_min, x1_max, len(data[x1]))

            z_list=np.linspace(z_min, z_max, len(data[x1]))
            
            reg=Ridge(alpha=2)
            # reg = LRPI()
            poly = PolynomialFeatures(degree = 3)
            if not (np.any(pd.isnull(data[x_list])) and np.all(pd.isfinite(data[x_list]))):
                if not (np.any(pd.isnull(data[y])) and np.all(np.isfinite(data[y]))):
                    # Code in current use
                    X_poly = poly.fit_transform(data[x_list])
                    poly.fit(X_poly, data[y])
                    reg.fit(X_poly, data[y])

                    # Code to test
                    # earth=Earth(max_terms=500,max_degree=1)
                    # fitted=earth.fit(data[x_list],data[y])
            else:
                return "x_list or y has nan or infinite"
            # #print(reg.predict(X_poly))
            pred_list=[]
            # pls_dataframe = pd.DataFrame(columns = ['lower', 'Pred', 'upper'])
            # #print("EMPTY DATAFRAME", pls_dataframe)
            # for i in x1_list:
            # # for i in data[x1]:
            #     # x1_list.append(i)
            #     temp_data[x1]=[i]
            # temp_data = x1_list
            # final_df = pd.DataFrame({})
            # final_df[x1] = x1_list
            # final_df[z] = z_list
            for i in range(len(z_list)):
            # for i in data[z]:
                # y_list.append(i)
                # #print(z_list[i])
                j = z_list[i].round(1)
                k = x1_list[i].round(1)
                temp_data[x1] = [k]
                temp_data[z] = [j]
                # #print("TEMP DATA!!!!!!!!!!!!", temp_data)
                temp_dataframe=pd.DataFrame(temp_data)
                # Code in current use
                pred=reg.predict(poly.fit_transform(temp_dataframe))
                
                # Code to test
                # pred=fitted.predict(temp_dataframe)
                pred_list.append(pred[0])
                # #print(pred[0])
            # new_pred_list = self.create_2D_array(pred_list)
            # final_df['pred'] = pred_list
            # final_df.to_csv("PredDF.csv")
            # #print(x1_list)
            # #print(z_list)
            # #print(pred_list)
            # #print(clean_X_list)
            # #print(clean_Y_list)
            # #print(clean_Z_list)
            return x1_list.tolist(), z_list.tolist(), pred_list, clean_X_list.tolist(), clean_Y_list.tolist(), clean_Z_list.tolist()
            # x_new = list(data[x1])
            # z_new = list(data[z])
            # x_new.sort()
            # z_new.sort()
            # return x_new, z_new, pred_list
        else:
            for column in data:
                if column!=y:
                    x_list.append(str(column))  
                    temp_data[column]=[]
            for key in other_x:
                if key in list(temp_data.keys()):
                    temp_data[key] = other_x[key]
            #print("TEMP", temp_data)
            #print("DATAFRAME", data)

            minValue = data[x1].min()
            maxValue = data[x1].max()
            # x1_list=np.arange(minValue, maxValue, 0.1)
            x1_list = np.linspace(minValue, maxValue, len(data[x1]))
            # data[x1] = x1_list
            
            # reg=LinearRegression()
            reg=Ridge(alpha=2)
            poly = PolynomialFeatures(degree = 2)
            # pls_dataframe = pd.DataFrame(columns = ['lower', 'Pred', 'upper'])
            # #print("EMPTY DATAFRAME", pls_dataframe)
            # if not (np.any(pd.isnull(data[x_list])) and np.all(pd.isfinite(data[x_list]))):
            #     if not (np.any(pd.isnull(data[y])) and np.all(np.isfinite(data[y]))):
            # #print("DATA OF X_LIST!!!!!", data[x_list])
            # Code in current use
            X_poly = poly.fit_transform(data[x_list])
            poly.fit(X_poly, data[y])
            reg.fit(X_poly, data[y])
            
            # Code to test
            # earth=Earth(max_terms=500,max_degree=1)
            # fitted=earth.fit(data[x_list],data[y])
            pred_list=[]
            for i in x1_list:
                # x1_list.append(i)
                # i = i.round(1)
                temp_data[x1]=[i.round(1)]
                temp_dataframe=pd.DataFrame(temp_data)
                # Code in current use
                pred=reg.predict(poly.fit_transform(temp_dataframe))
                
                # Code to test
                # pred=fitted.predict(temp_dataframe)
                pred_list.append(pred[0])
            return x1_list.tolist(), pred_list, clean_X_list.tolist(), clean_Y_list.tolist()
    #print("END CREATE REGRESSION")
    
    #print("START CREATE DATAFRAME")
    def create_dataframe(self, X, Y, duration, load, compare="None", sisorsim_imo="", Z=None, **other_X):
        maindb_collection = database.get_collection("Main_db")
        #print("START SHORT NAME DICT")
        shortNameDict = self.create_short_names_dictionary()
        #print("END SHORT NAME DICT")
        X_list = []
        Y_list = []
        Z_list = []
        X_last_list = []
        Y_last_list = []
        Z_last_list = []
        dict_for_dataframe = {}
        if compare == "None" or compare == "all_sisters" or compare == True:
            get_close_by_date = self.create_maindb_according_to_duration(duration, compare=compare)
            if load == 'any':
                for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+X: 1, 'processed_daily_data.'+Y: 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                    if doc['final_rep_dt'] != get_close_by_date:
                        # tempListForOtherX=[]
                        # for key in doc['processed_daily_data'].keys():
                            # if doc['processed_daily_data'][key]['identifier'].strip() == X:
                        if X in doc['processed_daily_data']:
                            if pd.isnull(doc['processed_daily_data'][X]['processed']):
                                X_list.append(None)
                                # continue
                            else:
                                X_list.append(doc['processed_daily_data'][X]['processed'])
                        # if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                        if Y in doc['processed_daily_data']:
                            if pd.isnull(doc['processed_daily_data'][Y]['processed']):
                                Y_list.append(None)
                                # continue
                            else:
                                Y_list.append(doc['processed_daily_data'][Y]['processed'])
                        # if doc['processed_daily_data'][key]['identifier'].strip() in keys_of_other_X:
                        #     if pd.isnull(doc['processed_daily_data'][key]['processed']):
                        #         # Y_list.append(0)
                        #         continue
                        #     else:
                        #         tempListForOtherX.append(doc['processed_daily_data'][key]['processed'])
                        # if Z is not None:
                        #     # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                        #     if Z in doc['processed_daily_data']:
                        #         if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                        #             # Z_list.append(0)
                        #             continue
                        #         else:
                        #             Z_list.append(doc['processed_daily_data'][Z]['processed'])
                    else:
                        break
                
                if Z is not None:
                    for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+Z: 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                        if doc['final_rep_dt'] != get_close_by_date:
                            # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                            if Z in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                                    Z_list.append(None)
                                    # continue
                                else:
                                    Z_list.append(doc['processed_daily_data'][Z]['processed'])
                        else:
                            break
                #print("END READ DATA AND CREATE LISTS")
                #print("START CREATE OTHER LISTS")
                #print("OTHER X", other_X)
                for key in other_X:
                    if key != 'None':
                        tempList = []
                        # for i in range(len(X_list)):
                        #     tempList.append(other_X[key])
                        # dict_for_dataframe[key] = tempList
                        for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'_id': 0, 'processed_daily_data.'+key: 1, 'final_rep_dt': 1}).sort('final_rep_dt', DESCENDING):
                            if doc['final_rep_dt'] != get_close_by_date:
                                if key in doc['processed_daily_data']:
                                    if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                        # Y_list.append(0)
                                        # continue
                                        tempList.append(None)
                                    else:
                                        tempList.append(doc['processed_daily_data'][key]['processed'])
                            else:
                                break
                        dict_for_dataframe[key] = tempList
            elif load == 'loaded':
                for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+X: 1, 'processed_daily_data.'+Y: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                    if doc['vessel_loaded_check'] == "Loaded":
                        if doc['final_rep_dt'] != get_close_by_date:
                            # tempListForOtherX=[]
                            # for key in doc['processed_daily_data'].keys():
                                # if doc['processed_daily_data'][key]['identifier'].strip() == X:
                            if X in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][X]['processed']):
                                    X_list.append(None)
                                    # continue
                                else:
                                    X_list.append(doc['processed_daily_data'][X]['processed'])
                            # if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                            if Y in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][Y]['processed']):
                                    Y_list.append(None)
                                    # continue
                                else:
                                    Y_list.append(doc['processed_daily_data'][Y]['processed'])
                            # if doc['processed_daily_data'][key]['identifier'].strip() in keys_of_other_X:
                            #     if pd.isnull(doc['processed_daily_data'][key]['processed']):
                            #         # Y_list.append(0)
                            #         continue
                            #     else:
                            #         tempListForOtherX.append(doc['processed_daily_data'][key]['processed'])
                            # if Z is not None:
                            #     # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                            #     if Z in doc['processed_daily_data']:
                            #         if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                            #             # Z_list.append(0)
                            #             continue
                            #         else:
                            #             Z_list.append(doc['processed_daily_data'][Z]['processed'])
                        else:
                            break
                
                if Z is not None:
                    for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+Z: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                        if doc['vessel_loaded_check'] == "Loaded":
                            if doc['final_rep_dt'] != get_close_by_date:
                                # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                                if Z in doc['processed_daily_data']:
                                    if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                                        Z_list.append(None)
                                        # continue
                                    else:
                                        Z_list.append(doc['processed_daily_data'][Z]['processed'])
                            else:
                                break
                #print("END READ DATA AND CREATE LISTS")
                #print("START CREATE OTHER LISTS")
                #print("OTHER X", other_X)
                for key in other_X:
                    if key != 'None':
                        tempList = []
                        # for i in range(len(X_list)):
                        #     tempList.append(other_X[key])
                        # dict_for_dataframe[key] = tempList
                        for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'_id': 0, 'processed_daily_data.'+key: 1, 'final_rep_dt': 1, 'vessel_loaded_check': 1}).sort('final_rep_dt', DESCENDING):
                            if doc['vessel_loaded_check'] == "Loaded":
                                if doc['final_rep_dt'] != get_close_by_date:
                                    if key in doc['processed_daily_data']:
                                        if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                            # Y_list.append(0)
                                            # continue
                                            tempList.append(None)
                                        else:
                                            tempList.append(doc['processed_daily_data'][key]['processed'])
                                else:
                                    break
                        dict_for_dataframe[key] = tempList
            elif load == 'ballast':
                for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+X: 1, 'processed_daily_data.'+Y: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                    if doc['vessel_loaded_check'] == "Ballast":
                        if doc['final_rep_dt'] != get_close_by_date:
                            # tempListForOtherX=[]
                            # for key in doc['processed_daily_data'].keys():
                                # if doc['processed_daily_data'][key]['identifier'].strip() == X:
                            if X in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][X]['processed']):
                                    X_list.append(None)
                                    # continue
                                else:
                                    X_list.append(doc['processed_daily_data'][X]['processed'])
                            # if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                            if Y in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][Y]['processed']):
                                    Y_list.append(None)
                                    # continue
                                else:
                                    Y_list.append(doc['processed_daily_data'][Y]['processed'])
                            # if doc['processed_daily_data'][key]['identifier'].strip() in keys_of_other_X:
                            #     if pd.isnull(doc['processed_daily_data'][key]['processed']):
                            #         # Y_list.append(0)
                            #         continue
                            #     else:
                            #         tempListForOtherX.append(doc['processed_daily_data'][key]['processed'])
                            # if Z is not None:
                            #     # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                            #     if Z in doc['processed_daily_data']:
                            #         if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                            #             # Z_list.append(0)
                            #             continue
                            #         else:
                            #             Z_list.append(doc['processed_daily_data'][Z]['processed'])
                        else:
                            break
                
                if Z is not None:
                    for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+Z: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                        if doc['vessel_loaded_check'] == "Ballast":
                            if doc['final_rep_dt'] != get_close_by_date:
                                # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                                if Z in doc['processed_daily_data']:
                                    if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                                        Z_list.append(None)
                                        # continue
                                    else:
                                        Z_list.append(doc['processed_daily_data'][Z]['processed'])
                            else:
                                break
                #print("END READ DATA AND CREATE LISTS")
                #print("START CREATE OTHER LISTS")
                #print("OTHER X", other_X)
                for key in other_X:
                    if key != 'None':
                        tempList = []
                        # for i in range(len(X_list)):
                        #     tempList.append(other_X[key])
                        # dict_for_dataframe[key] = tempList
                        for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'_id': 0, 'processed_daily_data.'+key: 1, 'final_rep_dt': 1, 'vessel_loaded_check': 1}).sort('final_rep_dt', DESCENDING):
                            if doc['vessel_loaded_check'] == "Ballast":
                                if doc['final_rep_dt'] != get_close_by_date:
                                    if key in doc['processed_daily_data']:
                                        if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                            # Y_list.append(0)
                                            # continue
                                            tempList.append(None)
                                        else:
                                            tempList.append(doc['processed_daily_data'][key]['processed'])
                                else:
                                    break
                        dict_for_dataframe[key] = tempList
        elif compare == "LastYear":
            get_close_by_date_current_year, get_close_by_date_last_year = self.create_maindb_according_to_duration(duration, compare=compare)
            if load == 'any':
                for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+X: 1, 'processed_daily_data.'+Y: 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                    if doc['final_rep_dt'] != get_close_by_date_current_year:
                        # tempListForOtherX=[]
                        # for key in doc['processed_daily_data'].keys():
                            # if doc['processed_daily_data'][key]['identifier'].strip() == X:
                        if X in doc['processed_daily_data']:
                            if pd.isnull(doc['processed_daily_data'][X]['processed']):
                                X_list.append(None)
                                # continue
                            else:
                                X_list.append(doc['processed_daily_data'][X]['processed'])
                        # if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                        if Y in doc['processed_daily_data']:
                            if pd.isnull(doc['processed_daily_data'][Y]['processed']):
                                Y_list.append(None)
                                # continue
                            else:
                                Y_list.append(doc['processed_daily_data'][Y]['processed'])
                    else:
                        break
                # X_last_list = X_list.copy()
                # Y_last_list = Y_list.copy()
                for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$lt': get_close_by_date_current_year, '$gt': get_close_by_date_last_year}}, {'final_rep_dt': 1, 'processed_daily_data.'+X: 1, 'processed_daily_data.'+Y: 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                    # if doc['final_rep_dt'] != get_close_by_date_current_year:
                        # tempListForOtherX=[]
                        # for key in doc['processed_daily_data'].keys():
                            # if doc['processed_daily_data'][key]['identifier'].strip() == X:
                    if X in doc['processed_daily_data']:
                        if pd.isnull(doc['processed_daily_data'][X]['processed']):
                            X_last_list.append(None)
                            # continue
                        else:
                            X_last_list.append(doc['processed_daily_data'][X]['processed'])
                    # if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                    if Y in doc['processed_daily_data']:
                        if pd.isnull(doc['processed_daily_data'][Y]['processed']):
                            Y_last_list.append(None)
                            # continue
                        else:
                            Y_last_list.append(doc['processed_daily_data'][Y]['processed'])
                    # else:
                    #     break
                
                if Z is not None:
                    for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+Z: 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                        if doc['final_rep_dt'] != get_close_by_date_current_year:
                            # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                            if Z in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                                    Z_list.append(None)
                                    # continue
                                else:
                                    Z_list.append(doc['processed_daily_data'][Z]['processed'])
                        else:
                            break
                    for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$lt': get_close_by_date_current_year, '$gt': get_close_by_date_last_year}}, {'final_rep_dt': 1, 'processed_daily_data.'+Z: 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                        # if doc['final_rep_dt'] != get_close_by_date_current_year:
                            # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                        if Z in doc['processed_daily_data']:
                            if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                                Z_last_list.append(None)
                                # continue
                            else:
                                Z_last_list.append(doc['processed_daily_data'][Z]['processed'])
                        # else:
                        #     break
                #print("END READ DATA AND CREATE LISTS")
                #print("START CREATE OTHER LISTS")
                #print("OTHER X", other_X)
                for key in other_X:
                    if key != 'None':
                        tempList = []
                        tempLastList = []
                        # for i in range(len(X_list)):
                        #     tempList.append(other_X[key])
                        # dict_for_dataframe[key] = tempList
                        for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'_id': 0, 'processed_daily_data.'+key: 1, 'final_rep_dt': 1}).sort('final_rep_dt', DESCENDING):
                            if doc['final_rep_dt'] != get_close_by_date_current_year:
                                if key in doc['processed_daily_data']:
                                    if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                        # Y_list.append(0)
                                        # continue
                                        tempList.append(None)
                                    else:
                                        tempList.append(doc['processed_daily_data'][key]['processed'])
                            else:
                                break
                        for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$lt': get_close_by_date_current_year, '$gt': get_close_by_date_last_year}}, {'_id': 0, 'processed_daily_data.'+key: 1, 'final_rep_dt': 1}).sort('final_rep_dt', DESCENDING):
                            # if doc['final_rep_dt'] != get_close_by_date_current_year:
                            if key in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                    # Y_list.append(0)
                                    # continue
                                    tempLastList.append(None)
                                else:
                                    tempLastList.append(doc['processed_daily_data'][key]['processed'])
                            # else:
                            #     break
                        dict_for_dataframe[key] = tempList
                        dict_for_dataframe['ly_'+key] = tempLastList
            elif load == 'loaded':
                for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+X: 1, 'processed_daily_data.'+Y: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                    if doc['vessel_loaded_check'] == "Loaded":
                        if doc['final_rep_dt'] != get_close_by_date_current_year:
                            # tempListForOtherX=[]
                            # for key in doc['processed_daily_data'].keys():
                                # if doc['processed_daily_data'][key]['identifier'].strip() == X:
                            if X in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][X]['processed']):
                                    X_list.append(None)
                                    # continue
                                else:
                                    X_list.append(doc['processed_daily_data'][X]['processed'])
                            # if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                            if Y in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][Y]['processed']):
                                    Y_list.append(None)
                                    # continue
                                else:
                                    Y_list.append(doc['processed_daily_data'][Y]['processed'])
                        else:
                            break
                
                for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$lt': get_close_by_date_current_year, '$gt': get_close_by_date_last_year}}, {'final_rep_dt': 1, 'processed_daily_data.'+X: 1, 'processed_daily_data.'+Y: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                    if doc['vessel_loaded_check'] == "Loaded":
                        # if doc['final_rep_dt'] != get_close_by_date_current_year:
                            # tempListForOtherX=[]
                            # for key in doc['processed_daily_data'].keys():
                                # if doc['processed_daily_data'][key]['identifier'].strip() == X:
                        if X in doc['processed_daily_data']:
                            if pd.isnull(doc['processed_daily_data'][X]['processed']):
                                X_last_list.append(None)
                                # continue
                            else:
                                X_last_list.append(doc['processed_daily_data'][X]['processed'])
                        # if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                        if Y in doc['processed_daily_data']:
                            if pd.isnull(doc['processed_daily_data'][Y]['processed']):
                                Y_last_list.append(None)
                                # continue
                            else:
                                Y_last_list.append(doc['processed_daily_data'][Y]['processed'])
                        # else:
                        #     break
                
                if Z is not None:
                    for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+Z: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                        if doc['vessel_loaded_check'] == "Loaded":
                            if doc['final_rep_dt'] != get_close_by_date_current_year:
                                # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                                if Z in doc['processed_daily_data']:
                                    if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                                        Z_list.append(None)
                                        # continue
                                    else:
                                        Z_list.append(doc['processed_daily_data'][Z]['processed'])
                            else:
                                break
                    for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$lt': get_close_by_date_current_year, '$gt': get_close_by_date_last_year}}, {'final_rep_dt': 1, 'processed_daily_data.'+Z: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                        if doc['vessel_loaded_check'] == "Loaded":
                            # if doc['final_rep_dt'] != get_close_by_date_current_year:
                                # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                            if Z in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                                    Z_last_list.append(None)
                                    # continue
                                else:
                                    Z_last_list.append(doc['processed_daily_data'][Z]['processed'])
                            # else:
                            #     break
                for key in other_X:
                    if key != 'None':
                        tempList = []
                        tempLastList = []
                        # for i in range(len(X_list)):
                        #     tempList.append(other_X[key])
                        # dict_for_dataframe[key] = tempList
                        for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'_id': 0, 'processed_daily_data.'+key: 1, 'final_rep_dt': 1, 'vessel_loaded_check': 1}).sort('final_rep_dt', DESCENDING):
                            if doc['vessel_loaded_check'] == "Loaded":
                                if doc['final_rep_dt'] != get_close_by_date_current_year:
                                    if key in doc['processed_daily_data']:
                                        if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                            # Y_list.append(0)
                                            # continue
                                            tempList.append(None)
                                        else:
                                            tempList.append(doc['processed_daily_data'][key]['processed'])
                                else:
                                    break
                        for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$lt': get_close_by_date_current_year, '$gt': get_close_by_date_last_year}}, {'_id': 0, 'processed_daily_data.'+key: 1, 'final_rep_dt': 1, 'vessel_loaded_check': 1}).sort('final_rep_dt', DESCENDING):
                            if doc['vessel_loaded_check'] == "Loaded":
                                # if doc['final_rep_dt'] != get_close_by_date_current_year:
                                if key in doc['processed_daily_data']:
                                    if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                        # Y_list.append(0)
                                        # continue
                                        tempLastList.append(None)
                                    else:
                                        tempLastList.append(doc['processed_daily_data'][key]['processed'])
                                # else:
                                #     break
                        dict_for_dataframe[key] = tempList
                        dict_for_dataframe['ly_'+key] = tempLastList
            elif load == 'ballast':
                for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+X: 1, 'processed_daily_data.'+Y: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                    if doc['vessel_loaded_check'] == "Ballast":
                        if doc['final_rep_dt'] != get_close_by_date_current_year:
                            # tempListForOtherX=[]
                            # for key in doc['processed_daily_data'].keys():
                                # if doc['processed_daily_data'][key]['identifier'].strip() == X:
                            if X in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][X]['processed']):
                                    X_list.append(None)
                                    # continue
                                else:
                                    X_list.append(doc['processed_daily_data'][X]['processed'])
                            # if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                            if Y in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][Y]['processed']):
                                    Y_list.append(None)
                                    # continue
                                else:
                                    Y_list.append(doc['processed_daily_data'][Y]['processed'])
                        else:
                            break
                
                for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$lt': get_close_by_date_current_year, '$gt': get_close_by_date_last_year}}, {'final_rep_dt': 1, 'processed_daily_data.'+X: 1, 'processed_daily_data.'+Y: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                    if doc['vessel_loaded_check'] == "Ballast":
                        # if doc['final_rep_dt'] != get_close_by_date_current_year:
                            # tempListForOtherX=[]
                            # for key in doc['processed_daily_data'].keys():
                                # if doc['processed_daily_data'][key]['identifier'].strip() == X:
                        if X in doc['processed_daily_data']:
                            if pd.isnull(doc['processed_daily_data'][X]['processed']):
                                X_last_list.append(None)
                                # continue
                            else:
                                X_last_list.append(doc['processed_daily_data'][X]['processed'])
                        # if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                        if Y in doc['processed_daily_data']:
                            if pd.isnull(doc['processed_daily_data'][Y]['processed']):
                                Y_last_list.append(None)
                                # continue
                            else:
                                Y_last_list.append(doc['processed_daily_data'][Y]['processed'])
                        # else:
                        #     break
                
                if Z is not None:
                    for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1, 'processed_daily_data.'+Z: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                        if doc['vessel_loaded_check'] == "Ballast":
                            if doc['final_rep_dt'] != get_close_by_date_current_year:
                                # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                                if Z in doc['processed_daily_data']:
                                    if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                                        Z_list.append(None)
                                        # continue
                                    else:
                                        Z_list.append(doc['processed_daily_data'][Z]['processed'])
                            else:
                                break
                    
                    for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$lt': get_close_by_date_current_year, '$gt': get_close_by_date_last_year}}, {'final_rep_dt': 1, 'processed_daily_data.'+Z: 1, 'vessel_loaded_check': 1, '_id': 0}).sort('final_rep_dt', DESCENDING):
                        if doc['vessel_loaded_check'] == "Ballast":
                            # if doc['final_rep_dt'] != get_close_by_date_current_year:
                                # if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                            if Z in doc['processed_daily_data']:
                                if pd.isnull(doc['processed_daily_data'][Z]['processed']):
                                    Z_last_list.append(None)
                                    # continue
                                else:
                                    Z_last_list.append(doc['processed_daily_data'][Z]['processed'])
                            # else:
                            #     break
                for key in other_X:
                    if key != 'None':
                        tempList = []
                        tempLastList = []
                        # for i in range(len(X_list)):
                        #     tempList.append(other_X[key])
                        # dict_for_dataframe[key] = tempList
                        for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'_id': 0, 'processed_daily_data.'+key: 1, 'final_rep_dt': 1, 'vessel_loaded_check': 1}).sort('final_rep_dt', DESCENDING):
                            if doc['vessel_loaded_check'] == "Ballast":
                                if doc['final_rep_dt'] != get_close_by_date_current_year:
                                    if key in doc['processed_daily_data']:
                                        if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                            # Y_list.append(0)
                                            # continue
                                            tempList.append(None)
                                        else:
                                            tempList.append(doc['processed_daily_data'][key]['processed'])
                                else:
                                    break
                        
                        for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$lt': get_close_by_date_current_year, '$gt': get_close_by_date_last_year}}, {'_id': 0, 'processed_daily_data.'+key: 1, 'final_rep_dt': 1, 'vessel_loaded_check': 1}).sort('final_rep_dt', DESCENDING):
                            if doc['vessel_loaded_check'] == "Ballast":
                                # if doc['final_rep_dt'] != get_close_by_date_current_year:
                                if key in doc['processed_daily_data']:
                                    if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                        # Y_list.append(0)
                                        # continue
                                        tempLastList.append(None)
                                    else:
                                        tempLastList.append(doc['processed_daily_data'][key]['processed'])
                                # else:
                                #     break
                        dict_for_dataframe[key] = tempList
                        dict_for_dataframe['ly_'+key] = tempLastList
            
        # keys_of_other_X = list(other_X.keys())
        
        #print("START READ DATA AND CREATE LISTS")
        
        #print("DICT FOR DATAFRAME!!!", dict_for_dataframe)
        for key in dict_for_dataframe.keys():
            print("LENGTHS OF INDI ", len(dict_for_dataframe[key]))
        #print("END CREATE OTHER LISTS")
        
        dict_for_dataframe[X] = X_list
        dict_for_dataframe[Y] = Y_list
        if compare == 'LastYear':
            dict_for_dataframe['ly_'+X] = X_last_list
            dict_for_dataframe['ly_'+Y] = Y_last_list
        X_name = shortNameDict[X]
        Y_name = shortNameDict[Y]
        #print("X LIST", X_list)
        if len(X_list) > len(Y_list):
            avg = sum(Y_list) / len(Y_list)
            for i in range(len(X_list)-len(Y_list)):
                Y_list.append(avg)
        elif len(X_list) < len(Y_list):
            for i in range(len(Y_list)-len(X_list)):
                Y_list.pop()
        
        if Z is not None and len(Z_list) != 0:
            if len(X_list) > len(Z_list):
                avg = sum(Z_list) / len(Z_list)
                for i in range(len(X_list)-len(Z_list)):
                    Z_list.append(avg)
            elif len(X_list) < len(Z_list):
                for i in range(len(Z_list)-len(X_list)):
                    Z_list.pop()
        
        if compare == 'LastYear':
            if len(X_last_list) > len(Y_last_list):
                avg = sum(Y_last_list) / len(Y_last_list)
                for i in range(len(X_last_list)-len(Y_last_list)):
                    Y_last_list.append(avg)
            elif len(X_last_list) < len(Y_last_list):
                for i in range(len(Y_last_list)-len(X_last_list)):
                    Y_last_list.pop()
            
            if Z is not None and len(Z_list) != 0:
                if len(X_last_list) > len(Z_last_list):
                    avg = sum(Z_last_list) / len(Z_last_list)
                    for i in range(len(X_last_list)-len(Z_last_list)):
                        Z_last_list.append(avg)
                elif len(X_last_list) < len(Z_last_list):
                    for i in range(len(Z_last_list)-len(X_last_list)):
                        Z_last_list.pop()
        #print("LENGTHS!!!!!!!!!!!!!", len(Z_list), len(Y_list), len(X_list))
        # empty_column_list = []
        if Z is not None and len(Z_list) != 0:
            dict_for_dataframe[Z] = Z_list
            if compare == 'LastYear':
                dict_for_dataframe['ly_'+Z] = Z_last_list
            Z_name = shortNameDict[Z]
            dataframe = pd.DataFrame(dict_for_dataframe)
            for col in dataframe.columns:
                if pd.isnull(dataframe[col]).all():
                    # empty_column_list.append(col)
                    dataframe=dataframe.drop(columns=col)
            dataframe=dataframe.dropna()
            dataframe=dataframe.reset_index(drop=True)
            #print("DATAFRAME BEFORE", dataframe)
            return dataframe, X_name, Y_name, Z_name, X_list, Y_list, Z_list
        else:
            dataframe = pd.DataFrame(dict_for_dataframe)
            for col in dataframe.columns:
                if pd.isnull(dataframe[col]).all():
                    # empty_column_list.append(col)
                    dataframe=dataframe.drop(columns=col)
            dataframe=dataframe.dropna()
            dataframe=dataframe.reset_index(drop=True)
            # #print("DATAFRAME BEFORE", dataframe)
            # dataframe.to_csv("Interactive.csv")
            return dataframe, X_name, Y_name, X_list, Y_list
        # self.maindb = maindb_collection.find({"ship_imo": self.ship_imo}).sort('processed_daily_data.rep_dt.processed', ASCENDING)
        # full_maindb = loads(dumps(self.maindb))
        # maindb = self.create_maindb_according_to_duration(duration, full_maindb)
        # X_list = []
        # Y_list = []
        # Z_list = []

        # shortNameDict = self.create_short_names_dictionary()
        # # variables = [i for i in other_X.keys()]
        # # stats = self.get_ship_stats(maindb, *variables)
        # dict_for_dataframe = {}
        # for val_dict in maindb:
        #     for key in val_dict['processed_daily_data'].keys():
        #         if val_dict['processed_daily_data'][key]['identifier'].strip() == X:
        #             X_list.append(val_dict['processed_daily_data'][key]['processed'])
        #         if val_dict['processed_daily_data'][key]['identifier'].strip() == Y:
        #             Y_list.append(val_dict['processed_daily_data'][key]['processed'])
        #         if Z is not None:
        #             if val_dict['processed_daily_data'][key]['identifier'].strip() == Z:
        #                 Z_list.append(val_dict['processed_daily_data'][key]['processed'])
        # for key in other_X:
        #     if key != "None":
        #         tempList=[]
        #         for i in range(len(X_list)):
        #             tempList.append(other_X[key])
        #         dict_for_dataframe[key] = tempList
        # #print(len(X_list), len(Y_list), len(Z_list))
        # dict_for_dataframe[X] = X_list
        # dict_for_dataframe[Y] = Y_list
        # if len(Z_list) != 0:
        #     dict_for_dataframe[Z] = Z_list
        # dataframe = pd.DataFrame(dict_for_dataframe)
        # if Z is not None:
        #     X_name = shortNameDict[X]
        #     Y_name = shortNameDict[Y]
        #     Z_name = shortNameDict[Z]
        #     return dataframe, X_name, Y_name, Z_name, X_list, Y_list, Z_list
        # else:
        #     X_name = shortNameDict[X]
        #     Y_name = shortNameDict[Y]
        #     return dataframe, X_name, Y_name, X_list, Y_list
    #print("START CREATE ACC TO DURATION")

    def create_surface_data(self, X, Y, duration, Z, load, compare, sis_or_sim="", **other_X):
        # maindb_collection = database.get_collection("Main_db")
        # #print("START SHORT NAME DICT")
        # shortNameDict = self.create_short_names_dictionary()
        # #print("END SHORT NAME DICT")
        # get_close_by_date = self.create_maindb_according_to_duration(duration)
        # X_list = []
        # Y_list = []
        # Z_list = []
        # dict_for_dataframe = {}
        # #print("START READ DATA AND CREATE LISTS")
        # for doc in maindb_collection.find({'ship_imo': self.ship_imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING):
        #     if doc['processed_daily_data']['rep_dt']['processed'] != get_close_by_date:
        #         for key in doc['processed_daily_data'].keys():
        #             if doc['processed_daily_data'][key]['identifier'].strip() == X:
        #                 if pd.isnull(doc['processed_daily_data'][key]['processed']):
        #                     # X_list.append(0)
        #                     continue
        #                 else:
        #                     X_list.append(doc['processed_daily_data'][key]['processed'])
        #             if doc['processed_daily_data'][key]['identifier'].strip() == Y:
        #                 if pd.isnull(doc['processed_daily_data'][key]['processed']):
        #                     # Y_list.append(0)
        #                     continue
        #                 else:
        #                     Y_list.append(doc['processed_daily_data'][key]['processed'])
        #             if doc['processed_daily_data'][key]['identifier'].strip() == Z:
        #                 if pd.isnull(doc['processed_daily_data'][key]['processed']):
        #                     # Z_list.append(0)
        #                     continue
        #                 else:
        #                     Z_list.append(doc['processed_daily_data'][key]['processed'])
        #     else:
        #         break
        # #print("END READ DATA AND CREATE LISTS")

        # # new_Z_list = [min(Z_list)]
        # # for i in range(len(X_list)):
        # #     numToPush = 100 + new_Z_list[len(new_Z_list) - 1] * 1.5
        # #     new_Z_list.append(numToPush)
        # new_X_list = np.linspace(min(X_list), max(X_list), len(X_list))
        # new_Z_list = np.linspace(min(Z_list), max(Z_list), len(X_list))
        # new_Y_list = np.linspace(min(Y_list), max(Y_list), len(X_list))
        # # Y_list_2d = [new_Y_list.tolist() for i in range(int(len(X_list)/2))]
        # Y_list_2d = [Y_list for i in range(int(len(X_list)/2))]
        # ####################################################
        # ''' Taking care of list length imbalance'''
        # # if len(X_list) == len(Z_list):
        # #     Y_list_new = self.createY(X_list, Z_list)
        # # elif len(X_list) > len(Z_list):
        # #     if len(X_list) - len(Z_list) == 1:
        # #         Z_list.append(min(Z_list))
        # #         Y_list_new = self.createY(X_list, Z_list)
        # #     elif len(X_list) - len(Z_list) > 1:
        # #         diff = len(X_list) - len(Z_list)
        # #         for i in range(0, diff):
        # #             random_index = random.randint(0, len(Z_list)-1)
        # #             Z_list.append(Z_list[random_index])
        # #         Y_list_new = self.createY(X_list, Z_list)
        # # elif len(X_list) < len(Z_list):
        # #     if len(Z_list) - len(X_list) == 1:
        # #         Z_list.pop()
        # #         Y_list_new = self.createY(X_list, Z_list)
        # #     elif len(Z_list) - len(X_list) > 1:
        # #         diff = len(Z_list) - len(X_list)
        # #         for i in range(0, diff):
        # #             Z_list.pop()
        # #         Y_list_new = self.createY(X_list, Z_list)
        # if len(X_list) > len(Y_list):
        #     avg = sum(Y_list) / len(Y_list)
        #     for i in range(len(X_list)-len(Y_list)):
        #         Y_list.append(avg)
        # elif len(X_list) < len(Y_list):
        #     for i in range(len(Y_list)-len(X_list)):
        #         Y_list.pop()

        # if len(X_list) > len(Z_list):
        #     avg = sum(Z_list) / len(Z_list)
        #     for i in range(len(X_list)-len(Z_list)):
        #         Z_list.append(avg)
        # elif len(X_list) < len(Z_list):
        #     for i in range(len(Z_list)-len(X_list)):
        #         Z_list.pop()


        
        # #print("START CREATE OTHER LISTS")
        # # for key in other_X:
        # #     if key != 'None':
        # #         tempList = []
        # #         for i in range(len(X_list)):
        # #             tempList.append(other_X[key])
        # #         dict_for_dataframe[key] = tempList
        # #print("END CREATE OTHER LISTS")
        
        # # dict_for_dataframe[X] = X_list
        # # dict_for_dataframe[Y] = Y_list
        # X_name = shortNameDict[X]
        # Y_name = shortNameDict[Y]
        # # dict_for_dataframe[Z] = Z_list
        # Z_name = shortNameDict[Z]
        #########################################################
        empty_x_constant_list = []
        dataframe, X_name, Y_name, Z_name, X_list, Y_list, Z_list = self.create_dataframe(X, Y, duration, load, compare, sis_or_sim, Z, **other_X)
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
                if col not in list(other_X.keys()):
                    empty_x_constant_list.append(col)
        X1, Y1, pred_list, clean_X_list, clean_Y_list, clean_Z_list = self.regress_for_constant_x(dataframe, X, Y, Z, **other_X)
        # Y1.extend(Y_list)
        # pred_list.append(max(Y1))
        # temp_pred_list=[5119.2, 11638.7]
        pred_list.sort()
        # equispaced_pred_list = np.linspace(min(pred_list), max(pred_list), len(X1))
        # equispaced_pred_list.tolist()
        # #print(type(equispaced_pred_list))
        X1.sort()
        Y1.sort()
        X_list.sort(key=lambda e: (e is None, e))
        Y_list.sort(key=lambda e: (e is None, e))
        Z_list.sort(key=lambda e: (e is None, e))
        new_pred_list = []
        for i in range(int(len(X_list))):
            # new_pred_list.append(equispaced_pred_list.tolist())
            new_pred_list.append(pred_list)
        #print(type(new_pred_list))
        return X_name, Y_name, Z_name, X1, new_pred_list, Y1, clean_X_list, clean_Y_list, clean_Z_list
        # return X_name, Y_name, Z_name, new_X_list.tolist(), Y_list_2d, new_Z_list.tolist(), X_list, Y_list, Z_list
    
    def clean_data(self, new_data):
        for column in new_data:
            new_data=new_data[new_data[column]!='']
            new_data=new_data[new_data[column]!=' ']
            new_data=new_data[new_data[column]!='  ']
            new_data=new_data[new_data[column]!='r[a-zA-Z]']
        new_data=new_data.reset_index(drop=True)

        for col in new_data.columns:
            for i in range(0,len(new_data)):
                if i in new_data.index and type(new_data[col][i])==str:
                    
                    new_data=new_data[new_data[col]!=new_data[col][i]]
                
                if i in new_data.index and type(new_data[col][i])==datetime:
                    
                    new_data=new_data[new_data[col]!=new_data[col][i]]
                    

            if pd.isnull(new_data[col]).all():
                new_data=new_data.drop(columns=col)
            
        new_data=new_data.dropna()
        new_data=new_data.reset_index(drop=True)

        return new_data
    
    def z_score_data(self, new_data, identifier):
        mean_val=np.mean(new_data[identifier])
        standdev=np.std(new_data[identifier])
        zsc_list=[]
        try:
            z_score=st.zscore(new_data[identifier])
            new_data['z_score']=z_score
        except:
            for rowval in new_data[identifier]:
                zsc=(rowval-mean_val)/standdev
                zsc_list.append(zsc)
            new_data['z_score']=zsc_list
        new_data= new_data.drop(index=new_data[new_data['z_score'] > 2].index)
        new_data= new_data.drop(index=new_data[new_data['z_score'] < -2].index)
        # new_data=new_data.reset_index(drop=True)
        new_data=new_data.drop(columns='z_score')
        # new_data=new_data.reset_index(drop=True)
        # #print(new_data)
        return new_data

    def z_score_data_3(self, new_data, identifier):
        mean_val=np.mean(new_data[identifier])
        standdev=np.std(new_data[identifier])
        zsc_list=[]
        try:
            z_score=st.zscore(new_data[identifier])
            new_data['z_score']=z_score
        except:
            for rowval in new_data[identifier]:
                zsc=(rowval-mean_val)/standdev
                zsc_list.append(zsc)
            new_data['z_score']=zsc_list

        new_data= new_data.drop(index=new_data[new_data['z_score'] > 3].index)
        new_data= new_data.drop(index=new_data[new_data['z_score'] < -3].index)
        # new_data=new_data.reset_index(drop=True)
        new_data=new_data.drop(columns='z_score')
        # new_data=new_data.reset_index(drop=True)
        # #print(new_data)
        return new_data

    def create_maindb_according_to_duration(self, duration, compare="None", main_db=[]):
        # #print("\n",main_db)
        # if main_db is not []:
        #     # #print(loads(dumps(main_db)))
        #     # maindb = loads(dumps(main_db))
        #     maindb = main_db
        #     # #print(maindb)
        # else:
        # compare == LastYear
        # duration == Lastyear180 or Lastyear
        # compare == None
        # duration == 180Days or 1Year
        dateList=[]
        maindb_collection = database.get_collection("Main_db")
        #print("START CREATE DATE LIST")
        for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1}).sort('final_rep_dt', ASCENDING):
            dateList.append(doc['final_rep_dt'])
        # for index, item in enumerate(maindb):
        #     dateList.append(item['processed_daily_data']['rep_dt']['processed'])
        # for doc in maindb_collection.find({'ship_imo': self.ship_imo}):
        #     dateList.append(doc['processed_daily_data']['rep_dt']['processed'])
        #print("END CREATE DATE LIST")
        #print("START SORT")
        # dateList.sort()
        #print("END SORT")
        current_date = dateList[-1]
        
        #print("NUMBER OF DAYS", number_of_days)
        # compare = None, LastYear, sister_or_similar_imo, all_sisters
        if compare == "None" or compare == "all_sisters" or compare == True:
            # if 'Days' in duration or 'Year' in duration else duration
            number_of_days = self.get_duration(duration)
            # if 'Year' in duration:
            #     days_to_subtract = timedelta(weeks = 52)
            # else:
            days_to_subtract = timedelta(days = int(number_of_days))
            to_date = current_date - days_to_subtract
            
            if to_date in dateList:
                return to_date
            else:
                return min(dateList, key=lambda x: abs(x - to_date))
        elif compare == "LastYear":
            if duration == 'Lastyear':
                number_of_days_current_year = self.get_duration('1Year')
                number_of_days_last_year = self.get_duration('Lastyear')

                days_to_subtract_from_current = timedelta(days = int(number_of_days_current_year))
                days_to_subtract_from_last = timedelta(days = int(number_of_days_last_year))

                to_date_current_year = current_date - days_to_subtract_from_current
                to_date_last_year = current_date - days_to_subtract_from_last

                if to_date_current_year in dateList and to_date_last_year in dateList:
                    return to_date_current_year, to_date_last_year
                elif to_date_current_year not in dateList and to_date_last_year not in dateList:
                    return min(dateList, key=lambda x: abs(x - to_date_current_year)), min(dateList, key=lambda x: abs(x - to_date_last_year))
                elif to_date_current_year in dateList and to_date_last_year not in dateList:
                    return to_date_current_year, min(dateList, key=lambda x: abs(x - to_date_last_year))
                elif to_date_last_year in dateList and to_date_current_year not in dateList:
                    return min(dateList, key=lambda x: abs(x - to_date_current_year)), to_date_last_year
            elif duration == 'Lastyear180':
                number_of_days_current_year = self.get_duration('180Days')
                number_of_days_last_year = self.get_duration('Lastyear180')

                days_to_subtract_from_current = timedelta(days = int(number_of_days_current_year))
                days_to_subtract_from_last = timedelta(days = int(number_of_days_last_year))

                to_date_current_year = current_date - days_to_subtract_from_current
                to_date_last_year = current_date - days_to_subtract_from_last

                if to_date_current_year in dateList and to_date_last_year in dateList:
                    return to_date_current_year, to_date_last_year
                elif to_date_current_year not in dateList and to_date_last_year not in dateList:
                    return min(dateList, key=lambda x: abs(x - to_date_current_year)), min(dateList, key=lambda x: abs(x - to_date_last_year))
                elif to_date_current_year in dateList and to_date_last_year not in dateList:
                    return to_date_current_year, min(dateList, key=lambda x: abs(x - to_date_last_year))
                elif to_date_last_year in dateList and to_date_current_year not in dateList:
                    return min(dateList, key=lambda x: abs(x - to_date_current_year)), to_date_last_year

        
        #     self.maindb = maindb_collection.find({"ship_imo": self.ship_imo}).sort('processed_daily_data.rep_dt.processed', ASCENDING)
        #     maindb = loads(dumps(self.maindb))
        #     #print(len(maindb))
        # number_of_days = self.get_duration(duration)
        
        # current_date = maindb[len(maindb) - 1]['processed_daily_data']['rep_dt']['processed']
        # if 'Year' in duration:
        #     days_to_subtract = timedelta(weeks=52)
        # else:
        #     days_to_subtract = timedelta(days = number_of_days)
        # dateList=[]
        # actual_maindb=[]

        # to_date = current_date - days_to_subtract

        # for val_dict in range(len(maindb)):
        #     dateList.append(maindb[val_dict]['processed_daily_data']['rep_dt']['processed'])

        # get_close_by_date = self.get_nearest_date(dateList, to_date)

        # for val_dict in reversed(maindb):
        #     tempDate = val_dict['processed_daily_data']['rep_dt']['processed']
        #     if tempDate != get_close_by_date:
        #         actual_maindb.append(val_dict)
        #     else:
        #         break
        # return get_close_by_date
    #print("END CREATE ACC TO DURATION")
    
    def get_duration(self, duration):
        ''' Returns the number of days of past data to fetch from maindb'''
        number_of_days=duration_mapping[duration]
        return number_of_days
        # for i in duration_mapping.keys():
        #     if i == duration:
        #         number_of_days = duration_mapping[i]
        #         return number_of_days
        #     else:
        #         return number_of_days
    
    def get_nearest_date(self, dateList, compareDate):
        ''' Returns the date nearest to compareDate, if compareDate not present, else returns compareDate itself.'''
        if compareDate in dateList:
            return compareDate
        else:
            return min(dateList, key=lambda x: abs(x - compareDate))
    
    def createY(self, list1, list2):
        ''' Creates the list for the dependent parameter i.e., Y'''
        new_list=[]
        for i in range(len(list1)):
            temp1 = list1[i] * 20
            temp2 = list2[i] * 6
            temp3 = (list1[i] ** 2) * 0.05 + random.randint(1, 10)
            numToPush = temp1 + temp2 - temp3
            new_list.append(numToPush)
        
        return new_list
        
    
    def get_ship_stats_2(self, data, *variables):
        data_dict={}
        stats_dict={}
        maindb_collection = database.get_collection("Main_db")
        # maindb = maindb_collection.find({'ship_imo': self.ship_imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING)
        for i in variables:
            tempList=[]
            #print("VARIABLES!!!!!!!", i)
            for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'processed_daily_data.'+i: 1, 'processed_daily_data.rep_dt.processed': 1}).sort('processed_daily_data.rep_dt.processed', DESCENDING):
                if doc['processed_daily_data']['rep_dt']['processed'] != data:
                    if i in doc['processed_daily_data'] and pd.isnull(doc['processed_daily_data'][i]['processed']) == False:
                        tempList.append(doc['processed_daily_data'][i]['processed'])
                else:
                    break
            try:
                data_dict[i] = {"Min": math.floor(min(tempList)), "Max": math.ceil(max(tempList))}
            except ValueError:
                continue
            except TypeError:
                continue
        # for i in variables:
        #     tempList=[]
        #     for val_dict in range(len(data)):
        #         for key in data[val_dict]['processed_daily_data'].keys():
        #             if data[val_dict]['processed_daily_data'][key]['identifier'].strip() == i:
        #                 tempList.append(data[val_dict]['processed_daily_data'][key]['processed'])
        #     data_dict[i] = tempList
        
        # for key in data_dict.keys():
        #     try:
        #         minValue = min(data_dict[key])
        #         maxValue = max(data_dict[key])
        #         stats_dict[key] = {"Min": math.floor(minValue), "Max": math.ceil(maxValue)}
        #     except TypeError:
        #         continue
        #     except ValueError:
        #         continue
        # #print(stats_dict)
        return data_dict
    
    def get_individual_parameters(self, equip=False, index=False):
        ''' Gets all the parameters from the ship collection. Returns a list of dictionary created for react-select dropdown.
            Will have two keys - label, and value. label will be short name, value will be identifier new. 
        '''
        optionsList=[]
        singledata = self.ship_configs['data']
        indices_data = self.ship_configs['indices_data']

        if equip == False and index == False:
            for var in singledata.keys():
                try:
                    if singledata[var]['dependent'] == False or singledata[var]['dependent'] == True:
                        temp = {'value': var.strip(), 'label': singledata[var]['short_names'].strip()}
                        optionsList.append(temp)
                except AttributeError:
                    temp = {'value': var, 'label': var}
                    optionsList.append(temp)
        if equip == True:
            for var in singledata.keys():
                try:
                    if singledata[var]['var_type'] == 'E' or singledata[var]['var_type'] == 'E1':
                        temp = {'value': var.strip(), 'label': singledata[var]['short_names'].strip()}
                        optionsList.append(temp)
                except AttributeError:
                    temp = {'value': var, 'label': var}
                    optionsList.append(temp)
        if index == True:
            for var in indices_data.keys():
                try:
                    if indices_data[var]['var_type'] == 'INDX' or indices_data[var]['var_type'] == 'T2&SPE':
                        temp = {'value': var, 'label': indices_data[var]['short_names'].strip() if pd.isnull(indices_data[var]['short_names'].strip()) == False else var}
                        optionsList.append(temp)
                except AttributeError:
                    temp = {'value': var, 'label': var}
                    optionsList.append(temp)
        
        optionsList.append({'value': 'None', 'label': 'None'})
        return optionsList
    
    ''' Functions for Overview Processing'''
    def create_imo_and_name_strings(self, id):
        ''' Returns the list of strings of the ships.
            Strings format: imo<br />name
        '''
        ship_collection = self.get_ship_configs()
        result=[]

        if id == "":
            for doc in ship_collection.find().sort('ship_imo', ASCENDING):
                ship_imo = doc['ship_imo']
                ship_name = doc['static_data']['ship_name']['value'].strip()
                new_string = str(ship_imo) + '-' + ship_name
                result.append(new_string)
        else:
            for doc in ship_collection.find({'organization_id': {"$in": [id, 'default']}}).sort('ship_imo', ASCENDING):
                ship_imo = doc['ship_imo']
                ship_name = doc['static_data']['ship_name']['value'].strip()
                new_string = str(ship_imo) + '-' + ship_name
                result.append(new_string)
        
        return result
    
    def get_list_of_ship_imos(self, id):
        ''' Returns a list of ship imos'''
        ship_collection = self.get_ship_configs()
        result=[]

        if id == "":
            for doc in ship_collection.find().sort('ship_imo', ASCENDING):
                result.append(doc['ship_imo'])
        else:
            for doc in ship_collection.find({'organization_id': {"$in": [id, 'default']}}).sort('ship_imo', ASCENDING):
                result.append(doc['ship_imo'])
        return result
    
    def create_vessel_load_list(self, id):
        ''' Returns the vessel load of each ship in a dictionary.'''
        maindb_collection = self.get_main_data()
        ship_imo_list = self.get_list_of_ship_imos(id)
        result = {}
        for imo in ship_imo_list:
            #print("IMO!!!!!!!!!!!!!", imo)
            for doc in maindb_collection.find({'ship_imo': imo}, {'vessel_loaded_check': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', DESCENDING).limit(1):
                if 'vessel_loaded_check' in doc:
                    result[imo] = doc['vessel_loaded_check']
                    # result.append(doc['vessel_loaded_check'])
                else:
                    result[imo] = ""
                    # result.append("")
                # #print("VESSEL LOADED CHECK!", doc['vessel_loaded_check']) if imo == 9591375 else #print("VESSEL LOADED CHECK!", doc['vessel_loaded_check'])
        
        return result
    
    def create_eta_list(self, id):
        ''' Returns the eta of each ship in a dictionary.'''
        maindb_collection = self.get_main_data()
        ship_imo_list = self.get_list_of_ship_imos(id)
        result = {}

        for imo in ship_imo_list:
            for doc in maindb_collection.find({'ship_imo': imo, "processed_daily_data.eta": {"$exists": True}}, {'processed_daily_data.eta.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', DESCENDING).limit(1):
                #print("DATE!!!!!", doc['processed_daily_data']['eta']['processed'])
                if 'eta' in doc['processed_daily_data']:
                    if type(doc['processed_daily_data']['eta']['processed']) == str:
                        # try:
                        new_date = datetime.strptime(doc['processed_daily_data']['eta']['processed'], '%d-%m-%Y %H:%M:%S')
                        newdate = new_date.strftime('%Y-%m-%d')
                    else:
                        if pd.isnull(doc['processed_daily_data']['eta']['processed']) == False:
                            newdate = doc['processed_daily_data']['eta']['processed'].strftime('%Y-%m-%d')
                        else:
                            newdate = None
                    result[imo] = newdate
                    # result.append(newdate)
                else:
                    result[imo] = ""
                    # result.append("")
                # break
        
        return result
    
    def create_cp_compliance_list(self):
        ''' Returns the cp compliance denoted by the identifier cp_hfo_cons in a list.'''
        maindb_collection = self.get_main_data()
        ship_imo_list = self.get_list_of_ship_imos()
        result = []

        for imo in ship_imo_list:
            for doc in maindb_collection.find({'ship_imo': imo}, {'processed_daily_data.cp_hfo_cons.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', DESCENDING).limit(1):
                if 'cp_hfo_cons' in doc['processed_daily_data']:
                    result.append(doc['processed_daily_data']['cp_hfo_cons']['processed'])
                else:
                    result.append("")
                # break
        
        return result
    
    def create_list_of_active_ships(self, id):
        ''' Returns the list of active ships.'''
        result=[]
        ship_imos = self.get_list_of_ship_imos(id)
        daily_data_collection = database.get_collection('daily_data')
        for imo in ship_imos:
            try:
                for doc in daily_data_collection.find_one({'ship_imo': imo}):
                    if imo not in result:
                        result.append(imo)
            except TypeError:
                result.append(None)
        
        return result
    
    def create_dict_of_issues(self, id, dateString=''):
        ''' Returns a dictionary of all the issues per ship. 
            Also used in the daily report process
            Also used in the trends process
        '''
        ship_imos = self.get_list_of_ship_imos(id)
        maindb_collection = self.get_main_data()
        result = {}
        
        issuesCount = {}
        if dateString != "":
            findDate = datetime.strptime(dateString,"%Y-%m-%d, %H:%M:%S")
            for imo in ship_imos:
            # #print(imo)
                tempList = []
                outlierCount = 0
                operationalCount = 0
                indicesCount = 0
                for doc in maindb_collection.find({'ship_imo': imo, 'final_rep_dt': findDate}):
                    for key in doc['processed_daily_data'].keys():
                        try:
                            if doc['processed_daily_data'][key]['within_outlier_limits']['m3'] == False:
                                # #print("YES")  or (doc['processed_daily_data'][key]['SPEy']['m3'] > doc['processed_daily_data'][key]['Q_y']['m3'][1])
                                if pd.isnull(doc['processed_daily_data'][key]['name']) == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip())
                                    result[str(imo)] = tempList
                            if doc['processed_daily_data'][key]['within_operational_limits']['m3'] == False:
                                if pd.isnull(doc['processed_daily_data'][key]['name']) == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip()+"_OP")
                                    result[str(imo)] = tempList
                            if 'spe_messages' in doc['processed_daily_data'][key].keys() and 'm3' in doc['processed_daily_data'][key]['spe_messages'].keys():
                                # spe_messages_dict[key] = doc['processed_daily_data'][key]['spe_messages'][new_duration][2]
                                if doc['processed_daily_data'][key]['is_not_spe_anamolous']['m3'][2] == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip()+"_SPE")
                                    result[str(imo)] = tempList
                                elif doc['processed_daily_data'][key]['is_not_spe_anamolous']['m3'][1] == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip()+"_SPE")
                                    result[str(imo)] = tempList
                                elif doc['processed_daily_data'][key]['is_not_spe_anamolous']['m3'][0] == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip()+"_SPE")
                                    result[str(imo)] = tempList
                            if doc['processed_daily_data'][key]['within_outlier_limits']['m3'] == False:
                                outlierCount = outlierCount + 1
                            elif doc['processed_daily_data'][key]['within_operational_limits']['m3'] == False:
                                operationalCount = operationalCount + 1
                            # elif doc['processed_daily_data'][key]['SPEy']['m3'] > doc['processed_daily_data'][key]['Q_y']['m3'][1]:
                            #     indicesCount = indicesCount + 1

                        except TypeError:
                            continue
                        except KeyError:
                            continue
                        except IndexError:
                            continue
                issuesCount[imo] = {'outlier': outlierCount, 'operational': operationalCount}
        else:
            for imo in ship_imos:
                # #print(imo)
                tempList = []
                outlierCount = 0
                operationalCount = 0
                indicesCount = 0
                for doc in maindb_collection.find({'ship_imo': imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING).limit(1):
                    for key in doc['processed_daily_data'].keys():
                        try:
                            if doc['processed_daily_data'][key]['within_outlier_limits']['m3'] == False:
                                # #print("YES")  or (doc['processed_daily_data'][key]['SPEy']['m3'] > doc['processed_daily_data'][key]['Q_y']['m3'][1])
                                if pd.isnull(doc['processed_daily_data'][key]['name']) == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip())
                                    result[str(imo)] = tempList
                            if doc['processed_daily_data'][key]['within_operational_limits']['m3'] == False:
                                if pd.isnull(doc['processed_daily_data'][key]['name']) == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip()+"_OP")
                                    result[str(imo)] = tempList
                            if 'spe_messages' in doc['processed_daily_data'][key].keys() and 'm3' in doc['processed_daily_data'][key]['spe_messages'].keys():
                                # spe_messages_dict[key] = doc['processed_daily_data'][key]['spe_messages'][new_duration][2]
                                if doc['processed_daily_data'][key]['is_not_spe_anamolous']['m3'][2] == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip()+"_SPE")
                                    result[str(imo)] = tempList
                                elif doc['processed_daily_data'][key]['is_not_spe_anamolous']['m3'][1] == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip()+"_SPE")
                                    result[str(imo)] = tempList
                                elif doc['processed_daily_data'][key]['is_not_spe_anamolous']['m3'][0] == False:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip()+"_SPE")
                                    result[str(imo)] = tempList
                            
                            if doc['processed_daily_data'][key]['within_outlier_limits']['m3'] == False:
                                outlierCount = outlierCount + 1
                            elif doc['processed_daily_data'][key]['within_operational_limits']['m3'] == False:
                                operationalCount = operationalCount + 1
                            # elif doc['processed_daily_data'][key]['SPEy']['m3'] > doc['processed_daily_data'][key]['Q_y']['m3'][1]:
                            #     indicesCount = indicesCount + 1
                        except TypeError:
                            continue
                        except KeyError:
                            continue
                        except IndexError:
                            continue
                issuesCount[imo] = {'outlier': outlierCount, 'operational': operationalCount}
                    # result[imo] = tempList
                        # result[imo] = tempList
        #print("DICT OF ISSUES", result)
        return result, issuesCount
    
    def create_dict_of_compliance_messages(self, dateString=""):
        '''
            Returns a string of the compliance message of a ship given a date or the most recent when no date is passed.
            Used in daily report.
        '''
        # ship_imos = self.get_list_of_ship_imos()
        maindb_collection = self.get_main_data()
        # result = {}

        if dateString != "":
            findDate = datetime.strptime(dateString,"%Y-%m-%d, %H:%M:%S")
            # for imo in ship_imos:
            # #print(imo)
            complianceMessage = ""
            for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'processed_daily_data.rep_dt.processed': findDate}):
                if 'cp_message' in doc and type(doc['cp_message']) == str:
                    complianceMessage = doc['cp_message']
                    # tempList.append(doc['cp_message'])
                    # result[str(imo)] = tempList
                else:
                    complianceMessage = None
                    # tempList.append(None)
                    # result[str(imo)] = tempList
            return complianceMessage
        else:
            # for imo in ship_imos:
                # #print(imo)
            complianceMessage = ""
            for doc in maindb_collection.find({'ship_imo': self.ship_imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING).limit(1):
                if 'cp_message' in doc and type(doc['cp_message']) == str:
                    complianceMessage = doc['cp_message']
                    # tempList.append(doc['cp_message'])
                    # result[str(imo)] = tempList
                else:
                    complianceMessage = None
                    # tempList.append(None)
                    # result[str(imo)] = tempList
            return complianceMessage
        # #print("DICT OF ISSUES", result)
        # return result

    def create_cp_compliance_dict(self, id):
        ''' Returns if the ship is cp compliant.'''
        ship_collection = self.get_ship_configs()
        maindb_collection = self.get_main_data()
        charter_party_info = {}

        if id == "":
            for doc in ship_collection.find().sort('ship_imo', ASCENDING):
                tempList = []
                #print(doc['ship_imo'])
                for key in doc['data'].keys():
                    if doc['data'][key]['subcategory'] == 'Charter party':
                        if pd.isnull(doc['data'][key]['static_data']) == False:
                            tempList.append({'identifier': key, 'static_data': doc['data'][key]['static_data'], 'source_identifier': doc['data'][key]['source_idetifier']})
                            charter_party_info[doc['ship_imo']] = tempList
                        else:
                            continue
        else:
            for doc in ship_collection.find({'organization_id': id}).sort('ship_imo', ASCENDING):
                tempList = []
                #print(doc['ship_imo'])
                for key in doc['data'].keys():
                    if doc['data'][key]['subcategory'] == 'Charter party':
                        if pd.isnull(doc['data'][key]['static_data']) == False:
                            tempList.append({'identifier': key, 'static_data': doc['data'][key]['static_data'], 'source_identifier': doc['data'][key]['source_idetifier']})
                            charter_party_info[doc['ship_imo']] = tempList
                        else:
                            continue

        compliant=""
        for imo in charter_party_info.keys():
            for doc in maindb_collection.find({'ship_imo': imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING).limit(1):
                for key in doc['processed_daily_data'].keys():
                    for dicts in charter_party_info[imo]:
                        if key == dicts['source_identifier']:
                            try:
                                if doc['processed_daily_data'][key]['processed'] > dicts['static_data']:
                                    compliant = "No"
                                else:
                                    compliant = "Yes"
                            except TypeError:
                                compliant = None
                        dicts.update({'compliant': compliant})

        return charter_party_info

    ''' Functions for Inputs Processing'''
    def get_dict_of_raw_values(self, duration, header_dict):
        ''' Returns a dictionary of raw values from daily data. 
            Only values from a particular duration are selected.
            Key - identifier, Value - List of all values in that duration.
        '''
        if(duration == 'lastyear'):
            # dictionary_structure = self.create_dictionary_structure_for_raw_values()
            # dictionary_structure_keys = list(dictionary_structure.keys())
            # close_by_date = self.create_maindb_according_to_duration(duration=duration)
            daily_data_collection = self.get_daily_data()
            identifier_mapping = self.get_identifier_mapping()
            static_data = self.get_static_data()
            list_of_static_data = list(static_data.keys())
            list_of_new_id = list(identifier_mapping.keys())
            common_elements = [i for i in list_of_new_id if i in list_of_static_data]
            # #print("COMMON ELEMENT!!!!", common_elements)
            # #print("LIST OF NEW ID", list_of_new_id)

            for doc in daily_data_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt': 1}).sort('data.rep_dt', DESCENDING).limit(1):
                current_year = int(doc['final_rep_dt'].strftime('%Y'))
            
            last_year = current_year - 1
            last_year_start_date = datetime(last_year, 1, 1, 12)
            last_year_end_date = datetime(last_year, 12, 31, 12)

            result = {}
            for header in header_dict:
                tempList = []
                for doc in daily_data_collection.find({'ship_imo': self.ship_imo, 'final_rep_dt': {'$gte': last_year_start_date, '$lte': last_year_end_date}}):
                    temp = {}
                    # #print("CURRENT DATE", doc['data']['rep_dt'])
                    # if doc['data']['rep_dt'] != close_by_date:
                    for elem in common_elements:
                        # #print("STATIC DATA", elem)
                        source_id = identifier_mapping[elem]
                        if source_id in header_dict[header]:
                            # source_id = identifier_mapping[elem]
                            # #print("SOURCE ID!!!!!!!", source_id)
                            # #print("STATIC DATA!!!!!!", static_data[elem]['value'])
                            if pd.isnull(static_data[elem]['value']) == False:
                                if static_data[elem]['value'] == 0 or static_data[elem]['value'] == 0.0:
                                    temp[source_id] = str(static_data[elem]['value'])
                                else:
                                    temp[source_id] = static_data[elem]['value']
                            else:
                                temp[source_id] = ""
                    # #print("STATIC DATA TEMP DICT!!!!!!!", temp)

                    for key in doc['data'].keys():
                        source_id = identifier_mapping[key]
                        if source_id in header_dict[header]:
                            # if source_id in list_values or source_id.lower() in list_values:
                                # index_of_value = list_values.index(source_id) if source_id in list_values else list_values.index(source_id.lower())
                                # key_for_dict = list_keys[index_of_value]
                            # if key in list_of_new_id:
                                # if key in doc['data']:
                            # #print("KEY FOR DICT", source_id)
                            if pd.isnull(doc['data'][key]) == False:
                                if type(doc['data'][key]) == datetime:
                                    if key == 'rep_dt':
                                        newdate = doc['final_rep_dt'].strftime('%d-%m-%Y %H:%M:%S')
                                        temp[source_id] = newdate
                                    else:
                                # if key == 'rep_dt' or key == 'eta' or key == 'UTC_GMT' or key == 'stp_frm' or key == 'stp_to' or key == 'red_frm' or key == 'red_to':
                                        newdate = doc['data'][key].strftime('%d-%m-%Y %H:%M:%S')
                                        temp[source_id] = newdate
                                else:
                                    if doc['data'][key] == 0 or doc['data'][key] == 0.0:
                                        temp[source_id] = str(doc['data'][key])
                                    else:
                                        temp[source_id] = doc['data'][key]
                                # dictionary_structure[key].append(doc['data'][key])
                            else:
                                temp[source_id] = ""
                                    # dictionary_structure[key].append("")
                                # else:
                                #     #print("ELSE PART!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                                #     temp[source_id] = static_data[key]['value']
                            # else:
                            #     temp[source_id] = ""
                            # else:
                            #     temp[source_id] = ""
                    # #print("ENTIRE TEMP!!!!!!!", temp)
                    tempList.append(temp)
                result[header] = tempList
                    # else:
                    #     break
        else:
            # dictionary_structure = self.create_dictionary_structure_for_raw_values()
            # dictionary_structure_keys = list(dictionary_structure.keys())
            # close_by_date = self.create_maindb_according_to_duration(duration=duration)
            daily_data_collection = self.get_daily_data()
            identifier_mapping = self.get_identifier_mapping()
            static_data = self.get_static_data()
            list_of_static_data = list(static_data.keys())
            list_of_new_id = list(identifier_mapping.keys())
            common_elements = [i for i in list_of_new_id if i in list_of_static_data]
            # #print("COMMON ELEMENT!!!!", common_elements)
            # #print("LIST OF NEW ID", list_of_new_id)
            result = {}
            for header in header_dict:
                tempList = []
                for doc in daily_data_collection.find({'ship_imo': self.ship_imo}).sort('data.rep_dt', DESCENDING).limit(int(duration)):
                    temp = {}
                    # #print("CURRENT DATE", doc['data']['rep_dt'])
                    # if doc['data']['rep_dt'] != close_by_date:
                    for elem in common_elements:
                        # #print("STATIC DATA", elem)
                        source_id = identifier_mapping[elem]
                        if source_id in header_dict[header]:
                            # source_id = identifier_mapping[elem]
                            # #print("SOURCE ID!!!!!!!", source_id)
                            # #print("STATIC DATA!!!!!!", static_data[elem]['value'])
                            if pd.isnull(static_data[elem]['value']) == False:
                                if static_data[elem]['value'] == 0 or static_data[elem]['value'] == 0.0:
                                    temp[source_id] = str(static_data[elem]['value'])
                                else:
                                    temp[source_id] = static_data[elem]['value']
                            else:
                                temp[source_id] = ""
                    # #print("STATIC DATA TEMP DICT!!!!!!!", temp)

                    for key in doc['data'].keys():
                        source_id = identifier_mapping[key]
                        if source_id in header_dict[header]:
                            # if source_id in list_values or source_id.lower() in list_values:
                                # index_of_value = list_values.index(source_id) if source_id in list_values else list_values.index(source_id.lower())
                                # key_for_dict = list_keys[index_of_value]
                            # if key in list_of_new_id:
                                # if key in doc['data']:
                            # #print("KEY FOR DICT", source_id)
                            if pd.isnull(doc['data'][key]) == False:
                                if type(doc['data'][key]) == datetime:
                                    if key == 'rep_dt':
                                        newdate = doc['final_rep_dt'].strftime('%d-%m-%Y %H:%M:%S')
                                        temp[source_id] = newdate
                                    else:
                                # if key == 'rep_dt' or key == 'eta' or key == 'UTC_GMT' or key == 'stp_frm' or key == 'stp_to' or key == 'red_frm' or key == 'red_to':
                                        newdate = doc['data'][key].strftime('%d-%m-%Y %H:%M:%S')
                                        temp[source_id] = newdate
                                else:
                                    if doc['data'][key] == 0 or doc['data'][key] == 0.0:
                                        temp[source_id] = str(doc['data'][key])
                                    else:
                                        temp[source_id] = doc['data'][key]
                                # dictionary_structure[key].append(doc['data'][key])
                            else:
                                temp[source_id] = ""
                                    # dictionary_structure[key].append("")
                                # else:
                                #     #print("ELSE PART!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                                #     temp[source_id] = static_data[key]['value']
                            # else:
                            #     temp[source_id] = ""
                            # else:
                            #     temp[source_id] = ""
                    # #print("ENTIRE TEMP!!!!!!!", temp)
                    tempList.append(temp)
                result[header] = tempList
                    # else:
                    #     break
        #print("RESULT!!!!!!", result)
        # #print("LIST KEYS!!!!!!", list(result[0].keys()), list_keys)
        actual_result={}
        for sheet_name in result:
            tempList=[]
            for dicts in result[sheet_name]:
                temp_list=[]
                for key in header_dict[sheet_name]:
                    if key in dicts:
                        temp={}
                        temp['value'] = dicts[key]
                        temp_list.append(temp)
                    else:
                        temp={}
                        temp['value'] = ""
                        temp_list.append(temp)
                tempList.append(temp_list)
            actual_result[sheet_name] = tempList
            # tempList=[]
        # for sheet_name in result:
        #     temp_list=[]
        #     for sheet_number in header_dict.keys():
        #         if sheet_name == sheet_number:
        #             for header in header_dict[sheet_number]:
        #                 temp={}
        #                 if header == None:
        #                     temp['value'] = ""
        #                     temp_list.append(temp)
        #                 else:
        #                 # if header in dictionary:
        #                     temp['value'] = result[sheet_name][header] if header in result[sheet_name] and header in header_dict[sheet_number] else ""
        #                     temp_list.append(temp)
        #                 # else:
        #                 #     temp['value'] = ""
        #                 #     temp_list.append(temp)
        #             tempList.append(temp_list)
        #     actual_result[sheet_number] = tempList
        #print("ACTUAL RESULT!!!!!", actual_result)
        return actual_result
    
    def get_key(self, list_values, identifier_mapping):
        ''' Creates a list of identifiers whose raw values are to be collected.'''
        result = []

        for key, value in identifier_mapping.items():
            if value in list_values:
                result.append(key)
            else:
                result.append(key)
        
        return result
    
    ''' Functions for Reports Processing '''
    def get_parameters_for_reports_data(self):
        '''
            Gets the parameters for pdf reports table
        '''
        ship_config_collection = self.get_ship_configs()
        maindb_collection = self.get_main_data()
        daily_collection = self.get_daily_data()

        parameter_list=[]

        for doc in daily_collection.find({'ship_imo': int(self.ship_imo)}).sort('data.rep_dt', ASCENDING).limit(1):
            for key in doc['data'].keys():
                parameter_list.append(key)
        
        return parameter_list
    
    def get_average_rpm(self, voyage, loaded=False):
        '''
            Gets the average rpm of the ship during a specific voyage.
        '''
        sum_rpm_when_loaded = 0
        sum_rpm_when_ballast = 0
        average_rpm_when_loaded = 0
        average_rpm_when_ballast = 0
        count_loaded = 0
        count_ballast = 0

        maindb_collection = self.get_main_data()

        for doc in maindb_collection.find({'ship_imo': int(self.ship_imo)}, {'processed_daily_data.voyage': 1, 'processed_daily_data.rpm.processed': 1}).sort('final_rep_dt', ASCENDING):
            if doc['processed_daily_data']['voyage']['processed'] == voyage + ' L' and loaded == True:
                count_loaded = count_loaded + 1
                sum_rpm_when_loaded = sum_rpm_when_loaded + doc['processed_daily_data']['rpm']['processed']
            elif doc['processed_daily_data']['voyage']['processed'] == voyage + 'B' and loaded == False:
                count_ballast = count_ballast + 1
                sum_rpm_when_ballast = sum_rpm_when_ballast + doc['processed_daily_data']['rpm']['processed']
        
        if loaded == True:
            try:
                average_rpm_when_loaded = self.makeDecimal(sum_rpm_when_loaded / count_loaded)
                return average_rpm_when_loaded
            except ZeroDivisionError:
                average_rpm_when_loaded = "No Data"
                return average_rpm_when_loaded
        elif loaded == False:
            try:
                average_rpm_when_ballast = self.makeDecimal(sum_rpm_when_ballast / count_ballast)
                return average_rpm_when_ballast
            except ZeroDivisionError:
                average_rpm_when_ballast = "No Data"
                return average_rpm_when_ballast
        

        # return average_rpm_when_loaded, average_rpm_when_ballast
    
    def get_average_speed(self, voyage, loaded=False):
        '''
            Gets the average speed of the ship during a specific voyage.
        '''

        sum_speed_when_loaded = 0
        sum_speed_when_ballast = 0
        average_speed_when_loaded = 0
        average_speed_when_ballast = 0
        count_loaded = 0
        count_ballast = 0

        maindb_collection = self.get_main_data()

        for doc in maindb_collection.find({'ship_imo': int(self.ship_imo)}, {'processed_daily_data.voyage': 1, 'processed_daily_data.speed_stw.processed': 1,'processed_daily_data.speed_stw_calc.processed': 1}).sort('final_rep_dt', ASCENDING):
            if doc['processed_daily_data']['voyage']['processed'] == voyage + ' L' and loaded == True and pd.isnull(doc['processed_daily_data']['speed_stw']['processed'])==False:
                count_loaded = count_loaded + 1
                sum_speed_when_loaded = sum_speed_when_loaded + doc['processed_daily_data']['speed_stw']['processed']
            elif doc['processed_daily_data']['voyage']['processed'] == voyage + 'B' and loaded == False and pd.isnull(doc['processed_daily_data']['speed_stw']['processed'])==False:
                count_ballast = count_ballast + 1
                sum_speed_when_ballast = sum_speed_when_ballast + doc['processed_daily_data']['speed_stw']['processed']

            elif doc['processed_daily_data']['voyage']['processed'] == voyage + ' L' and loaded == True and pd.isnull(doc['processed_daily_data']['speed_stw_calc']['processed'])==False:
                count_loaded = count_loaded + 1
                sum_speed_when_loaded = sum_speed_when_loaded + doc['processed_daily_data']['speed_stw_calc']['processed']
            elif doc['processed_daily_data']['voyage']['processed'] == voyage + 'B' and loaded == False and pd.isnull(doc['processed_daily_data']['speed_stw_calc']['processed'])==False:
                count_ballast = count_ballast + 1
                sum_speed_when_ballast = sum_speed_when_ballast + doc['processed_daily_data']['speed_stw_calc']['processed']
        
        if loaded == True:
            try:
                average_speed_when_loaded = self.makeDecimal(sum_speed_when_loaded / count_loaded)
                return average_speed_when_loaded
            except ZeroDivisionError:
                average_speed_when_loaded = "No Data"
                return average_speed_when_loaded
        elif loaded == False:
            try:
                average_speed_when_ballast = self.makeDecimal(sum_speed_when_ballast / count_ballast)
                return average_speed_when_ballast
            except ZeroDivisionError:
                average_speed_when_ballast = "No Data"
                return average_speed_when_ballast
    
    def get_total_steaming_time(self, voyage, loaded=False):
        '''
            Gets the total steaming time of the ship during a specific voyage.
        '''

        sum_steaming_hours = 0

        maindb_collection = self.get_main_data()

        for doc in maindb_collection.find({'ship_imo': int(self.ship_imo)}, {'processed_daily_data.voyage': 1, 'processed_daily_data.stm_hrs.processed': 1}).sort('final_rep_dt', ASCENDING):
            if doc['processed_daily_data']['voyage']['processed'] == voyage + ' L' and loaded == True:
                sum_steaming_hours = sum_steaming_hours + doc['processed_daily_data']['stm_hrs']['processed']
            elif doc['processed_daily_data']['voyage']['processed'] == voyage + 'B' and loaded == False:
                sum_steaming_hours = sum_steaming_hours + doc['processed_daily_data']['stm_hrs']['processed']
        
        sum_steaming_hours = self.makeDecimal(sum_steaming_hours)
        return sum_steaming_hours
    
    def get_24_hr_corrected_FOC(self, voyage='', loaded=False):
        '''
            Use formula lsfo*24/stm_hrs to get 24 hr corrected fuel oil consumption.
        '''
        foc_list = []
        maindb_collection = self.get_main_data()

        if voyage != '':
            if voyage == '17':
                    voyage_string = voyage + 'L' if loaded == True else voyage + 'B'
            else:
                voyage_string = voyage + ' L' if loaded == True else voyage + 'B'

            for doc in maindb_collection.find(
                {'ship_imo': int(self.ship_imo)},
                {'processed_daily_data.voyage.processed': 1, 'processed_daily_data.main_fuel.processed': 1, 'processed_daily_data.stm_hrs.processed': 1}
            ).sort('final_rep_dt', ASCENDING):
                if doc['processed_daily_data']['voyage']['processed'] == voyage_string:
                    if pd.isnull(doc['processed_daily_data']['main_fuel']['processed']) == False and pd.isnull(doc['processed_daily_data']['stm_hrs']['processed']) == False:
                        foc = (doc['processed_daily_data']['main_fuel']['processed'] * 24) / doc['processed_daily_data']['stm_hrs']['processed']
                        foc_list.append(self.makeDecimal(foc))
                    else:
                        foc_list.append("")
                elif doc['processed_daily_data']['voyage']['processed'] == voyage_string:
                    if pd.isnull(doc['processed_daily_data']['main_fuel']['processed']) == False and pd.isnull(doc['processed_daily_data']['stm_hrs']['processed']) == False:
                        foc = (doc['processed_daily_data']['main_fuel']['processed'] * 24) / doc['processed_daily_data']['stm_hrs']['processed']
                        foc_list.append(self.makeDecimal(foc))
                    else:
                        foc_list.append("")
        else:
            for doc in maindb_collection.find(
                {'ship_imo': int(self.ship_imo)},
                {'processed_daily_data.main_fuel.processed': 1, 'processed_daily_data.stm_hrs.processed': 1,
                 'processed_daily_data.voyage.processed': 1}
            ).sort('final_rep_dt', DESCENDING):
                if 'L' in doc['processed_daily_data']['voyage']['processed'] and loaded == True:
                    if pd.isnull(doc['processed_daily_data']['main_fuel']['processed']) == False and pd.isnull(doc['processed_daily_data']['stm_hrs']['processed']) == False:
                        foc = (doc['processed_daily_data']['main_fuel']['processed'] * 24) / doc['processed_daily_data']['stm_hrs']['processed']
                        foc_list.append(self.makeDecimal(foc))
                    else:
                        foc_list.append("")
                elif 'B' in doc['processed_daily_data']['voyage']['processed'] and loaded == False:
                    if pd.isnull(doc['processed_daily_data']['main_fuel']['processed']) == False and pd.isnull(doc['processed_daily_data']['stm_hrs']['processed']) == False:
                        foc = (doc['processed_daily_data']['main_fuel']['processed'] * 24) / doc['processed_daily_data']['stm_hrs']['processed']
                        foc_list.append(self.makeDecimal(foc))
                    else:
                        foc_list.append("")
        
        return foc_list
    
    def get_baseline_pred(self, voyage='', loaded=False):
        '''
            Get the baseline pred for the specific voyage when `voyage!=''`
            else get all the baseline pred values.
        '''
        baseline = []
        maindb_collection = self.get_main_data()
        #print("BASELINE START")
        
        if voyage != '':
            if voyage == '17':
                voyage_string = voyage + 'L' if loaded == True else voyage + 'B'
            else:
                voyage_string = voyage + ' L' if loaded == True else voyage + 'B'

            for doc in maindb_collection.find({'ship_imo': int(self.ship_imo)}, {'processed_daily_data.voyage.processed': 1, 'baseline_pred': 1}).sort('final_rep_dt', ASCENDING):
                if doc['processed_daily_data']['voyage']['processed'] == voyage_string and loaded == True:
                    if pd.isnull(doc['baseline_pred']['main_fuel']) == False:
                        baseline.append(self.makeDecimal(doc['baseline_pred']['main_fuel']))
                    else:
                        baseline.append("")
                elif doc['processed_daily_data']['voyage']['processed'] == voyage_string and loaded == False:
                    if pd.isnull(doc['baseline_pred']['main_fuel']) == False:
                        baseline.append(self.makeDecimal(doc['baseline_pred']['main_fuel']))
                    else:
                        baseline.append("")
        else:
            for doc in maindb_collection.find({'ship_imo': int(self.ship_imo)}, {'processed_daily_data.voyage.processed': 1, 'baseline_pred': 1}).sort('final_rep_dt', ASCENDING):
                if 'L' in doc['processed_daily_data']['voyage']['processed'] and loaded == True:
                    if pd.isnull(doc['baseline_pred']['main_fuel']) == False:
                        baseline.append(self.makeDecimal(doc['baseline_pred']['main_fuel']))
                    else:
                        baseline.append("")
                elif 'B' in doc['processed_daily_data']['voyage']['processed'] and loaded == False:
                    if pd.isnull(doc['baseline_pred']['main_fuel']) == False:
                        baseline.append(self.makeDecimal(doc['baseline_pred']['main_fuel']))
                    else:
                        baseline.append("")
        #print("BASELINE END")
        
        return baseline
    
    def get_percentage_difference(self, voyage, loaded=False):
        '''
            Gets the percentage difference between 24 hr corrected foc and baseline pred.
            Uses the formula - ((24hr - baseline) * 100) / 24hr
        '''
        foc_list = self.get_24_hr_corrected_FOC(voyage, loaded)
        baseline_list = self.get_baseline_pred(voyage, loaded)
        difference_list = []

        for i in range(len(foc_list)):
            if foc_list[i] != "" and baseline_list[i] != "":
                num = ((foc_list[i] - baseline_list[i]) * 100) / foc_list[i]
                difference_list.append(self.makeDecimal(num))
            else:
                difference_list.append("")
        
        return difference_list


    
    def get_vessel_details(self, voyage, loaded=False):
        '''
            Gets the vessel details that are required in reports.
        '''

        static_data = self.get_static_data()
        total_steaming_time = self.get_total_steaming_time(voyage=voyage, loaded=loaded)
        avg_speed = self.get_average_speed(voyage=voyage, loaded=loaded)
        avg_rpm = self.get_average_rpm(voyage=voyage, loaded=loaded)
        parameter_list_for_vessel_details = ['ship_name', 'ship_imo', 'ship_type', 'voyage', None, None, None,
            'total_steaming_time', 'avg_speed', 'avg_rpm'
        ]
        keys_for_vessel_details = ['Name of Vessel', 'IMO Number', 'Type of Vessel', 'Voyage',
            'Departure', 'Arrival', 'Total Distance (nautical miles)', 'Total Steaming Time (h)', 'Average Speed (m/s)',
            'Average RPM (rpm)'
        ]

        result = {}

        for param in range(len(parameter_list_for_vessel_details)):
            if parameter_list_for_vessel_details[param] != None:
                if parameter_list_for_vessel_details[param] == 'voyage':
                    result['Voyage'] = voyage + ' L' if loaded == True else voyage + ' B'
                elif parameter_list_for_vessel_details[param] == 'total_steaming_time':
                    result['Total Steaming Time (h)'] = total_steaming_time
                elif parameter_list_for_vessel_details[param] == 'avg_speed':
                    result['Average Speed (m/s)'] = avg_speed
                elif parameter_list_for_vessel_details[param] == 'avg_rpm':
                    result['Average RPM (rpm)'] = avg_rpm
                else:
                    result[keys_for_vessel_details[param]] = static_data[parameter_list_for_vessel_details[param]]['value']
        

        return result
    
    def get_performance_data(self, voyage, loaded=False):
        '''
            Gets the performance-related ship data for a specific voyage.
            {'$in': [voyage_string]}
        '''
        performance_data_list = []
        maindb_collection = self.get_main_data()
        if voyage == '17':
            voyage_string = voyage + 'L' if loaded == True else voyage + 'B'
        else:
            voyage_string = voyage + ' L' if loaded == True else voyage + 'B'
        #print("PERFORMANCE START")
        for doc in maindb_collection.find(
            {'ship_imo': int(self.ship_imo), 'processed_daily_data.voyage.processed': voyage_string},
            {'processed_daily_data': 1, 'final_rep_dt': 1}
        ).sort('final_rep_dt', ASCENDING):
            tempDict = {}
            for key, value in performance_analysis_identifier_mapping.items():
                #print("VALUES", value)
                if doc['processed_daily_data']['voyage']['processed'] == voyage_string:
                    if value != 'final_rep_dt' and value != "":
                        if value in doc['processed_daily_data'].keys() and pd.isnull(doc['processed_daily_data'][value]['processed']) == False:
                            tempDict[key] = self.makeDecimal(doc['processed_daily_data'][value]['processed'])
                        else:
                            tempDict[key] = "No Data"
                    elif value != "final_rep_dt" and value == "":
                        tempDict[key] = "No Data"
                    else:
                        tempDict[key] = doc['final_rep_dt'].strftime('%Y-%m-%d')
                # elif doc['processed_daily_data']['voyage']['processed'] == voyage + ' B' and loaded == False:
                #     if value != 'final_rep_dt':
                #         if value in doc['processed_daily_data'].keys() and pd.isnull(doc['processed_daily_data'][value]['processed']) == False:
                #             tempDict[key] = self.makeDecimal(doc['processed_daily_data'][value]['processed'])
                #         else:
                #             tempDict[key] = ""
                #     else:
                #         tempDict[key] = doc['final_rep_dt'].strftime('%Y-%m-%d')
            if len(list(tempDict.keys())) > 0:
                performance_data_list.append(tempDict)
        #print("PERFORMANCE END")

        return performance_data_list
    
    def get_weather_data(self, voyage, loaded=False):
        '''
            Gets the weather-related ship data for a specific voyage.
            {'$in': [voyage_string]}
        '''
        weather_data_list = []
        maindb_collection = self.get_main_data()
        if voyage == '17':
            voyage_string = voyage + 'L' if loaded == True else voyage + 'B'
        else:
            voyage_string = voyage + ' L' if loaded == True else voyage + 'B'
        #print("WEATHER START")
        for doc in maindb_collection.find(
            {'ship_imo': int(self.ship_imo), 'processed_daily_data.voyage.processed': voyage_string},
            {'processed_daily_data': 1, 'final_rep_dt': 1}
        ).sort('final_rep_dt', ASCENDING):
            tempDict = {}
            for key, value in weather_data_identifier_mapping.items():
                #print("VALUES", value)
                if doc['processed_daily_data']['voyage']['processed'] == voyage_string:
                    if value != 'final_rep_dt' and value != "":
                        if value in doc['processed_daily_data'].keys() and pd.isnull(doc['processed_daily_data'][value]['processed']) == False and type(doc['processed_daily_data'][value]['processed'])!=str:
                            tempDict[key] = self.makeDecimal(doc['processed_daily_data'][value]['processed'])
                        else:
                            tempDict[key] = "No Data"
                    elif value != "final_rep_dt" and value == "":
                        tempDict[key] = "No Data"
                    else:
                        tempDict[key] = doc['final_rep_dt'].strftime('%Y-%m-%d')
                # elif doc['processed_daily_data']['voyage']['processed'] == voyage + ' B' and loaded == False:
                #     if value != 'final_rep_dt':
                #         if value in doc['processed_daily_data'].keys() and pd.isnull(doc['processed_daily_data'][value]['processed']) == False:
                #             tempDict[key] = self.makeDecimal(doc['processed_daily_data'][value]['processed'])
                #         else:
                #             tempDict[key] = ""
                #     else:
                #         tempDict[key] = doc['final_rep_dt'].strftime('%Y-%m-%d')
            if len(list(tempDict.keys())) > 0:
                weather_data_list.append(tempDict)
        #print("WEATHER END")
        return weather_data_list
    
    def get_fuel_cons_data(self, voyage='', loaded=False):
        '''
            Gets the Fuel Oil Consumption Table related ship data for a specific voyage.
            When `voyage == ''` it will get the last seven days data for the performance
            page tables.
            {'$in': [voyage_string]}
        '''
        fuel_cons_list = []
        maindb_collection = self.get_main_data()
        if voyage != '':
            fuel_cons_24_hr = self.get_24_hr_corrected_FOC(voyage, loaded)
            baseline_pred = self.get_baseline_pred(voyage, loaded)
            percentage_difference = self.get_percentage_difference(voyage, loaded)
            if voyage == '17':
                voyage_string = voyage + 'L' if loaded == True else voyage + 'B'
            else:
                voyage_string = voyage + ' L' if loaded == True else voyage + 'B'
            #print("FUEL CONS START")
            for doc in maindb_collection.find(
                {'ship_imo': int(self.ship_imo), 'processed_daily_data.voyage.processed': voyage_string},
                {'processed_daily_data': 1, 'final_rep_dt': 1}
            ).sort('final_rep_dt', ASCENDING):
                tempDict = {}
                for key, value in FOC_identifier_mapping.items():
                    #print("VALUES", value)
                    if doc['processed_daily_data']['voyage']['processed'] == voyage_string:
                        if value != 'final_rep_dt' and value != "":
                            if value == 'main_fuel':
                                tempDict[key] = fuel_cons_24_hr
                            elif value == '' and key == 'Weather corrected Baseline (t)':
                                tempDict[key] = baseline_pred
                            elif value == '' and key == 'Difference (%)':
                                tempDict[key] = percentage_difference
                            elif value in doc['processed_daily_data'].keys() and value != 'main_fuel' and pd.isnull(doc['processed_daily_data'][value]['processed']) == False:
                                tempDict[key] = self.makeDecimal(doc['processed_daily_data'][value]['processed'])
                            else:
                                tempDict[key] = "No Data"
                        elif value != "final_rep_dt" and value == "":
                            tempDict[key] = "No Data"
                        else:
                            tempDict[key] = doc['final_rep_dt'].strftime('%Y-%m-%d')
                    # elif doc['processed_daily_data']['voyage']['processed'] == voyage + ' B' and loaded == False:
                    #     if value != 'final_rep_dt':
                    #         if value in doc['processed_daily_data'].keys() and pd.isnull(doc['processed_daily_data'][value]['processed']) == False:
                    #             tempDict[key] = self.makeDecimal(doc['processed_daily_data'][value]['processed'])
                    #         else:
                    #             tempDict[key] = ""
                    #     else:
                    #         tempDict[key] = doc['final_rep_dt'].strftime('%Y-%m-%d')
                if len(list(tempDict.keys())) > 0:
                    fuel_cons_list.append(tempDict)
                for dicts in range(len(fuel_cons_list)):
                    fuel_cons_list[dicts]['24 h Corrected F.O.C (t)'] = fuel_cons_24_hr[dicts]
                    fuel_cons_list[dicts]['Weather corrected Baseline (t)'] = baseline_pred[dicts]
                    fuel_cons_list[dicts]['Difference (%)'] = percentage_difference[dicts]
                    fuel_cons_list[dicts]['Torque Rich Index'] = 0.99
            #print("FUEL CONS END")
        else:
            fuel_cons_24_hr = self.get_24_hr_corrected_FOC(voyage, loaded)
            baseline_pred = self.get_baseline_pred(voyage, loaded)
            # baseline_pred = baseline_pred[len(baseline_pred)-7:len(baseline_pred)]
            # baseline_pred.reverse()
            percentage_difference = self.get_percentage_difference(voyage, loaded)
            load_string = 'L' if loaded == True else 'B'
            for doc in maindb_collection.find(
                {'ship_imo': int(self.ship_imo)},
                {'processed_daily_data': 1, 'final_rep_dt': 1}
            ).sort('final_rep_dt', DESCENDING):
                tempDict = {}
                for key, value in FOC_identifier_mapping.items():
                    #print("VALUES", value)
                    if load_string in doc['processed_daily_data']['voyage']['processed'] and value != 'trim':
                        if value != 'final_rep_dt' and value != "":
                            if value == 'main_fuel':
                                tempDict[key] = fuel_cons_24_hr
                            elif value == '' and key == 'Weather corrected Baseline (t)':
                                tempDict[key] = baseline_pred
                            elif value == '' and key == 'Difference (%)':
                                tempDict[key] = percentage_difference
                            elif value in doc['processed_daily_data'].keys() and value != 'main_fuel' and pd.isnull(doc['processed_daily_data'][value]['processed']) == False:
                                tempDict[key] = self.makeDecimal(doc['processed_daily_data'][value]['processed'])
                            else:
                                tempDict[key] = "No Data"
                        elif value != "final_rep_dt" and value == "":
                            tempDict[key] = "No Data"
                        else:
                            tempDict[key] = doc['final_rep_dt'].strftime('%Y-%m-%d')
                    # elif doc['processed_daily_data']['voyage']['processed'] == voyage + ' B' and loaded == False:
                    #     if value != 'final_rep_dt':
                    #         if value in doc['processed_daily_data'].keys() and pd.isnull(doc['processed_daily_data'][value]['processed']) == False:
                    #             tempDict[key] = self.makeDecimal(doc['processed_daily_data'][value]['processed'])
                    #         else:
                    #             tempDict[key] = ""
                    #     else:
                    #         tempDict[key] = doc['final_rep_dt'].strftime('%Y-%m-%d')
                if len(list(tempDict.keys())) > 0:
                    fuel_cons_list.append(tempDict)
                for dicts in range(len(fuel_cons_list)):
                    fuel_cons_list[dicts]['24 h Corrected F.O.C (t)'] = fuel_cons_24_hr[dicts]
                    fuel_cons_list[dicts]['Weather corrected Baseline (t)'] = baseline_pred[dicts]
                    fuel_cons_list[dicts]['Difference (%)'] = percentage_difference[dicts]
                    fuel_cons_list[dicts]['Torque Rich Index'] = 0.99
        return fuel_cons_list
    
    def create_voyage_number_list(self):
        '''
            Creates a list of the voyage numbers.
        '''
        voyage_list = []
        maindb_collection = self.get_main_data()

        for doc in maindb_collection.find({'ship_imo': int(self.ship_imo)}, {'processed_daily_data.voyage.processed': 1}).sort('final_rep_dt', ASCENDING):
            if pd.isnull(doc['processed_daily_data']['voyage']['processed']) == False:
                voyage_splice = doc['processed_daily_data']['voyage']['processed'][0:2]
                if voyage_splice not in voyage_list:
                    voyage_list.append(voyage_splice)

        return voyage_list
    
    def create_voyage_options_list(self):
        '''
            Creates the options list of the React-select dropdown for different voyages.
            Will be a list of dictionaries. Dictionary have for keys - `Voyage No. number, fromDate-toDate`,
            and for values - `voyage_number`.
        '''
        voyage_options = []
        maindb_collection = self.get_main_data()
        voyage_numbers = self.create_voyage_number_list()

        for num in voyage_numbers:
            startDate = None
            endDate = None
            for doc in maindb_collection.find({'ship_imo': int(self.ship_imo)}, {'processed_daily_data.voyage.processed': 1, 'final_rep_dt': 1}).sort('final_rep_dt', ASCENDING):
                if pd.isnull(doc['processed_daily_data']['voyage']['processed'])==False and num in doc['processed_daily_data']['voyage']['processed']:
                    startDate = doc['final_rep_dt'].strftime('%d/%m/%Y')
                    break
            for doc in maindb_collection.find({'ship_imo': int(self.ship_imo)}, {'processed_daily_data.voyage.processed': 1, 'final_rep_dt': 1}).sort('final_rep_dt', DESCENDING):
                if pd.isnull(doc['processed_daily_data']['voyage']['processed'])==False and num in doc['processed_daily_data']['voyage']['processed']:
                    endDate = doc['final_rep_dt'].strftime('%d/%m/%Y')
                    break
            tempdict = {'label': "Voyage No. "+num+', '+startDate+'-'+endDate, 'value': num}
            voyage_options.append(tempdict)

        return voyage_options
    
    def get_all_fuel_cons_data(self, voyage='', loaded=False):
        '''
            Gets all the Fuel Oil Consumption data.
        '''
        fuel_cons_list = []
        maindb_collection = self.get_main_data()

        if voyage != '':
            if voyage == '17':
                voyage_string = voyage + 'L' if loaded == True else voyage + 'B'
            else:
                voyage_string = voyage + ' L' if loaded == True else voyage + 'B'
            #print("ALL FUEL CONS START")
            for doc in maindb_collection.find(
                {'ship_imo': int(self.ship_imo), 'processed_daily_data.voyage.processed': voyage_string},
                {'processed_daily_data.main_fuel.processed': 1, 'final_rep_dt': 1}
            ).sort('final_rep_dt', ASCENDING):
                if pd.isnull(doc['processed_daily_data']['main_fuel']['processed']) == False and doc['processed_daily_data']['main_fuel']['processed']!=0:
                    fuel_cons_list.append(self.makeDecimal(doc['processed_daily_data']['main_fuel']['processed']))
                else:
                    fuel_cons_list.append(None)
            #print("ALL FUEL CONS END")
        else:
            for doc in maindb_collection.find(
                {'ship_imo': int(self.ship_imo)},
                {'processed_daily_data.main_fuel.processed': 1, 'final_rep_dt': 1, 'processed_daily_data.voyage.processed': 1}
            ).sort('final_rep_dt', ASCENDING):
                if 'L' in doc['processed_daily_data']['voyage']['processed'] and loaded == True:
                    if pd.isnull(doc['processed_daily_data']['main_fuel']['processed']) == False and doc['processed_daily_data']['main_fuel']['processed']!=0:
                        fuel_cons_list.append(self.makeDecimal(doc['processed_daily_data']['main_fuel']['processed']))
                    else:
                        fuel_cons_list.append(None)
                elif 'B' in doc['processed_daily_data']['voyage']['processed'] and loaded == False:
                    if pd.isnull(doc['processed_daily_data']['main_fuel']['processed']) == False and doc['processed_daily_data']['main_fuel']['processed']!=0:
                        fuel_cons_list.append(self.makeDecimal(doc['processed_daily_data']['main_fuel']['processed']))
                    else:
                        fuel_cons_list.append(None)
        return fuel_cons_list
    
    def get_speed_data(self, voyage='', loaded=False):
        '''
            Gets all the speed data.
        '''

        speed_list = []
        maindb_collection = self.get_main_data()

        if voyage != '':
            if voyage == '17':
                voyage_string = voyage + 'L' if loaded == True else voyage + 'B'
            else:
                voyage_string = voyage + ' L' if loaded == True else voyage + 'B'
            #print("SPEED START")
            for doc in maindb_collection.find(
                {'ship_imo': int(self.ship_imo), 'processed_daily_data.voyage.processed': voyage_string},
                {
                    # 'processed_daily_data.speed_stw_calc.processed': 1,
                    'final_rep_dt': 1,
                    'processed_daily_data.speed_stw.processed': 1,
                    'processed_daily_data.speed_stw_calc.processed': 1
                }
            ).sort('final_rep_dt', ASCENDING):
                if pd.isnull(doc['processed_daily_data']['speed_stw']['processed']) == False:
                    speed_list.append(self.makeDecimal(doc['processed_daily_data']['speed_stw']['processed']))
                elif pd.isnull(doc['processed_daily_data']['speed_stw_calc']['processed']) == False:
                    speed_list.append(self.makeDecimal(doc['processed_daily_data']['speed_stw_calc']['processed']))
                else:
                    speed_list.append(None)
            #print("SPEED END")
        else:
            for doc in maindb_collection.find(
                {'ship_imo': int(self.ship_imo)},
                {
                    # 'processed_daily_data.speed_stw_calc.processed': 1,
                    'final_rep_dt': 1,
                    'processed_daily_data.speed_stw.processed': 1,
                    'processed_daily_data.voyage.processed': 1,
                    'processed_daily_data.speed_stw_calc.processed': 1
                }
            ).sort('final_rep_dt', ASCENDING):
                if 'L' in doc['processed_daily_data']['voyage']['processed'] and loaded == True:
                    if pd.isnull(doc['processed_daily_data']['speed_stw']['processed']) == False:
                        speed_list.append(self.makeDecimal(doc['processed_daily_data']['speed_stw']['processed']))
                    elif pd.isnull(doc['processed_daily_data']['speed_stw_calc']['processed']) == False:
                        speed_list.append(self.makeDecimal(doc['processed_daily_data']['speed_stw_calc']['processed']))
                    else:
                        speed_list.append(None)
                elif 'B' in doc['processed_daily_data']['voyage']['processed'] and loaded == False:
                    if pd.isnull(doc['processed_daily_data']['speed_stw']['processed']) == False:
                        speed_list.append(self.makeDecimal(doc['processed_daily_data']['speed_stw']['processed']))
                    elif pd.isnull(doc['processed_daily_data']['speed_stw_calc']['processed']) == False:
                        speed_list.append(self.makeDecimal(doc['processed_daily_data']['speed_stw_calc']['processed']))
                    else:
                        speed_list.append(None)
        return speed_list

    def get_percentage_difference_for_all_values(self, voyage, loaded=False):
        '''
            Gets the percentage difference between 24 hr corrected foc and baseline pred.
            Uses the formula - ((24hr - baseline) * 100) / 24hr
        '''
        foc_list = self.get_all_fuel_cons_data(voyage, loaded)
        baseline_list = self.get_baseline_pred(voyage, loaded)
        difference_list = []
        #print("PERCENTAGE START")
        for i in range(len(foc_list)):
            if foc_list[i] != None and baseline_list[i] != None:
                num = ((foc_list[i] - baseline_list[i]) * 100) / foc_list[i]
                difference_list.append(self.makeDecimal(num))
            else:
                difference_list.append("")
        #print("PERCENTAGE END")
        return difference_list

    def get_date_list(self, voyage='', loaded=False):
        '''
            Gets the list of all dates.
        '''
        date_list = []
        maindb_collection = self.get_main_data()

        if voyage != '':
            if voyage == '17':
                voyage_string = voyage + 'L' if loaded == True else voyage + 'B'
            else:
                voyage_string = voyage + ' L' if loaded == True else voyage + 'B'
            #print("DATE START")
            for doc in maindb_collection.find({'ship_imo': int(self.ship_imo), 'processed_daily_data.voyage.processed': voyage_string},
                                            {'final_rep_dt': 1}).sort('final_rep_dt', ASCENDING):
                tempDate = doc['final_rep_dt'].strftime('%d-%m-%Y')
                date_list.append(tempDate)
            #print("DATE END")
        else:
            for doc in maindb_collection.find({'ship_imo': int(self.ship_imo)},
                                            {'final_rep_dt': 1, 'processed_daily_data.voyage.processed': 1}).sort('final_rep_dt', ASCENDING):
                if 'L' in doc['processed_daily_data']['voyage']['processed'] and loaded == True:
                    tempDate = doc['final_rep_dt'].strftime('%d-%m-%Y')
                    date_list.append(tempDate)
                elif 'B' in doc['processed_daily_data']['voyage']['processed'] and loaded == False:
                    tempDate = doc['final_rep_dt'].strftime('%d-%m-%Y')
                    date_list.append(tempDate)
        return date_list









        
# obj=Configurator(6185798)
# obj.get_ship_configs()
# res = obj.get_fuel_cons_data('16', loaded=True)
# #print(res)
# res = obj.text_wrapping('FO Serv Tank #1 Temp')
# #print(res)
# obj.get_loaded_and_ballast_data()
# obj.get_main_data()
# obj.get_ship_stats('rpm', 'w_force', 'sea_st', 'draft_mean', 'trim')
# obj.create_maindb_according_to_duration('30Days')
# obj.get_nearest_date('')
# df, x_name, y_name, z_name, x_list, y_list, z_list = obj.create_dataframe('draft_mean','real_slip', '30Days', 'pwr', rpm=50,beaufort=1,sea_state=1,draft=5,trim=-2)
# obj.regress_for_constant_x(df, 'draft_mean', 'real_slip', 'pwr', rpm=50,beaufort=1,sea_state=1,draft=5,trim=-2)
# lists = obj.static_lists()
# #print(lists)
# obj.engine_var_list()
# obj.nav_var_list()
# obj.ais_api_var_list()
# obj.calculated_var_list()
# obj.get_group_selection("COMBUSTION PROCESS")
# b=obj.base_formula("ship_lenlbp*ship_beam*ship_maxsummerdft")
# #print(b)


