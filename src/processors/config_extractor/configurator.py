from math import isfinite, isnan
import sys
import os
from dotenv import load_dotenv
import re
from numpy.core.numeric import NaN
import pandas as pd
sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
from pymongo import MongoClient
from bson.json_util import dumps, loads
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from src.processors.config_extractor.prediction_interval import LRPI
from pymongo import ASCENDING, DESCENDING
import math

load_dotenv()

# client = MongoClient("mongodb://localhost:27017/")
# db=client.get_database("aranti")
client = MongoClient(os.getenv('MONGODB_ATLAS'))
# client = MongoClient("mongodb://iamuser:iamuser@democluster.lw5i0.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db=client.get_database("aranti")
database = db

# Constants
height_per_block = 300
height_per_block_after_double_click = height_per_block * 2.5
default_height = 650
default_Y_position = -0.05
common_header = ['Name', "Unit", 'Reported', 'Expected', 'Statement', 'Cause', 'Probability']
daily_report_column_headers = {
    'VESSEL PARTICULARS': ["Name", "Constant"],
    'VESSEL STATUS': ["Name", "Unit", "Reported", "Expected", "API_DATA", "Statement"],
    'CHANGE IN SPEED': ["Name", "Unit", "Reported", "Expected", "Statement"],
    'WEATHER PARAMETERS': ["Name", "Unit", "Reported", "API_DATA"],
    'DISTANCE AND TIME': ["Name", "Unit", "Reported", "Expected", "API_DATA"],
    'VESSEL POSITION': ["Name", "Reported", "Expected"],
    'FUEL OIL CONSUMPTION': common_header,
    'MAIN ENGINE': common_header,
    'GENERATOR': common_header,
    'AUXILLIARIES': common_header,
    'INDICES': common_header
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
    '30Days': 90,
    '90Days': 180,
    '1Year': 52
}

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

    def static_lists(self):
        self.stat_var_list=[]
        for i,j in self.ship_configs['static_data'].items():
            self.stat_var_list.append(str(i))
        return self.stat_var_list

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
        if duration == '30Days':
            durationActual = 'm3'
        elif duration == '90Days':
            durationActual = 'm6'
        elif duration == '1Year':
            durationActual = 'm12'
        elif duration == 'Lastyear30':
            durationActual = 'ly_m3'
        elif duration == 'Lastyear90':
            durationActual = 'ly_m6'
        else:
            durationActual = 'ly_m12'
        return durationActual
    
    def get_dict_for_ships(self):
        ''' Returns list of dictionaries with the ship imo as key and the ship name as values'''
        ship_collection = self.get_ship_configs()
        result=[]
        for doc in ship_collection.find({}).sort('ship_imo', ASCENDING):
            ship_imo = doc['ship_imo']
            ship_name = doc['static_data']['ship_name']['value'].strip()
            temp = {'value': ship_imo, 'label': ship_name}
            result.append(temp)
        
        return result
    
    def get_dependent_parameters(self):
        ''' Returns dictionary containing list of dependent parameters according to ship_imo.'''
        result = {}
        var_list = []
        var_list2 = []
        # ship_collection = self.get_ship_configs()
        maindb_collection = self.get_main_data()
        # for doc in ship_collection.find({}).sort('ship_imo', ASCENDING):
        #     singledata = doc['data']
        #     for var in singledata.keys():
        #         # print(doc)
        #         if singledata[var]['dependent'] == True:
        #             var_list.append(var)
        #         else:
        #             continue
        #     result[int(doc['ship_imo'])] = var_list
        
        # for var in result[self.ship_imo]:
        for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'processed_daily_data': 1, '_id': 0}):
            for var in doc['processed_daily_data'].keys():
                if 'SPEy' in doc['processed_daily_data'][var] and 'Q_y' in doc['processed_daily_data'][var]:
                    if doc['processed_daily_data'][var]['SPEy'] != None and doc['processed_daily_data'][var]['Q_y'] != None:
                        if var not in var_list2:
                            var_list2.append(var)
            break
        
        return var_list2
    
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
    
    def get_sister_vessel(self):
        ''' Returns the list of all the sister vessels of a particular ship.'''
        ship_collection = self.get_ship_configs()
        # result = []

        for doc in ship_collection.find({'ship_imo': self.ship_imo}).sort('ship_imo', ASCENDING):
            result = doc['sister_vessel_list']
        return result


    ''' Functions for Trends processing '''
    def get_group_selection(self,groupname):
        '''For getting the groups list for particular group'''
        result = []
        singledata = self.ship_configs['data']
        for var in singledata.keys():            
            groups = singledata[var]['group_selection']
            for group in groups:
                if group['groupname'] == groupname:
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
        # newgroup=[]
        # datadictList = list(datadict.keys())
        # spedictList = list(spedict.keys())
        # if spe == False:
        # for i in range(len(oldgroup)):
        #     # for j in datadict.keys():
        #     if oldgroup[i]['name'] in datadictList or oldgroup[i]['name'] in spedictList:
        #         # if j == oldgroup[i]['name']:
        #         newgroup.append(oldgroup[i])
        # print("NEW GROUP BEFORE !!!!!", newgroup)
        block = self.get_number_of_blocks('', oldgroup)
        print("BLOCK WITH NEWGROUP", block)
        # for i in range(len(newgroup)):
        #     block_number = newgroup[i]['block_number']
        #     print(block_number)
        #     for j in range(len(block)):
        #         print(j+1)
        #         if newgroup[i]['block_number'] == block[j]:
        #             newgroup[i]['block_number'] = j+1
        for i in range(len(block)):#1, 2, 9, 10, 11, 12, 13
            for j in range(len(oldgroup)):
                if oldgroup[j]['block_number'] == block[i]:
                    oldgroup[j]['block_number'] = i + 1
                # if newgroup[j]['block_number'] == 1:
                #     newgroup[j]['block_number'] = 1
                # else:
                #     if newgroup[j]['block_number'] == block[i]:
                #         if newgroup[j]['block_number'] == i:
                #             newgroup[i]['block_number'] = i
                #         if newgroup[j]['block_number'] < i:
                #             continue
                #         if newgroup[j]['block_number'] > i:
                #             newgroup[j]['block_number'] = i+1
        # block = self.get_number_of_blocks('', newgroup)
        # sorted_group = []
        # for i in block:
        #     for j in range(len(newgroup)):
        #         if newgroup[j]['block_number'] == i:
        #             sorted_group.append(newgroup[j])
        # if spe == True:
        #     datadictList = list(datadict.keys())
        #     # for i in range(len(oldgroup)):
        #     #     for j in datadict.keys():
        #     #         if 'spe_' in oldgroup[i]['name'] and j == oldgroup[i]['name']:
        #     #             newgroup.append(oldgroup[i])
        #     for i in range(len(oldgroup)):
        #         if oldgroup[i]['name'] in datadictList:
        #             newgroup.append(oldgroup[i])
        #     for i in range(len(oldgroup)):
        #         if 'spe_' not in oldgroup[i]['name']:
        #             newgroup.append(oldgroup[i])
        #     print("NEW GROUP BEFORE !!!!!", newgroup)
        #     block = self.get_number_of_blocks('', newgroup)
        #     print("BLOCK WITH NEWGROUP", block)
        #     for i in range(len(newgroup)):
        #         block_number = newgroup[i]['block_number']
        #         print(block_number)
        #         for j in range(len(block)):
        #             print(j+1)
        #             if newgroup[i]['block_number'] == block[j]:
        #                 newgroup[i]['block_number'] = j+1
        return oldgroup
    
    def get_group_selection_including_spe(self, group, datadict, spedict):
        ''' Returns the groups list by including the spe for dependent parameters, and correcting the block numbers.'''
        print("START DEPENDENT PARAMS")
        # spe_list = self.get_dependent_parameters()
        print("END DEPENDENT PARAMS")
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
        print("GROUP OF PARAMS WITH VALUES", group_of_only_params_with_values)
        block = self.get_number_of_blocks('', group_of_only_params_with_values)
        print("START SORT GROUP")
        for i in block:
            for j in range(len(group_of_only_params_with_values)):
                if group_of_only_params_with_values[j]['block_number'] == i:
                    sorted_group.append(group_of_only_params_with_values[j])
        print("END SORT GROUP")
        print("START SPE INSIDE")
        for i in range(len(sorted_group)):
            tempName = 'spe_' + sorted_group[i]['name']
            
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
                    try:
                        if newgroup[-1]['name'] in spekeylist:
                            if name_and_block_dict[newgroup[-2]['name']] == name_and_block_dict[sorted_group[i]['name']]:
                                sorted_group[i]['block_number'] = newgroup[-2]['block_number']
                                newgroup.append(sorted_group[i])
                                temp = {}
                                temp['name'] = 'spe_' + sorted_group[i]['name']
                                temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                                temp['block_number'] = newgroup[-1]['block_number'] + 1
                                temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                                newgroup.append(temp)
                            else:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                                newgroup.append(sorted_group[i])
                                temp = {}
                                temp['name'] = 'spe_' + sorted_group[i]['name']
                                temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                                temp['block_number'] = newgroup[-1]['block_number'] + 1#
                                temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                                newgroup.append(temp)
                        else:
                            if name_and_block_dict[newgroup[-1]['name']] == name_and_block_dict[sorted_group[i]['name']]:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number']
                                newgroup.append(sorted_group[i])
                                temp = {}
                                temp['name'] = 'spe_' + sorted_group[i]['name']
                                temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                                temp['block_number'] = newgroup[-1]['block_number'] + 1#
                                temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                                newgroup.append(temp)
                            else:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                                newgroup.append(sorted_group[i])
                                temp = {}
                                temp['name'] = 'spe_' + sorted_group[i]['name']
                                temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                                temp['block_number'] = newgroup[-1]['block_number'] + 1#
                                temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                                newgroup.append(temp)
                    except IndexError:
                        newgroup.append(sorted_group[i])
                        temp = {}
                        temp['name'] = 'spe_' + sorted_group[i]['name']
                        temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                        temp['block_number'] = newgroup[-1]['block_number'] + 1
                        temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                        newgroup.append(temp)
                    # else:
                    #     if name_and_block_dict[newgroup[-1]['name']] == name_and_block_dict[sorted_group[i]['name']]:
                    #         sorted_group[i]['block_number'] = newgroup[-1]['block_number']
                    #         newgroup.append(sorted_group[i])
                    #         temp = {}
                    #         temp['name'] = 'spe_' + sorted_group[i]['name']
                    #         temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                    #         temp['block_number'] = newgroup[-1]['block_number'] + 1#
                    #         temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                    #         newgroup.append(temp)
                    #     else:
                    #         sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                    #         newgroup.append(sorted_group[i])
                    #         temp = {}
                    #         temp['name'] = 'spe_' + sorted_group[i]['name']
                    #         temp['group_availability_code'] = sorted_group[i]['group_availability_code']
                    #         temp['block_number'] = newgroup[-1]['block_number'] + 1#
                    #         temp['short_names'] = 'SPE ' + sorted_group[i]['short_names']
                    #         newgroup.append(temp)
                # if sorted_group[i]['name'] in spe_list:
                
                '''works for FO CONS, Main Engine Air Cooler, Main Engine Controls, partially for BASIC'''
                # try:
                    # if sorted_group[i+1]['block_number'] != sorted_group[i]['block_number']:
                    #     if temp['name'] not in spekeylist:
                    #         sorted_group[i+1]['block_number'] = temp['block_number']
                    #     if temp['name'] in spekeylist:
                    #         sorted_group[i+1]['block_number'] = temp['block_number'] + 1
                    # else:
                    #     if name_and_block_dict[sorted_group[i+1]['name']] != name_and_block_dict[sorted_group[i]['name']]:
                    #         print("YES",sorted_group[i]['name'])
                    #         if temp['name'] in spekeylist:
                    #             sorted_group[i+1]['block_number'] = temp['block_number'] + 1
                    #         if temp['name'] not in spekeylist:
                    #             sorted_group[i+1]['block_number'] = temp['block_number']
                    #     else:
                    #         sorted_group[i+1]['block_number'] = sorted_group[i]['block_number']


                    # if temp['name'] in spekeylist:
                    #     if sorted_group[i+1]['group_availability_code'] % 10 == sorted_group[i]['group_availability_code']:
                    #         sorted_group[i+1]['block_number'] = sorted_group[i]['block_number']
                    #     if sorted_group[i+1]['block_number'] + 2 == temp['block_number']:
                    #         sorted_group[i+1]['block_number'] = temp['block_number'] + 1
                    #     elif sorted_group[i+1]['block_number'] + 2 == sorted_group[i]['block_number']:
                    #         sorted_group[i+1]['block_number'] = temp['block_number'] + 1
                    #     else:
                    #         sorted_group[i+1]['block_number'] = sorted_group[i+1]['block_number'] + 2
                    # elif temp['name'] not in spekeylist and sorted_group[i]['block_number'] != sorted_group[i+1]['block_number']:
                    #     sorted_group[i+1]['block_number'] = temp['block_number']
                    # elif temp['name'] not in spekeylist and sorted_group[i]['block_number'] == sorted_group[i+1]['block_number']:
                    #     sorted_group[i+1]['block_number'] = sorted_group[i]['block_number']
                    # else:
                    #     sorted_group[i+1]['block_number'] = sorted_group[i+1]['block_number'] + 1
                # except IndexError:
                #     continue
            else:
                if sorted_group[i]['block_number'] == 1:
                    newgroup.append(sorted_group[i])
                else:
                    try:
                        if newgroup[-1]['name'] in spekeylist:
                            if name_and_block_dict[newgroup[-2]['name']] == name_and_block_dict[sorted_group[i]['name']]:
                                sorted_group[i]['block_number'] = newgroup[-2]['block_number']
                                newgroup.append(sorted_group[i])
                            else:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                                newgroup.append(sorted_group[i])
                        else:
                            if name_and_block_dict[newgroup[-1]['name']] == name_and_block_dict[sorted_group[i]['name']]:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number']
                                newgroup.append(sorted_group[i])
                            else:
                                sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                                newgroup.append(sorted_group[i])
                    except IndexError:
                        newgroup.append(sorted_group[i])
                    # if name_and_block_dict[sorted_group[i-1]['name']] == name_and_block_dict[sorted_group[i]['name']]:
                    #     sorted_group[i]['block_number'] = sorted_group[i-1]['block_number']
                    #     newgroup.append(sorted_group[i])
                    # else:
                    #     tempName2 = 'spe_' + sorted_group[i-1]['name']
                    #     if tempName2 in spekeylist:
                    #         sorted_group[i]['block_number'] = newgroup[-1]['block_number'] + 1
                    #         newgroup.append(sorted_group[i])
                    #     else:
                    #         sorted_group[i]['block_number'] = sorted_group[i-1]['block_number'] + 1
                    #         newgroup.append(sorted_group[i])
            # else:
            #     continue
        print("END SPE INSIDE")
        ''' Completely works for BASIC'''
        ''' List of block numbers of parameters with substring SPE in their names.'''
        # blocklist = []
        # for i in range(len(newgroup)):
        #     if 'spe_' in newgroup[i]['name']:
        #         blocklist.append(newgroup[i]['block_number'])
        
        # block_params = []
        # for i in blocklist:
        #     for j in range(len(newgroup)):
        #         if 'spe_' not in newgroup[j]['name'] and newgroup[j]['block_number'] == i:
        #             if newgroup[j]['block_number'] not in block_params:
        #                 block_params.append(newgroup[j]['block_number'])
        # print("BLOCK PARAMS !!!!!", block_params)
        
        # ''' Adding 1 to all those block numbers that are same as the ones in block_list'''
        # for i in block_params:
        #     # print("BLOCK!!!!!!",i)
        #     # temp = i + 1
        #     for group in newgroup:
        #         if group['block_number'] == i and 'spe_' in group['name']:
        #             continue
        #         if group['block_number'] >= i:
        #             group['block_number'] = group['block_number'] + 1
        
        # ''' List of block numbers of parameters with substring spe and without.'''
        # block_params = []
        # for i in blocklist:
        #     for j in range(len(newgroup)):
        #         if ('spe_' not in newgroup[j]['name'] or 'spe_' in newgroup[j]['name']) and newgroup[j]['block_number'] == i:
        #             block_params.append(newgroup[j]['block_number'])
        # ''' Adding 1 to block numbers that are same as the ones in block_params'''
        # for i in block_params:
        #     for j in range(len(newgroup)):
        #         if newgroup[j]['block_number'] == i and 'spe_' in newgroup[j]['name']:
        #             continue
        #         if newgroup[j]['block_number'] >= i:
        #             newgroup[j]['block_number'] = newgroup[j]['block_number'] + 1

        print("NEWGROUP!!!!",newgroup)
        
        return newgroup




    def temporary_group_selection_for_11(self, groupname):
        ''' Temporary group selection for multi-axis'''
        result = []
        singledata = self.ship_configs['data']
        for var in singledata.keys():            
            groups = singledata[var]['group_selection']
            for group in groups:
                if group['groupname'] == 'COMBUSTION PROCESS':
                    if 'Combst' in singledata[var]['short_names'] and '1' in singledata[var]['short_names']:
                        temp = {'short_names': singledata[var]['short_names']}
                        group.update(temp)
                        result.append(group)
                    if 'ME Exh Temp' in singledata[var]['short_names'] and '2' in singledata[var]['short_names']:
                        temp = {'short_names': singledata[var]['short_names']}
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
    
    def get_short_names_list_for_multi_axis(self, groupname):
        ''' Returns the list of short names from the group selection of multi axis'''
        groupList = self.temporary_group_selection_for_11('MultiAxis') # Only passing hard set groupname because
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

        grpnames=self.get_list_of_groupnames()
        generic=self.get_dict_groupnames()
                
        
        for i in grpnames:
            block = self.get_number_of_blocks(i)
            temp_dict={}
            blocks=[]
            for j in block:
                temp_list=[]
                for var in singledata.keys():
                    groups = singledata[var]['group_selection']
                    for group in groups:
                        if group['block_number'] == j and group['groupname'] == i:
                            temp_list.append(singledata[var]['short_names'])
                temp_dict['Sub-Group '+ str(int(j))] = temp_list
            # blocks.append(temp_dict)
            if i in generic.keys():
                generic[i] = temp_dict

        return generic

    def get_dict_groupnames(self):
        ''' Returns a dictionary with keys as the names of individual groups
            and values as empty lists.
        '''
        grpnames=self.get_list_of_groupnames()

        generic_dict={}
        for i in grpnames:
            generic_dict[i] = []

        return generic_dict
    
    def get_list_of_groupnames(self):
        ''' Returns list of all the groupnames'''
        singledata = self.ship_configs['data']
        grpnames=[]
        for var in singledata.keys():
            groups = singledata[var]['group_selection']
            for group in groups:
                if group['groupname'] not in grpnames:
                    grpnames.append(group['groupname'])
        return grpnames
        
    
    def get_number_of_blocks(self, groupname='', groupsList=[]):
        ''' Returns the list of block numbers in a particular group'''
        block_number = []
        if groupname != '':
            singledata = self.ship_configs['data']
            for var in singledata.keys():
                groups = singledata[var]['group_selection']
                for group in groups:
                    if group['groupname'] == groupname:
                        if group['block_number'] not in block_number:
                            block_number.append(group['block_number'])
        else:
            for i in range(len(groupsList)):
                if groupsList[i]['block_number'] not in block_number:
                    block_number.append(groupsList[i]['block_number'])
        block_number.sort()
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
        return res1, res2, names
    
    def get_dict_and_list_according_to_groups(self, groupsList):
        ''' Returns a dictionary with the name of the parameters (as per the group selection)
            as keys and empty lists as values.
            Also returns a list of parameter names.
        '''
        res1 = {}
        res2 = {}
        names=[]
        # group = self.get_group_selection(groupname)
        # count = self.get_number_of_group_members(groupname)
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
    
    def get_overall_min_and_max(self, groupsList, datadict):
        ''' Returns a dictionary with the block numbers as keys and a dictionary containing
            overall minimum and maximum of all variables according to blocks, as values.
        '''
        # groups = self.get_group_selection(groupname)
        # groupName = groupsList[0]['groupname'] if 'groupname' in groupsList[0] else ''
        blocks = self.get_number_of_blocks('', groupsList)
        block_wise_datadict={}

        print("START PROCESS OVERALL 1st FOR LOOP")
        for block in blocks:
            minNum=None
            maxNum=None
            print('OVERALL 2nd FOR LOOP')
            for group in range(len(groupsList)):
                print("OVERALL 3rd FOR LOOP")
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
        print("END PROCESS OVERALL 1st FOR LOOP")
        
        return block_wise_datadict
    
    def get_shapes_for_loaded_ballast_data(self):
        maindbcollection = self.get_main_data()
        loaded_ballast_list=[]
        res_data={'Ballast': [], 'Loaded': [], 'Engine': [], 'Nav': []}
        
        tempList=[]
        for doc in maindbcollection.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'vessel_loaded_check': 1, 'data_available_engine': 1, 'data_available_nav': 1, 'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
            newdate = doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
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
        for doc in maindbcollection.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'vessel_loaded_check': 1, 'data_available_engine': 1, 'data_available_nav': 1, 'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
            newdate = doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
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
        for doc in maindbcollection.find({'ship_imo': self.ship_imo}, {'ship_imo': 1, 'data_available_engine': 1, 'data_available_nav': 1, 'processed_daily_data.rep_dt.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', ASCENDING):
            newdate = doc['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d')
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
        # print("RES NO DATA!!!!!!!!", res_data['No Data'])
        
        for i in res_data['Ballast']:
            print(i)
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
                #     print(rel_date.months)
                #     tempballast['x0'] = i[0]
                #     tempballast['x1'] = i[-1]
                #     loaded_ballast_list.append(tempballast)
            except IndexError:
                continue
        for i in res_data['Loaded']:
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
        print("START GET MAIN DATA")
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
        print("END GET MAIN DATA")
        # groups = self.get_group_selection(groupname)
        print("START BLOCK WISE DATADICT")
        block_wise_datadict = self.get_overall_min_and_max(groupsList, datadict)
        print("END BLOCK WISE DATADICT")
        # print(block_wise_datadict)

        result={}
        
        print("START PROCESSING BALLAST LOADED 1st FOR LOOP")
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
                print("2nd FOR LOOP")
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
        print("END PROCESSING BALLAST LOADED 1st FOR LOOP")

        return result, block_wise_datadict
    
    ''' Functions for Daily Report processing '''
    def get_category_dict(self):
        ''' Returns a dictionary containing category as the key and the list of subcategories in that category as the value'''
        # single = tempRes[0]
        print(type(self.ship_configs))
        singledata = self.ship_configs['data']
        catDict={}
        catList=[]
        categoryDict={}
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
                        # print(temp)
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

        for var in self.ship_configs['data']:
            if type(self.ship_configs['data'][var]['short_names']) == float:
                shortNameDict[var] = var
            else:
                shortNameDict[var] = self.ship_configs['data'][var]['short_names']
        
        return shortNameDict
    
    def get_Equipment_and_Parameter_list(self):
        ''' Returns the list of parameters that are classified as equipments(E)
            and the list of parameters that are classified as parameters(P).
        '''
        singledata = self.ship_configs['data']
        equipment_list = []
        parameter_list = []

        for var in singledata.keys():
            if pd.isnull(singledata[var]['var_type']) != True:
                if 'E' in singledata[var]['var_type'] and '1' not in singledata[var]['var_type'] and singledata[var]['source_idetifier'] == 'available':
                    equipment_list.append(var)
                if 'P' in singledata[var]['var_type']:
                    parameter_list.append(var)
        # for var in singledata.keys():
        #     if singledata[var]['var_type'] == 'E':
        #         print(singledata[var])
        print("EQUIPMENT LIST", equipment_list)
        
        return equipment_list, parameter_list
    
    def check_for_nan_and_replace(self, data):
        dupli_data = data
        for var in dupli_data['processed_daily_data'].keys():
            for subvar in dupli_data['processed_daily_data'][var]:
                if subvar == 'predictions':
                    for i in range(0, 3):
                        try:
                            try:
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
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m3']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m3'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m6']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m6'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m12']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m12'] = 'null'
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
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m3']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m3'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m6']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m6'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m12']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m12'] = 'null'
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
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m3']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m3'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m6']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m6'] = 'null'
                        if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['m12']) == True:
                            dupli_data['processed_daily_data'][var][subvar]['m12'] = 'null'
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
                except TypeError:
                    continue
                except KeyError:
                    continue
        return subcategoryDictData
    
    def get_category_and_subcategory_with_issues(self, dateString):
        ''' Returns all the categories and the subcategories that have outlier parameters.'''
        result={}
        ship_collection = self.get_ship_configs()
        issues, issuesCount = self.create_dict_of_issues(dateString=dateString) if dateString != "" else self.create_dict_of_issues("")
        for doc in ship_collection.find({'ship_imo': self.ship_imo}):
            for key in doc['data'].keys():
                for var in issues[str(self.ship_imo)]:
                    if pd.isnull(doc['data'][key]['short_names']) == False and doc['data'][key]['short_names'].strip() == var:
                        tempList=[]
                        if doc['data'][key]['subcategory'].strip() not in tempList:
                            tempList.append(doc['data'][key]['subcategory'].strip())
                            print(tempList)
                        if doc['data'][key]['category'].strip() not in result.keys():
                            result[doc['data'][key]['category'].strip()] = tempList
                            print(tempList)
                        else:
                            result[doc['data'][key]['category'].strip()].extend(tempList)
        print(result)
        return result
    
    def get_static_data_for_charter_party(self):
        ''' Returns the dictionary with the static data of the charter party parameters. '''
        ship_collection = self.get_ship_configs()
        result = {}

        for doc in ship_collection.find({'ship_imo': self.ship_imo}):
            for key in doc['data'].keys():
                if doc['data'][key]['subcategory'] == 'Charter party':
                    result[key] = doc['data'][key]['static_data'] if pd.isnull(doc['data'][key]['static_data']) == False else None
        
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

        if dateString != '':
            findDate = datetime.strptime(dateString,"%Y-%m-%d, %H:%M:%S")
            for cp in charter_party_dict.keys():
                for doc in maindb_collection.find({'ship_imo': self.ship_imo, 'processed_daily_data.rep_dt.processed': findDate}):
                    for key in doc['processed_daily_data'].keys():
                        if key == charter_party_dict[cp]:
                            result[cp] = self.makeDecimal(doc['processed_daily_data'][key]['processed'])
            return result
        else:
            for cp in charter_party_dict.keys():
                for doc in maindb_collection.find({'ship_imo': self.ship_imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING).limit(1):
                    for key in doc['processed_daily_data'].keys():
                        if key == charter_party_dict[cp]:
                            result[cp] = self.makeDecimal(doc['processed_daily_data'][key]['processed'])
        return result



    ''' Functions for Interactive processing '''
    print("START CREATE REGRESSION")
    def regress_for_constant_x(self,data,x1,y,z=None,**other_x):
        '''
            function for prediction by changing 1 x dimension and keeping other dimensios contant
            data=dataframe,x1=the x dimension to be changed,y= y dimension(to be predicted),otherx=other x dimensions constant values
        '''
        x_list=[]
        temp_data={}
        if z is not None:
            for column in data:
                if column!=z:
                    x_list.append(str(column))  
                    temp_data[column]=[]
            for key in other_x:
                for key_2 in temp_data:
                    if key==key_2:
                        temp_data[key]=other_x[key]

            x1_min = data[x1].min()
            x1_max = data[x1].max()
            y_min = data[y].min()
            y_max = data[y].max()
            x1_list=np.arange(x1_min, x1_max, 0.1)
            # lenx1 = len(x1_list) - 1
            y_list=np.arange(y_min, y_max, 0.1)
            # print(y_min, y_max, y_list)
            # y_list = y_list[0:lenx1]
            # reg=LinearRegression()
            reg = LRPI()
            poly = PolynomialFeatures(degree = 3)
            if not (np.any(pd.isnull(data[x_list])) and np.all(pd.isfinite(data[x_list]))):
                if not (np.any(pd.isnull(data[z])) and np.all(np.isfinite(data[z]))):
                    X_poly = poly.fit_transform(data[x_list])
                    # X_poly = pd.DataFrame(poly.fit_transform(data[x_list]), x_list)
                    poly.fit(X_poly, data[z])
                    reg.fit(X_poly, data[z])
            else:
                return "x_list or y has nan or infinite"
            # print(reg.predict(X_poly))
            pred_list=[]
            # pls_dataframe = pd.DataFrame(columns = ['lower', 'Pred', 'upper'])
            # print("EMPTY DATAFRAME", pls_dataframe)
            for i in x1_list:
                # x1_list.append(i)
                temp_data[x1]=[i]
            
            for i in y_list:
                # y_list.append(i)
                print(i)
                i = i.round(1)
                temp_data[y] = [i]
                temp_dataframe=pd.DataFrame(temp_data)
                pred=reg.predict(poly.fit_transform(temp_dataframe))
                pred_list.append(pred['Pred'][0])
            # new_pred_list = self.create_2D_array(pred_list)
            return x1_list.tolist(), y_list.tolist(), pred_list
        else:
            for column in data:
                if column!=y:
                    x_list.append(str(column))  
                    temp_data[column]=[]
            for key in other_x:
                if key in list(temp_data.keys()):
                    temp_data[key] = other_x[key]
            print("TEMP", temp_data)
            print("DATAFRAME", data)

            minValue = data[x1].min()
            maxValue = data[x1].max()
            # x1_list=np.arange(minValue, maxValue, 0.1)
            x1_list = np.linspace(minValue, maxValue, len(data[x1]))
            # data[x1] = x1_list
            
            reg=LinearRegression()
            poly = PolynomialFeatures(degree = 2)
            # pls_dataframe = pd.DataFrame(columns = ['lower', 'Pred', 'upper'])
            # print("EMPTY DATAFRAME", pls_dataframe)
            # if not (np.any(pd.isnull(data[x_list])) and np.all(pd.isfinite(data[x_list]))):
            #     if not (np.any(pd.isnull(data[y])) and np.all(np.isfinite(data[y]))):
            X_poly = poly.fit_transform(data[x_list])
            poly.fit(X_poly, data[y])
            reg.fit(X_poly, data[y])
            # else:
            #     return "x_list or y has nan or infinite"
            pred_list=[]
            for i in x1_list:
                # x1_list.append(i)
                # i = i.round(1)
                temp_data[x1]=[i.round(1)]
                temp_dataframe=pd.DataFrame(temp_data)
                pred=reg.predict(poly.fit_transform(temp_dataframe))
                pred_list.append(pred[0])
            return x1_list.tolist(), pred_list
    print("END CREATE REGRESSION")
    
    print("START CREATE DATAFRAME")
    def create_dataframe(self, X, Y, duration, Z=None, **other_X):
        maindb_collection = database.get_collection("Main_db")
        print("START SHORT NAME DICT")
        shortNameDict = self.create_short_names_dictionary()
        print("END SHORT NAME DICT")
        get_close_by_date = self.create_maindb_according_to_duration(duration)
        X_list = []
        Y_list = []
        Z_list = []
        dict_for_dataframe = {}
        print("START READ DATA AND CREATE LISTS")
        for doc in maindb_collection.find({'ship_imo': self.ship_imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING):
            if doc['processed_daily_data']['rep_dt']['processed'] != get_close_by_date:
                for key in doc['processed_daily_data'].keys():
                    if doc['processed_daily_data'][key]['identifier'].strip() == X:
                        if pd.isnull(doc['processed_daily_data'][key]['processed']):
                            X_list.append(0)
                        else:
                            X_list.append(doc['processed_daily_data'][key]['processed'])
                    if doc['processed_daily_data'][key]['identifier'].strip() == Y:
                        if pd.isnull(doc['processed_daily_data'][key]['processed']):
                            Y_list.append(0)
                        else:
                            Y_list.append(doc['processed_daily_data'][key]['processed'])
                    if Z is not None:
                        if doc['processed_daily_data'][key]['identifier'].strip() == Z:
                            if pd.isnull(doc['processed_daily_data'][key]['processed']):
                                Z_list.append(0)
                            else:
                                Z_list.append(doc['processed_daily_data'][key]['processed'])
            else:
                break
        print("END READ DATA AND CREATE LISTS")
        print("START CREATE OTHER LISTS")
        for key in other_X:
            if key != 'None':
                tempList = []
                for i in range(len(X_list)):
                    tempList.append(other_X[key])
                dict_for_dataframe[key] = tempList
        print("END CREATE OTHER LISTS")
        
        dict_for_dataframe[X] = X_list
        dict_for_dataframe[Y] = Y_list
        X_name = shortNameDict[X]
        Y_name = shortNameDict[Y]
        
        if Z is not None and len(Z_list) != 0:
            dict_for_dataframe[Z] = Z_list
            Z_name = shortNameDict[Z]
            dataframe = pd.DataFrame(dict_for_dataframe)
            return dataframe, X_name, Y_name, Z_name, X_list, Y_list, Z_list
        else:
            dataframe = pd.DataFrame(dict_for_dataframe)
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
        # print(len(X_list), len(Y_list), len(Z_list))
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
    print("START CREATE ACC TO DURATION")
    def create_maindb_according_to_duration(self, duration, main_db=[]):
        # print("\n",main_db)
        # if main_db is not []:
        #     # print(loads(dumps(main_db)))
        #     # maindb = loads(dumps(main_db))
        #     maindb = main_db
        #     # print(maindb)
        # else:
        dateList=[]
        maindb_collection = database.get_collection("Main_db")
        print("START CREATE DATE LIST")
        maindb = maindb_collection.find({'ship_imo': self.ship_imo})
        for index, item in enumerate(maindb):
            dateList.append(item['processed_daily_data']['rep_dt']['processed'])
        # for doc in maindb_collection.find({'ship_imo': self.ship_imo}):
        #     dateList.append(doc['processed_daily_data']['rep_dt']['processed'])
        print("END CREATE DATE LIST")
        print("START SORT")
        dateList.sort()
        print("END SORT")
        current_date = dateList[-1]
        number_of_days = self.get_duration(duration)
        if 'Year' in duration:
            days_to_subtract = timedelta(weeks = 52)
        else:
            days_to_subtract = timedelta(days = number_of_days)
        
        to_date = current_date - days_to_subtract

        # get_close_by_date = self.get_nearest_date(dateList, to_date)
        if to_date in dateList:
            return to_date
        else:
            return min(dateList, key=lambda x: abs(x - to_date))
        #     self.maindb = maindb_collection.find({"ship_imo": self.ship_imo}).sort('processed_daily_data.rep_dt.processed', ASCENDING)
        #     maindb = loads(dumps(self.maindb))
        #     print(len(maindb))
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
    print("END CREATE ACC TO DURATION")
    
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
    
    def create_2D_array(self, array):
        new_2D_array = np.reshape(array, (8,8))
        
        return new_2D_array.tolist()
    
    # def get_ship_stats(self, data, *variables):
    #     stats_dict={}
    #     tempList=[]
    #     for i in variables:
    #         for val_dict in range(len(data)):
    #             for key in data[val_dict]['processed_daily_data'].keys():
    #                 keyName = data[val_dict]['processed_daily_data'][key]['name'].strip()
    #                 if i == keyName:
    #                     tempList.append(data[val_dict]['processed_daily_data'][key]['processed'])
    #         mintemp = min(tempList)
    #         maxtemp = max(tempList)
    #         stats_dict[i] = {'Min': mintemp, 'Max': maxtemp}
    #     print(stats_dict)
    #     return stats_dict
    
    def get_ship_stats_2(self, data, *variables):
        data_dict={}
        stats_dict={}
        maindb_collection = database.get_collection("Main_db")
        # maindb = maindb_collection.find({'ship_imo': self.ship_imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING)
        for i in variables:
            tempList=[]
            for doc in maindb_collection.find({'ship_imo': self.ship_imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING):
                if doc['processed_daily_data']['rep_dt']['processed'] != data:
                    tempList.append(doc['processed_daily_data'][i]['processed'])
                else:
                    break
            data_dict[i] = tempList
        # for i in variables:
        #     tempList=[]
        #     for val_dict in range(len(data)):
        #         for key in data[val_dict]['processed_daily_data'].keys():
        #             if data[val_dict]['processed_daily_data'][key]['identifier'].strip() == i:
        #                 tempList.append(data[val_dict]['processed_daily_data'][key]['processed'])
        #     data_dict[i] = tempList
        
        for key in data_dict.keys():
            try:
                minValue = min(data_dict[key])
                maxValue = max(data_dict[key])
                stats_dict[key] = {"Min": math.floor(minValue), "Max": math.ceil(maxValue)}
            except TypeError:
                continue
            except ValueError:
                continue
        # print(stats_dict)
        return stats_dict
    
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
        
        return optionsList
    
    ''' Functions for Overview Processing'''
    def create_imo_and_name_strings(self):
        ''' Returns the list of strings of the ships.
            Strings format: imo<br />name
        '''
        ship_collection = self.get_ship_configs()
        result=[]

        for doc in ship_collection.find({}).sort('ship_imo', ASCENDING):
            ship_imo = doc['ship_imo']
            ship_name = doc['static_data']['ship_name']['value'].strip()
            new_string = str(ship_imo) + '-' + ship_name
            result.append(new_string)
        
        return result
    
    def get_list_of_ship_imos(self):
        ''' Returns a list of ship imos'''
        ship_collection = self.get_ship_configs()
        result=[]

        for doc in ship_collection.find({}).sort('ship_imo', ASCENDING):
            result.append(doc['ship_imo'])
        
        return result
    
    def create_vessel_load_list(self):
        ''' Returns the vessel load of each ship in a list.'''
        maindb_collection = self.get_main_data()
        ship_imo_list = self.get_list_of_ship_imos()
        result = []
        for imo in ship_imo_list:
            for doc in maindb_collection.find({'ship_imo': imo}, {'vessel_loaded_check': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', DESCENDING):
                result.append(doc['vessel_loaded_check'])
                break
        
        return result
    
    def create_eta_list(self):
        ''' Returns the eta of each ship in a list.'''
        maindb_collection = self.get_main_data()
        ship_imo_list = self.get_list_of_ship_imos()
        result = []

        for imo in ship_imo_list:
            for doc in maindb_collection.find({'ship_imo': imo}, {'processed_daily_data.eta.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', DESCENDING):
                newdate = doc['processed_daily_data']['eta']['processed'].strftime('%Y-%m-%d')
                result.append(newdate)
                break
        
        return result
    
    def create_cp_compliance_list(self):
        ''' Returns the cp compliance denoted by the identifier cp_hfo_cons in a list.'''
        maindb_collection = self.get_main_data()
        ship_imo_list = self.get_list_of_ship_imos()
        result = []

        for imo in ship_imo_list:
            for doc in maindb_collection.find({'ship_imo': imo}, {'processed_daily_data.cp_hfo_cons.processed': 1, '_id': 0}).sort('processed_daily_data.rep_dt.processed', DESCENDING):
                result.append(doc['processed_daily_data']['cp_hfo_cons']['processed'])
                break
        
        return result
    
    def create_list_of_active_ships(self):
        ''' Returns the list of active ships.'''
        result=[]
        ship_imos = self.get_list_of_ship_imos()
        daily_data_collection = database.get_collection('daily_data')
        for imo in ship_imos:
            try:
                for doc in daily_data_collection.find_one({'ship_imo': imo}):
                    if imo not in result:
                        result.append(imo)
            except TypeError:
                result.append(None)
        
        return result
    
    def create_dict_of_issues(self, dateString=''):
        ''' Returns a dictionary of all the issues per ship. 
            Also used in the daily report process
        '''
        ship_imos = self.get_list_of_ship_imos()
        maindb_collection = self.get_main_data()
        result = {}
        
        issuesCount = {}
        if dateString != "":
            findDate = datetime.strptime(dateString,"%Y-%m-%d, %H:%M:%S")
            for imo in ship_imos:
            # print(imo)
                tempList = []
                outlierCount = 0
                operationalCount = 0
                indicesCount = 0
                for doc in maindb_collection.find({'ship_imo': imo, 'processed_daily_data.rep_dt.processed': findDate}):
                    for key in doc['processed_daily_data'].keys():
                        try:
                            if doc['processed_daily_data'][key]['within_outlier_limits']['m3'] == False or doc['processed_daily_data'][key]['within_operational_limits']['m3'] == False or (doc['processed_daily_data'][key]['SPEy']['m3'] > doc['processed_daily_data'][key]['Q_y']['m3'][1]):
                                # print("YES")
                                if key not in tempList:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip())
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
                issuesCount[imo] = {'outlier': outlierCount, 'operational': operationalCount}
        else:
            for imo in ship_imos:
                # print(imo)
                tempList = []
                outlierCount = 0
                operationalCount = 0
                indicesCount = 0
                for doc in maindb_collection.find({'ship_imo': imo}).sort('processed_daily_data.rep_dt.processed', DESCENDING).limit(1):
                    for key in doc['processed_daily_data'].keys():
                        try:
                            if doc['processed_daily_data'][key]['within_outlier_limits']['m3'] == False or doc['processed_daily_data'][key]['within_operational_limits']['m3'] == False or (doc['processed_daily_data'][key]['SPEy']['m3'] > doc['processed_daily_data'][key]['Q_y']['m3'][1]):
                                # print("YES")
                                if key not in tempList:
                                    tempList.append(doc['processed_daily_data'][key]['name'].strip())
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
                issuesCount[imo] = {'outlier': outlierCount, 'operational': operationalCount}
                    # result[imo] = tempList
                        # result[imo] = tempList
        print("DICT OF ISSUES", result)
        return result, issuesCount

    def create_cp_compliance_dict(self):
        ''' Returns if the ship is cp compliant.'''
        ship_collection = self.get_ship_configs()
        maindb_collection = self.get_main_data()
        charter_party_info = {}

        for doc in ship_collection.find({}).sort('ship_imo', ASCENDING):
            tempList = []
            print(doc['ship_imo'])
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
                            if doc['processed_daily_data'][key]['processed'] > dicts['static_data']:
                                compliant = "No"
                            else:
                                compliant = "Yes"
                        dicts.update({'compliant': compliant})

        return charter_party_info




        
# obj=Configurator(9591301)
# obj.get_ship_configs()
# res = obj.get_group_selection_for_individual_parameters('draft_mean', 'BASIC')
# print(res)
# res = obj.text_wrapping('FO Serv Tank #1 Temp')
# print(res)
# obj.get_loaded_and_ballast_data()
# obj.get_main_data()
# obj.get_ship_stats('rpm', 'w_force', 'sea_st', 'draft_mean', 'trim')
# obj.create_maindb_according_to_duration('30Days')
# obj.get_nearest_date('')
# df, x_name, y_name, z_name, x_list, y_list, z_list = obj.create_dataframe('draft_mean','real_slip', '30Days', 'pwr', rpm=50,beaufort=1,sea_state=1,draft=5,trim=-2)
# obj.regress_for_constant_x(df, 'draft_mean', 'real_slip', 'pwr', rpm=50,beaufort=1,sea_state=1,draft=5,trim=-2)
# lists = obj.static_lists()
# print(lists)
# obj.engine_var_list()
# obj.nav_var_list()
# obj.ais_api_var_list()
# obj.calculated_var_list()
# obj.get_group_selection("COMBUSTION PROCESS")
# b=obj.base_formula("ship_lenlbp*ship_beam*ship_maxsummerdft")
# print(b)


