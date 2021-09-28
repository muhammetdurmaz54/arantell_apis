from math import isfinite, isnan
import sys
import re
from numpy.core.numeric import NaN
import pandas as pd
sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from datetime import datetime, timedelta
import numpy as np
from pymongo import MongoClient
from bson.json_util import dumps, loads
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from src.processors.config_extractor.prediction_interval import LRPI
from pymongo import ASCENDING
import math


# client = MongoClient("mongodb://localhost:27017")
# db=client.get_database("aranti")
client = MongoClient("mongodb+srv://iamuser:iamuser@democluster.lw5i0.mongodb.net/test")
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
    'VESSEL POSITION': ["Name", "Reported"],
    'FUEL OIL CONSUMPTION': common_header,
    'MAIN ENGINE': common_header,
    'GENERATOR': common_header,
    'AUXILLIARIES': common_header
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
    'AUXILLIARIES': 9
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

    ''' Functions for Trends processing '''
    def get_group_selection(self,groupname):
        '''For getting the groups list for particular group'''
        result = []
        singledata = self.ship_configs['data']
        for var in singledata.keys():            
            groups = singledata[var]['group_selection']
            for group in groups:
                if group['groupname'] == groupname:
                    tempName = self.text_wrapping(singledata[var]['short_names'])
                    temp = {'short_names': tempName}
                    group.update(temp)
                    result.append(group)
        return result

    def temporary_group_selection_for_11(self, groupname):
        ''' Temporary group selection for multi-axis'''
        result = []
        singledata = self.ship_configs['data']
        for var in singledata.keys():            
            groups = singledata[var]['group_selection']
            for group in groups:
                if group['groupname'] == 'COMBUSTION PROCESS':
                    if 'Combst' in singledata[var]['short_names'] and '1' in singledata[var]['short_names']:
                        temp = {'short_names': singledata[var]['short_names'], 'unit': singledata[var]['unit']}
                        group.update(temp)
                        result.append(group)
                    if 'ME Exh Temp' in singledata[var]['short_names'] and '1' in singledata[var]['short_names']:
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
                    temp = {vars: singledata[vars]['short_names']}
                    result.append(temp)
                if vars == 'sfoc':
                    temp={vars: singledata[vars]['short_names']}
                    result.append(temp)
        result.insert(0, {'rep_dt': 'Report Date'})
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

    def get_height_of_chart(self, groupname):
        ''' Returns the height of the chart to be set according to the group'''
        height=0
        blocks=None
        if 'Multi Axis' not in groupname:
            blocks = self.get_number_of_blocks(groupname)
            if(len(blocks) != 0):
                block = max(blocks)
                if(block <= 3):
                    # height = default_height
                    return default_height
                else:
                    height = 200 + height_per_block*block
                    return height
        else:
            height = default_height
            return height

    def get_height_of_chart_after_double_click(self, groupname):
        ''' Returns the height of the chart to be set according to the group after double click on the chart'''
        height=0
        blocks=None
        if 'Multi Axis' not in groupname:
            blocks = self.get_number_of_blocks(groupname)
            if(len(blocks) != 0):
                block = max(blocks)
                if(block <= 3):
                    # height = default_height
                    return default_height
                else:
                    height = 200 + height_per_block_after_double_click*block
                    return height
        else:
            height = default_height
            return height
    
    def get_Y_position_for_legend(self, groupname):
        ''' Returns the Y position for setting the legend on the chart'''
        blocks = self.get_number_of_blocks(groupname)
        if len(blocks) != 0:
            block = max(blocks)
        if block < 8:
            y_position = -((block - 1)/100) - height_per_block/10000
            return y_position
        if block >= 8:
            y_position = -((block - 1)/100) + height_per_block/10000
            return y_position

    def get_Y_position_for_legend_after_double_click(self, groupname):
        ''' Returns the Y position for setting the legend on the chart after double click on the chart'''
        blocks = self.get_number_of_blocks(groupname)
        prev_height = self.get_height_of_chart_after_double_click(groupname)
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
        
    
    def get_number_of_blocks(self, groupname):
        ''' Returns the list of block numbers in a particular group'''
        block_number = []
        singledata = self.ship_configs['data']
        for var in singledata.keys():
            groups = singledata[var]['group_selection']
            for group in groups:
                if group['groupname'] == groupname:
                    if group['block_number'] not in block_number:
                        block_number.append(group['block_number'])
        return block_number
    
    def get_variables_list_in_order_of_blocks(self, groupname):
        ''' Returns the list of variables in the ascending order of the blocks w.r.t. groupname.'''
        block_list = self.get_number_of_blocks(groupname)
        # print(block_list)
        singledata = self.ship_configs['data']
        variable_list=[]

        for block in reversed(block_list):
            for var in singledata.keys():
                groups = singledata[var]['group_selection']
                for group in groups:
                    if group['groupname'] == groupname and group['block_number'] == block:
                        variable_list.append(var)
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

    def temporary_dict_and_list_according_to_groups(self, groupname):
        ''' TEMPORARY Returns a dictionary with the name of the parameters (as per the group selection)
            as keys and empty lists as values.
            Also returns a list of parameter names.'''
        res1 = {}
        res2 = {}
        names=[]
        group = self.temporary_group_selection_for_11(groupname)
        count = 2
        for i in range(0, count):
            temp = {group[i]['name']: []}
            res1.update(temp)
            res2.update(temp)
        res1.update(rep_dt = [])
        res2.update(rep_dt = [])
        for i in range(len(group)):
            names.append(group[i]['name'])
        names.insert(0,'rep_dt')
        return res1, res2, names
    
    def get_dict_and_list_according_to_groups(self, groupname):
        ''' Returns a dictionary with the name of the parameters (as per the group selection)
            as keys and empty lists as values.
            Also returns a list of parameter names.
        '''
        res1 = {}
        res2 = {}
        names=[]
        group = self.get_group_selection(groupname)
        count = self.get_number_of_group_members(groupname)
        for i in range(0, count):
            temp = {group[i]['name']: []}
            res1.update(temp)
            res2.update(temp)
        res1.update(rep_dt = [])
        res2.update(rep_dt = [])
        for i in range(len(group)):
            names.append(group[i]['name'])
        names.insert(0,'rep_dt')
        return res1, res2, names
    
    def get_overall_min_and_max(self, groupname, datadict):
        ''' Returns a dictionary with the block numbers as keys and a dictionary containing
            overall minimum and maximum of all variables according to blocks, as values.
        '''
        groups = self.get_group_selection(groupname)
        blocks = self.get_number_of_blocks(groupname)
        block_wise_datadict={}

        for block in blocks:
            minNum=None
            maxNum=None
            for group in range(len(groups)):
                for key in datadict.keys():
                    if groups[group]['block_number'] == block and key == groups[group]['name']:
                        try:
                            tempMin = min(datadict[key])
                            tempMax = max(datadict[key])
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
                            block_wise_datadict[block] = {'Min': minNum, 'Max': maxNum-minNum}    
                        except TypeError:
                            continue
                        except ValueError:
                            continue
        
        return block_wise_datadict

    
    def get_loaded_and_ballast_data(self, datadict, groupname):
        ''' Returns the loaded and ballast data along with the overall minimum and maximum of all the variables.'''
        maindbcollection = self.get_main_data()
        maindata = maindbcollection.find({'ship_imo': self.ship_imo})
        main_data = loads(dumps(maindata))
        groups = self.get_group_selection(groupname)
        block_wise_datadict = self.get_overall_min_and_max(groupname, datadict)
        # print(block_wise_datadict)

        result={}
        
        for key in datadict.keys():
            if key != 'rep_dt':
                ballast_list=[]
                loaded_list=[]
                # minNum = min(datadict[key]) if len(datadict[key]) > 0 else 0
                # ballast_list.append(str(minNum))
                # loaded_list.append(str(minNum))
                # maxNum = max(datadict[key])
                # minNum = min(datadict[key])
                for group in range(len(groups)):
                    if groups[group]['name'] == key:
                        block_num = groups[group]['block_number']
                        for i in range(len(main_data)):
                            if main_data[i]['vessel_loaded_check'] == 'Ballast':
                                try:
                                    ballast_list.append(block_wise_datadict[block_num]['Max'])
                                except IndexError:
                                    continue
                                except KeyError:
                                    continue
                            else:
                                try:
                                    ballast_list.append(0)
                                except KeyError:
                                    continue
                            if main_data[i]['vessel_loaded_check'] == 'Loaded':
                                try:
                                    loaded_list.append(block_wise_datadict[block_num]['Max'])
                                except IndexError:
                                    continue
                                except KeyError:
                                    continue
                            else:
                                try:
                                    loaded_list.append(0)
                                except KeyError:
                                    continue
                result[key] = {'Ballast': ballast_list, 'Loaded': loaded_list}

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
                if 'E' in singledata[var]['var_type'] and '1' not in singledata[var]['var_type']:
                    equipment_list.append(var)
                if 'P' in singledata[var]['var_type']:
                    parameter_list.append(var)
        # for var in singledata.keys():
        #     if singledata[var]['var_type'] == 'E':
        #         print(singledata[var])
        
        return equipment_list, parameter_list
    
    def check_for_nan_and_replace(self, data):
        dupli_data = data
        for var in dupli_data['processed_daily_data'].keys():
            for subvar in dupli_data['processed_daily_data'][var]:
                if subvar == 'predictions':
                    for i in range(0, 3):
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
                            if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m3'][i]) == True or dupli_data['processed_daily_data'][var][subvar]['ly_m3'][i] == -np.inf or dupli_data['processed_daily_data'][var][subvar]['ly_m3'][i] == np.inf:
                                dupli_data['processed_daily_data'][var][subvar]['ly_m3'].pop(i)
                                dupli_data['processed_daily_data'][var][subvar]['ly_m3'].insert(i, 'null')
                            if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m6'][i]) == True or dupli_data['processed_daily_data'][var][subvar]['ly_m6'][i] == -np.inf or dupli_data['processed_daily_data'][var][subvar]['ly_m6'][i] == np.inf:
                                dupli_data['processed_daily_data'][var][subvar]['ly_m6'].pop(i)
                                dupli_data['processed_daily_data'][var][subvar]['ly_m6'].insert(i, 'null')
                            if pd.isnull(dupli_data['processed_daily_data'][var][subvar]['ly_m12'][i]) == True or dupli_data['processed_daily_data'][var][subvar]['ly_m12'][i] == -np.inf or dupli_data['processed_daily_data'][var][subvar]['ly_m12'][i] == np.inf:
                                dupli_data['processed_daily_data'][var][subvar]['ly_m12'].pop(i)
                                dupli_data['processed_daily_data'][var][subvar]['ly_m12'].insert(i, 'null')
                        except TypeError:
                            continue
                elif subvar == 'crit_val_dynamic':
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

    ''' Functions for Interactive processing '''
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
                for key_2 in temp_data:
                    if key==key_2:
                        temp_data[key]=other_x[key]

            minValue = data[x1].min()
            maxValue = data[x1].max()
            x1_list=np.arange(minValue, maxValue, 0.1)
            
            reg=LinearRegression()
            poly = PolynomialFeatures(degree = 2)
            # pls_dataframe = pd.DataFrame(columns = ['lower', 'Pred', 'upper'])
            # print("EMPTY DATAFRAME", pls_dataframe)
            if not (np.any(pd.isnull(data[x_list])) and np.all(pd.isfinite(data[x_list]))):
                if not (np.any(pd.isnull(data[y])) and np.all(np.isfinite(data[y]))):
                    X_poly = poly.fit_transform(data[x_list])
                    poly.fit(X_poly, data[y])
                    reg.fit(X_poly, data[y])
            else:
                return "x_list or y has nan or infinite"
            pred_list=[]
            for i in x1_list:
                # x1_list.append(i)
                i = i.round(1)
                temp_data[x1]=[i]
                temp_dataframe=pd.DataFrame(temp_data)
                pred=reg.predict(poly.fit_transform(temp_dataframe))
                pred_list.append(pred[0])
            return x1_list.tolist(), pred_list
    
    def create_dataframe(self, X, Y, duration, Z=None, **other_X):
        maindb_collection = database.get_collection("Main_db")
        self.maindb = maindb_collection.find({"ship_imo": self.ship_imo}).sort('processed_daily_data.rep_dt.processed', ASCENDING)
        full_maindb = loads(dumps(self.maindb))
        maindb = self.create_maindb_according_to_duration(duration, full_maindb)
        X_list = []
        Y_list = []
        Z_list = []

        shortNameDict = self.create_short_names_dictionary()
        # variables = [i for i in other_X.keys()]
        # stats = self.get_ship_stats(maindb, *variables)
        dict_for_dataframe = {}
        for val_dict in maindb:
            for key in val_dict['processed_daily_data'].keys():
                if val_dict['processed_daily_data'][key]['identifier'].strip() == X:
                    X_list.append(val_dict['processed_daily_data'][key]['processed'])
                if val_dict['processed_daily_data'][key]['identifier'].strip() == Y:
                    Y_list.append(val_dict['processed_daily_data'][key]['processed'])
                if Z is not None:
                    if val_dict['processed_daily_data'][key]['identifier'].strip() == Z:
                        Z_list.append(val_dict['processed_daily_data'][key]['processed'])
        for key in other_X:
            if key != "None":
                tempList=[]
                for i in range(len(X_list)):
                    tempList.append(other_X[key])
                dict_for_dataframe[key] = tempList
        print(len(X_list), len(Y_list), len(Z_list))
        dict_for_dataframe[X] = X_list
        dict_for_dataframe[Y] = Y_list
        if len(Z_list) != 0:
            dict_for_dataframe[Z] = Z_list
        dataframe = pd.DataFrame(dict_for_dataframe)
        if Z is not None:
            X_name = shortNameDict[X]
            Y_name = shortNameDict[Y]
            Z_name = shortNameDict[Z]
            return dataframe, X_name, Y_name, Z_name, X_list, Y_list, Z_list
        else:
            X_name = shortNameDict[X]
            Y_name = shortNameDict[Y]
            return dataframe, X_name, Y_name, X_list, Y_list
    
    def create_maindb_according_to_duration(self, duration, main_db=[]):
        # print("\n",main_db)
        if main_db is not []:
            # print(loads(dumps(main_db)))
            # maindb = loads(dumps(main_db))
            maindb = main_db
            # print(maindb)
        else:
            maindb_collection = database.get_collection("Main_db")
            self.maindb = maindb_collection.find({"ship_imo": self.ship_imo}).sort('processed_daily_data.rep_dt.processed', ASCENDING)
            maindb = loads(dumps(self.maindb))
            print(len(maindb))
        number_of_days = self.get_duration(duration)
        
        current_date = maindb[len(maindb) - 1]['processed_daily_data']['rep_dt']['processed']
        if 'Year' in duration:
            days_to_subtract = timedelta(weeks=52)
        else:
            days_to_subtract = timedelta(days = number_of_days)
        dateList=[]
        actual_maindb=[]

        to_date = current_date - days_to_subtract

        for val_dict in range(len(maindb)):
            dateList.append(maindb[val_dict]['processed_daily_data']['rep_dt']['processed'])

        get_close_by_date = self.get_nearest_date(dateList, to_date)

        for val_dict in reversed(maindb):
            tempDate = val_dict['processed_daily_data']['rep_dt']['processed']
            if tempDate != get_close_by_date:
                actual_maindb.append(val_dict)
            else:
                break
        return actual_maindb
    
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
        for i in variables:
            tempList=[]
            for val_dict in range(len(data)):
                for key in data[val_dict]['processed_daily_data'].keys():
                    if data[val_dict]['processed_daily_data'][key]['identifier'].strip() == i:
                        tempList.append(data[val_dict]['processed_daily_data'][key]['processed'])
            data_dict[i] = tempList
        
        for key in data_dict.keys():
            try:
                minValue = min(data_dict[key])
                maxValue = max(data_dict[key])
                stats_dict[key] = {"Min": math.floor(minValue), "Max": math.ceil(maxValue)}
            except TypeError:
                continue
        return stats_dict
    
    def get_individual_parameters(self):
        ''' Gets all the parameters from the ship collection. Returns a list of dictionary created for react-select dropdown.
            Will have two keys - label, and value. label will be short name, value will be identifier new. 
        '''
        optionsList=[]
        singledata = self.ship_configs['data']

        for var in singledata.keys():
            try:
                if singledata[var]['dependent'] == False or singledata[var]['dependent'] == True:
                    temp = {'value': var.strip(), 'label': singledata[var]['short_names'].strip()}
                    optionsList.append(temp)
            except AttributeError:
                continue
        
        return optionsList




        
# obj=Configurator(9591301)
# obj.get_ship_configs()
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


