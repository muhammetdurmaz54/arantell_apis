import sys
from time import process_time_ns
# sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.processors.config_extractor.configurator import Configurator
from src.processors.config_extractor.vti_process import vti_process
from src.helpers.check_status import check_status
from flask import request,jsonify
from pymongo import DESCENDING, MongoClient, ASCENDING
from bson.json_util import dumps, loads
import numpy as np
import pandas as pd
from datetime import datetime

log = CommonLogger(__name__,debug=True).setup_logger()



class DailyReportExtractor():
    def __init__(self, ship_imo, id, dateString=''):
        self.ship_imo = int(ship_imo)
        self.dateString = dateString
        self.id = id
        
    
    ''' Uncomment later
    def connect(self):
        self.db = connect_db()
    '''
    
    def process_data(self):
        self.configuration = Configurator(self.ship_imo)
        self.ship_config = self.configuration.get_ship_configs()
        self.main_db = self.configuration.get_main_data()
        categoryList, categoryDict, column_headers = self.configuration.get_category_dict("dailydata")
        subcategoryDict = self.configuration.get_subcategory_dict()
        # shortNameDict = self.configuration.create_short_names_dictionary()
        equipment_list, parameter_list = self.configuration.get_Equipment_and_Parameter_list()
        static_data_for_charter_party = self.configuration.get_static_data_for_charter_party()
        
        # anomalyList=[]
        dateList=[]
        subcategoryDictData={}
        categoryDictData={}
        latestRes=None

        res = self.main_db.find(
            {
                'ship_imo': self.ship_imo
            },
            {
                'processed_daily_data.rep_dt.processed': 1,
                'processed_daily_data.source.processed': 1,
                'final_rep_dt': 1,
                '_id': 0
            }
        ).sort('final_rep_dt', ASCENDING)
        res = loads(dumps(res))

        for i in res:
            # newdate = i['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d, %H:%M:%S')
            newdate = i['final_rep_dt'].strftime('%Y-%m-%d, %H:%M:%S')
            dateList.append(newdate)

        if self.dateString != '':
            latestResult = self.read_data_for_specific_date(self.dateString)
            latestRes = self.configuration.check_for_nan_and_replace(latestResult)
            issues, issuesCount = self.configuration.get_category_and_subcategory_with_issues(self.dateString, self.id, self.ship_imo)
            charter_party_values, charter_party_prediction_values = self.configuration.get_daily_charter_party_values(dateString=self.dateString)
            compliance_messages = self.configuration.create_dict_of_compliance_messages(dateString=self.dateString)
        else:
            # tempDateList = dateList.reverse()[0]
            latestResult = self.read_data_for_specific_date(dateList[len(dateList) - 1])
            latestRes = self.configuration.check_for_nan_and_replace(latestResult)
            issues, issuesCount = self.configuration.get_category_and_subcategory_with_issues("", self.id, self.ship_imo)
            charter_party_values, charter_party_prediction_values = self.configuration.get_daily_charter_party_values('')
            compliance_messages = self.configuration.create_dict_of_compliance_messages('')
            
            # print(issues)

        for j in subcategoryDict.keys():
            temp=[]
            for k in range(0, len(subcategoryDict[j])):
                if j == 'Vessel Particulars':
                    continue
                for i in latestRes['processed_daily_data'].keys():
                    if i in subcategoryDict[j][k].keys():
                        if i in parameter_list:
                            # nameDict = {'short_name': shortNameDict[i]}
                            # latestRes['processed_daily_data'][i].update(nameDict)
                            temp.append(latestRes['processed_daily_data'][i])
                for i in latestRes['Equipment'].keys():
                    if i in subcategoryDict[j][k].keys():
                        if i in equipment_list:
                            # typeDict = {'var_type': 'E'}
                            # latestRes['processed_daily_data'][i].update(typeDict)
                            temp.append(latestRes['Equipment'][i])
            if len(temp) > 0:
                subcategoryDictData[j] = temp
        subcategoryDictDataDecimal = self.configuration.get_decimal_control_on_daily_values(subcategoryDictData)

        for key in categoryDict['data'][0].keys():
            tempList=[]
            for i in range(0, len(categoryDict['data'][0][key])):
                for keyName in subcategoryDictDataDecimal.keys():
                    if categoryDict['data'][0][key][i] == keyName:
                        tempDict = { keyName: subcategoryDictDataDecimal[keyName]}
                        tempList.append(tempDict)
            if len(tempList) > 0:
                categoryDictData[key] = tempList
        newVesselParticulars={}
        for key in subcategoryDict['Vessel Particulars'].keys():
            if subcategoryDict['Vessel Particulars'][key]['name'] != "" and subcategoryDict['Vessel Particulars'][key]['value'] != "":
                newVesselParticulars[key] = subcategoryDict['Vessel Particulars'][key]
        categoryDictData['VESSEL PARTICULARS'] = [{'Vessel Particulars': newVesselParticulars}]

        # print(categoryDictData)
        listOfVesselParticularKeys = list(newVesselParticulars.keys())

        # anomalyList = self.getAnomalyList(categoryDictData)
        
        # categoryDictDataDecimal = self.configuration.get_decimal_control_on_daily_values(categoryDictData)
        new_category_dict = self.get_corrected_daily_data(categoryDict=categoryDict, categoryDictData=categoryDictData)
        # print(new_category_dict)
        latest_vti,latest_avg_vti= vti_process(int(self.ship_imo))
        # print("resssssssssssssssssssssssssssssssssssssssssssssssss",latestResult)
        if 'source' in latestResult['processed_daily_data']:
            source=latestResult['processed_daily_data']['source']['processed']
            if pd.isnull(source):
                source=None
        else:
            source=None
        # print("resssssssssssssssssssssssssssssssssssssssssssssssss",source)
        return categoryList, new_category_dict, column_headers, subcategoryDict, dateList, categoryDictData, issues, static_data_for_charter_party, charter_party_values, charter_party_prediction_values, compliance_messages, listOfVesselParticularKeys, issuesCount, latest_vti, latest_avg_vti,source


        # return categoryDict, column_headers, subcategoryDict, dateList, latestRes
    
    def read_data_for_specific_date(self, date):
        findDate = datetime.strptime(date,"%Y-%m-%d, %H:%M:%S")
        res = self.main_db.find_one(
            {
                'ship_imo': self.ship_imo,
                # 'processed_daily_data.rep_dt.processed': findDate
                'final_rep_dt': findDate
            },
            {
                '_id': 0
            }
        )
        res = loads(dumps(res))
        return res
    
    def getAnomalyList(self, data):
        ''' Returns the list of all the category names that have outliers.'''
        result=[]
        for category in data.keys():
            for subcategoryObj in range(0, len(data[category])):
                for subcategory in data[category][subcategoryObj].keys():
                    for item in data[category][subcategoryObj][subcategory]:
                        if 'within_outlier_limits' in item:
                            if item['within_outlier_limits']['m3'] == False:
                                result.append(category)
                        else:
                            continue
        
        return result

    def get_corrected_daily_data(self, categoryDict, categoryDictData):
        ''' Returns category dictionary after removing all the categories and
            subcategories that don't have values.
        '''
        new_category_dict={'data': []}
        category_dict={}
        for category in categoryDictData.keys():
            subcategory_list=[]
            for subcategory_dict_list in categoryDictData[category]:
                tempKeyList = list(subcategory_dict_list.keys())
                subcategory_list.extend(tempKeyList)
                # tempDict = {category: subcategory_list}
            category_dict[category] = subcategory_list
            # new_category_dict['data'].append({category: subcategory_list})
        
        new_category_dict['data'].append(category_dict)

        return new_category_dict



# res = DailyReportExtractor(9591301)
# res.read_data_for_specific_date('2016-07-16, 12:00:00')
# res.process_data()

