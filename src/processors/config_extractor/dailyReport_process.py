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



class DailyReportExtractor():
    def __init__(self, ship_imo, dateString=''):
        self.ship_imo = int(ship_imo)
        self.dateString = dateString
        
    
    ''' Uncomment later
    def connect(self):
        self.db = connect_db()
    '''
    
    def process_data(self):
        self.configuration = Configurator(self.ship_imo)
        self.ship_config = self.configuration.get_ship_configs()
        self.main_db = self.configuration.get_main_data()
        categoryList, categoryDict, column_headers = self.configuration.get_category_dict()
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
                '_id': 0
            }
        ).sort('processed_daily_data.rep_dt.processed', ASCENDING)
        res = loads(dumps(res))

        for i in res:
            newdate = i['processed_daily_data']['rep_dt']['processed'].strftime('%Y-%m-%d, %H:%M:%S')
            dateList.append(newdate)

        if self.dateString != '':
            latestResult = self.read_data_for_specific_date(self.dateString)
            latestRes = self.configuration.check_for_nan_and_replace(latestResult)
            issues = self.configuration.get_category_and_subcategory_with_issues(dateString=self.dateString)
            charter_party_values = self.configuration.get_daily_charter_party_values(dateString=self.dateString)
        else:
            # tempDateList = dateList.reverse()[0]
            latestResult = self.read_data_for_specific_date(dateList[len(dateList) - 1])
            latestRes = self.configuration.check_for_nan_and_replace(latestResult)
            issues = self.configuration.get_category_and_subcategory_with_issues("")
            charter_party_values = self.configuration.get_daily_charter_party_values('')
            
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
            subcategoryDictData[j] = temp
        subcategoryDictDataDecimal = self.configuration.get_decimal_control_on_daily_values(subcategoryDictData)

        for key in categoryDict['data'][0].keys():
            tempList=[]
            for i in range(0, len(categoryDict['data'][0][key])):
                for keyName in subcategoryDictDataDecimal.keys():
                    if categoryDict['data'][0][key][i] == keyName:
                        tempDict = { keyName: subcategoryDictDataDecimal[keyName]}
                        tempList.append(tempDict)
            categoryDictData[key] = tempList
        categoryDictData['VESSEL PARTICULARS'] = [{'Vessel Particulars': subcategoryDict['Vessel Particulars']}]

        # anomalyList = self.getAnomalyList(categoryDictData)
        
        # categoryDictDataDecimal = self.configuration.get_decimal_control_on_daily_values(categoryDictData)
        
        return categoryList, categoryDict, column_headers, subcategoryDict, dateList, categoryDictData, issues, static_data_for_charter_party, charter_party_values


        # return categoryDict, column_headers, subcategoryDict, dateList, latestRes
    
    def read_data_for_specific_date(self, date):
        findDate = datetime.strptime(date,"%Y-%m-%d, %H:%M:%S")
        res = self.main_db.find_one(
            {
                'ship_imo': self.ship_imo,
                'processed_daily_data.rep_dt.processed': findDate
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



# res = DailyReportExtractor(9591301)
# res.read_data_for_specific_date('2016-07-16, 12:00:00')
# res.process_data()

