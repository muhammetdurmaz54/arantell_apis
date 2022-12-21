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
import pandas as pd

log = CommonLogger(__name__,debug=True).setup_logger()



class DailyIssueExtractor():
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

        # print("ISSUES", categoryDictData)
        newCategoryDictData = self.create_only_issues(categoryDictData)

        listOfVesselParticularKeys = list(newVesselParticulars.keys())

        # anomalyList = self.getAnomalyList(categoryDictData)
        
        # categoryDictDataDecimal = self.configuration.get_decimal_control_on_daily_values(categoryDictData)
        new_category_dict = self.get_corrected_daily_data(categoryDict=categoryDict, categoryDictData=newCategoryDictData)

        # print(new_category_dict)
        
        return categoryList, new_category_dict, column_headers, subcategoryDict, dateList, newCategoryDictData, issues, static_data_for_charter_party, charter_party_values, charter_party_prediction_values, compliance_messages, listOfVesselParticularKeys, issuesCount


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
    
    def create_only_issues(self, categoryDictData):
        '''
            Create dictionary containing only the issues of ship
        '''

        issues_dict = categoryDictData.copy()

        for category in issues_dict.keys():
            # print("CATEGORY ", category)
            if category != 'VESSEL PARTICULARS':
                for i in range(0, len(issues_dict[category])):
                    # print("i ", i)
                    for subcategory in issues_dict[category][i].keys():
                        # print("SUBCATEGORY ", subcategory)
                        # j = len(issues_dict[category][i][subcategory]) - 1
                        j = 0
                        # length_of_subcategory = len(issues_dict[category][i][subcategory])
                        index_list = []
                        # print(len(issues_dict[category][i][subcategory]))
                        # print(j)
                        while j < len(issues_dict[category][i][subcategory]):
                            print('ID ', issues_dict[category][i][subcategory][j]['identifier'])
                            # if issues_dict[category][i][subcategory][j]['identifier'] == 'real_slip_calc':
                            #     print(issues_dict[category][i][subcategory][j])
                            try:
                                # within_outlier_limits_flag = False
                                # within_operational_flag = False

                                # if issues_dict[category][i][subcategory][j]['within_outlier_limits']['m6'] == False:
                                #     index_list.append(j)
                                #     within_outlier_limits_flag = True
                                # elif within_outlier_limits_flag == False and issues_dict[category][i][subcategory][j]['within_operational_limits']['m6'] == False:
                                #     index_list.append(j)
                                #     within_operational_flag = True
                                # elif within_outlier_limits_flag == False and within_operational_flag == False and pd.isnull(issues_dict[category][i][subcategory][j]['spe_messages']['m6'][2]) == True:
                                #     index_list.append(j)

                                # if pd.isnull(issues_dict[category][i][subcategory][j]['within_outlier_limits']['m6']) == False:
                                if issues_dict[category][i][subcategory][j]['within_outlier_limits']['m6'] == False:
                                    index_list.append(j)
                                    print("WITHIN OUTLIER")
                                    # within_outlier_limits_flag = True
                                
                                # elif within_outlier_limits_flag == False and pd.isnull(issues_dict[category][i][subcategory][j]['within_operational_limits']['m6']) == False:
                                elif issues_dict[category][i][subcategory][j]['within_operational_limits']['m6'] == False:
                                    index_list.append(j)
                                    print("WITHIN OPERATIONAL")
                                    # within_operational_flag = True
                                elif pd.isnull(issues_dict[category][i][subcategory][j]['spe_messages']['m6'][2]) == False:
                                    index_list.append(j)
                                    print("SPE MESSAGE")
                            except KeyError:
                                # index_list.append(j)
                                # continue
                                print("EXCEPT")
                            j = j + 1
                        
                        new_subcategory_list = []
                        for k in range(0, len(issues_dict[category][i][subcategory])):
                            if k not in index_list:
                            # if k in index_list:
                                continue
                            else:
                                new_subcategory_list.append(issues_dict[category][i][subcategory][k])
                        # print('NEW LIST ', new_subcategory_list)
                        issues_dict[category][i][subcategory] = new_subcategory_list

        # Getting rid of the subcategories and categories that are empty
        for category in issues_dict.keys():
            if category != 'VESSEL PARTICULARS':
                if len(issues_dict[category]) == 0:
                    # print("IF IN RID")
                    del issues_dict[category]
                else:
                    print(category)
                    for i in issues_dict[category]:
                        for subcategory in i.copy().keys():
                            if len(i[subcategory]) == 0:
                                # print("IF IN ELSE IN RID")
                                del i[subcategory]
                    
                    tempdictarray = list(filter(None, issues_dict[category]))
                    print(tempdictarray)
                    issues_dict[category] = tempdictarray

        for category in issues_dict.copy():            
            if all(len(list(x.keys())) == 0 for x in issues_dict[category]):
                del issues_dict[category]
            
        
        return issues_dict





# res = DailyReportExtractor(9591301)
# res.read_data_for_specific_date('2016-07-16, 12:00:00')
# res.process_data()

