# react application backend for generating report based on selected period

from src.processors.config_extractor.configurator import Configurator
from datetime import datetime
import pandas as pd
from pymongo import ASCENDING

class SelectedPeriod:
    def __init__(self, ship_imo, fromDate="", toDate=""):
        self.ship_imo = int(ship_imo)
        self.fromDate = fromDate
        self.toDate = toDate
    
    def process_data(self):
        # configuration = Configurator(self.ship_imo)
        # short_names = configuration.create_short_names_dictionary()
        result = self.get_list_of_parameters_with_issues()
        outlier_result, operational_result, spe_result = self.get_expected_and_messages(result)
        outliers = self.get_outlier_values(result['Outliers'])
        operationals = self.get_operational_values(result['Operational'])
        spe = self.get_spe_values(result['SPE'])

        return outliers, operationals, spe, outlier_result, operational_result, spe_result
    
    def get_available_dates(self):
        '''
            Gets the list of available dates for a particular ship.
        '''

        main_db_collection = self.get_main_db_collection()
        dates=[]

        for doc in main_db_collection.find({'ship_imo': self.ship_imo}, {'final_rep_dt'}).sort('final_rep_dt', ASCENDING):
            dates.append(doc['final_rep_dt'].strftime("%Y-%m-%d"))
        
        return dates
    
    def get_main_db_collection(self):
        '''
            Get Main Db.
        '''
        configuration = Configurator(int(self.ship_imo))
        main_db = configuration.get_main_data()

        return main_db

    
    def get_list_of_parameters_with_issues(self):
        '''
            Gets the list of parameters with issues.
        '''

        main_db_collection = self.get_main_db_collection()
        result = {
            'Outliers': {},
            'Operational': {},
            'SPE': {}
        }
        
        from_date = datetime.strptime(self.fromDate, "%Y-%m-%d")
        to_date = datetime.strptime(self.toDate, "%Y-%m-%d")

        for doc in main_db_collection.find({'ship_imo': self.ship_imo,
                                            'final_rep_dt': {'$gte': from_date, '$lte': to_date}
                                            }):
            outliers = []
            operational = []
            spe = []
            for key in doc['processed_daily_data'].keys():
                if doc['processed_daily_data'][key]['within_outlier_limits']['m12'] == False:
                    outliers.append(key)
                elif doc['processed_daily_data'][key]['within_operational_limits']['m12'] == False:
                    operational.append(key)
                elif ('spe_messages' in doc['processed_daily_data'][key] and
                      'm12' in doc['processed_daily_data'][key]['spe_messages'] and
                      pd.isnull(doc['processed_daily_data'][key]['spe_messages']['m12'][2]) == False):
                    spe.append(key)
            result['Outliers'][doc['final_rep_dt']] = outliers
            result['Operational'][doc['final_rep_dt']] = operational
            result['SPE'][doc['final_rep_dt']] = spe
        
        return result
    
    def get_outlier_values(self, outlier_params):
        '''
            Get the values of the parameters that are Outliers.
        '''

        main_db_collection = self.get_main_db_collection()
        configuration = Configurator(self.ship_imo)
        short_names = configuration.create_short_names_dictionary()
        result = []

        for date in outlier_params.keys():
            outlier_values = []
            for param in outlier_params[date]:
                name = short_names[param] if short_names[param] else param
                for doc in main_db_collection.find({'ship_imo': self.ship_imo,
                                                    'final_rep_dt': date
                                                    },
                                                    {'processed_daily_data.'+param+'.processed': 1}):
                    outlier_values.append({name: configuration.makeDecimal(doc['processed_daily_data'][param]['processed'])})
            result.append({date.strftime('%Y-%m-%d'): outlier_values})
        
        return result
    
    def get_operational_values(self, operational_params):
        '''
            Get the values of the parameters that are beyond Operational limits.
        '''

        main_db_collection = self.get_main_db_collection()
        configuration = Configurator(self.ship_imo)
        short_names = configuration.create_short_names_dictionary()
        result = []

        for date in operational_params.keys():
            operational_values = []
            for param in operational_params[date]:
                name = short_names[param] if short_names[param] else param
                for doc in main_db_collection.find({'ship_imo': self.ship_imo,
                                                    'final_rep_dt': date
                                                    },
                                                    {'processed_daily_data.'+param+'.processed': 1}):
                    operational_values.append({name: configuration.makeDecimal(doc['processed_daily_data'][param]['processed'])})
            result.append({date.strftime('%Y-%m-%d'): operational_values})
        
        return result
    
    def get_spe_values(self, spe_params):
        '''
            Get the values of the parameters that are Anomalies.
        '''

        main_db_collection = self.get_main_db_collection()
        configuration = Configurator(self.ship_imo)
        ship_config_collection = configuration.get_ship_configs()
        short_names = configuration.create_short_names_dictionary()
        result = []
        spe_param_limits = {}

        for date in spe_params.keys():
            for param in spe_params[date]:
                for doc in ship_config_collection.find({'ship_imo': self.ship_imo}, {'spe_limits.'+param}):
                    spe_param_limits[param] = doc['spe_limits'][param]['m12']['zero_zero_five']

        for date in spe_params.keys():
            spe_values = []
            for param in spe_params[date]:
                name = short_names[param] if short_names[param] else param
                for doc in main_db_collection.find({'ship_imo': self.ship_imo,
                                                    'final_rep_dt': date
                                                    },
                                                    {'processed_daily_data.'+param: 1}):
                    new_spe = configuration.makeDecimal(configuration.spe_divide(doc['processed_daily_data'][param]['SPEy']['m12'], spe_param_limits[param]))
                    spe_values.append({name: new_spe})
            result.append({date.strftime('%Y-%m-%d'): spe_values})
        
        return result
    
    def get_expected_and_messages(self, params_with_issues):
        '''
            Gets the expected values of the parameters in
            `params_with_issues` between fromDate and toDate.
        '''

        main_db_collection = self.get_main_db_collection()
        configuration = Configurator(self.ship_imo)
        outlier_result = []
        operational_result = []
        spe_result = []

        for date in params_with_issues['Outliers'].keys():
            tempList=[]
            for param in params_with_issues['Outliers'][date]:
                for doc in main_db_collection.find({'ship_imo': self.ship_imo,
                                                    'final_rep_dt': date},
                                                    {'processed_daily_data.'+param: 1}).sort('final_rep_dt', ASCENDING):
                    try:
                        exp = configuration.makeDecimal(doc['processed_daily_data'][param]['predictions']['m12'][1])
                    except:
                        exp = "-"
                    temp = {
                        doc['processed_daily_data'][param]['name']: [exp,
                                                                    doc['processed_daily_data'][param]['outlier_limit_msg']['m12']]
                    }
                tempList.append(temp)
            outlier_result.append({date.strftime("%Y-%m-%d"): tempList})
        
        for date in params_with_issues['Operational'].keys():
            tempList=[]
            for param in params_with_issues['Operational'][date]:
                for doc in main_db_collection.find({'ship_imo': self.ship_imo,
                                                    'final_rep_dt': date},
                                                    {'processed_daily_data.'+param: 1}).sort('final_rep_dt', ASCENDING):
                    try:
                        exp = configuration.makeDecimal(doc['processed_daily_data'][param]['predictions']['m12'][1])
                    except:
                        exp = "-"
                    temp = {
                        doc['processed_daily_data'][param]['name']: [exp,
                                                                    doc['processed_daily_data'][param]['operational_limit_msg']['m12']]
                    }
                tempList.append(temp)
            operational_result.append({date.strftime("%Y-%m-%d"): tempList})
        
        for date in params_with_issues['SPE'].keys():
            tempList=[]
            for param in params_with_issues['SPE'][date]:
                for doc in main_db_collection.find({'ship_imo': self.ship_imo,
                                                    'final_rep_dt': date},
                                                    {'processed_daily_data.'+param: 1}).sort('final_rep_dt', ASCENDING):
                    try:
                        exp = configuration.makeDecimal(1/0.8)
                    except:
                        exp = "-"
                    temp = {
                        doc['processed_daily_data'][param]['name']: [exp,
                                                                     doc['processed_daily_data'][param]['spe_messages']['m12'][2]]
                    }
                tempList.append(temp)
            spe_result.append({date.strftime("%Y-%m-%d"): tempList})
        
        return outlier_result, operational_result, spe_result
