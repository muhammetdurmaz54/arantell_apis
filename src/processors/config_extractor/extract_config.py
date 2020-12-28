from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.helpers.check_status import check_status
from src.db.ship import Ship
import pandas as pd
import numpy as np

log = CommonLogger(__name__,debug=True).setup_logger()


class ConfigExtractor():

    def __init__(self,
                 ship_imo,
                 file):
        self.ship_imo = ship_imo
        self.file = file
        


    def do_steps(self):
        self.connect()
        self.read_files()
        self.process_file()
        inserted_id = self.write_configs()
        if self.error:
            return False, str(self.traceback_msg)
        else:
            return True, str(inserted_id)


    @check_status
    def connect(self):

        db = connect_db()

    @check_status
    def read_files(self):
        df_configurations = pd.read_excel(self.file, sheet_name='Configurations')
        df_variables = pd.read_excel(self.file, sheet_name='Variables')
        return df_configurations, df_variables

    @check_status
    def process_file(self):
        configurations, variables = self.read_files()
        limit = {}
        data_values = {}

        ship_imo = configurations['Value'][0]
        ship_name = configurations['Value'][1]
        ship_description = configurations['Value'][2]

        data_available_nav = list(variables[variables['Type']=='fuel']['Identifier NEW'])

        data_available_engine = list(variables[variables['Type']=='engine']['Identifier NEW'])

        identifier_mapping = dict(zip(variables[variables['Type'] != np.NaN]['Source Identifier'], variables[variables['Type'] != np.NaN]['Identifier NEW']))
        identifier_mapping = dict((k, v) for k, v in identifier_mapping.items() if not (type(k) == float and np.isnan(k)))

        for i in range(0, len(variables['Identifier NEW'])):   
            if variables['Type'][i] != 'static':               
                limit[variables['Identifier NEW'][i]] = {      
                    'type': variables['Limit Type'][i],
                    'min': variables['Limit Low'][i],
                    'max': variables['Limit High'][i]
                }
        
        limit = dict((k,v) for k, v in limit.items() if not (type(k) == float and np.isnan(k)))

        for i in range(0, len(variables['Identifier NEW'])):
            if variables['Type'][i] != 'static':
                data_values[variables['Identifier NEW'][i]] = {
                    'name': variables['Identifier NEW'][i],
                    'unit': variables['Units'][i],
                    'category': variables['Category'][i],
                    'subcategory': variables['SubCategory'][i],
                    'limits': limit[variables['Identifier NEW'][i]]
                }

        data_values = dict((k,v) for k, v in data_values.items() if not (type(k) == float and np.isnan(k)))

        data = dict(data_values)
        return ship_imo, ship_name, ship_description, data_available_nav, data_available_engine, identifier_mapping, data


        

    @check_status
    def write_configs(self):

        ship_imo, ship_name, ship_description, data_available_nav, data_available_engine, identifier_mapping, data = self.process_file()

        ship = Ship(
            ship_imo = ship_imo,
            ship_name = ship_name,
            ship_description = ship_description,
            data_available_nav = data_available_nav,
            data_available_engine = data_available_engine,
            identifier_mapping = identifier_mapping,
            data = data
        )

        ship.save()