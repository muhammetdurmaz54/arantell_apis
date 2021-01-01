from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.helpers.check_status import check_status
from src.db.schema.ship import Ship
import pandas as pd
import numpy as np

log = CommonLogger(__name__,debug=True).setup_logger()


class ConfigExtractor():

    def __init__(self,
                 ship_imo,
                 file,
                 override):
        self.ship_imo = ship_imo
        self.file = file
        self.override = override
        self.error = False
        self.traceback_msg = None

        


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

        self.db = connect_db()

    @check_status
    def read_files(self):
        self.df_configurations = pd.read_excel(self.file, sheet_name='Configurations')
        self.df_variables = pd.read_excel(self.file, sheet_name='Variables')

    @check_status
    def process_file(self):
        limit = {}
        data_values = {}

        self.ship_imo = self.df_configurations['Value'][0]
        self.ship_name = self.df_configurations['Value'][1]
        self.ship_description = self.df_configurations['Value'][2]

        self.data_available_nav = list(self.df_variables[self.df_variables['Type']=='fuel']['IdentifierNEW'])

        self.data_available_engine = list(self.df_variables[self.df_variables['Type']=='engine']['IdentifierNEW'])

        self.identifier_mapping = dict(zip(self.df_variables[self.df_variables['Type'] != np.NaN]['SourceIdentifier'], self.df_variables[self.df_variables['Type'] != np.NaN]['IdentifierNEW']))
        self.identifier_mapping = dict((k, v) for k, v in self.identifier_mapping.items() if not (type(k) == float and np.isnan(k)))

        

        for row in self.df_variables.itertuples():
	        if row.Type == 'static':
		        continue
	        else:
		        limit[row.IdentifierNEW] = {
                    'type': row.LimitType,
                    'min': row.LimitLow,
                    'max': row.LimitHigh
                }
        
        limit = dict((k,v) for k, v in limit.items() if not (type(k) == float and np.isnan(k)))

        for row in self.df_variables.itertuples():
            if row.Type == 'static':
                continue
            else:
                data_values[row.IdentifierNEW] = {
                    'name': row.IdentifierNEW,
                    'unit': row.Units,
                    'category': row.Category,
                    'subcategory': row.SubCategory,
                    'limits': limit[row.IdentifierNEW]
                }

        data_values = dict((k,v) for k, v in data_values.items() if not (type(k) == float and np.isnan(k)))

        self.data = dict(data_values)


    
    @check_status
    def write_configs(self):


        ship = Ship(
            ship_imo = self.ship_imo,
            ship_name = self.ship_name,
            ship_description = self.ship_description,
            data_available_nav = self.data_available_nav,
            data_available_engine = self.data_available_engine,
            identifier_mapping = self.identifier_mapping,
            data = self.data
        )

        if self.override:
            if not Ship.objects(ship_imo = self.ship_imo):
                ship.save()
            else:
                Ship.objects.get(ship_imo = self.ship_imo).delete()
                ship.save()
        else:
            if not Ship.objects(ship_imo = self.ship_imo):
                ship.save()
            else:
                return "Record already exists!"