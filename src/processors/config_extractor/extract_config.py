import sys

from pandas.core.dtypes.missing import isnull 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.helpers.check_status import check_status
from src.db.schema.ship import Ship
import pandas as pd
import numpy as np
from mongoengine import *
from pymongo import MongoClient
import os

log = CommonLogger(__name__,debug=True).setup_logger()

connect("aranti")
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
        self.df_variables = pd.read_excel(self.file, sheet_name='N&E')
        
    

    def stat(self,s):
        self.dest={}

        for i,row in self.df_variables.iterrows():
            
            for k in s:
                if row['Identifer NEW']==k:
                    self.dest[k]=row['Source Data']
        
        return self.dest


    def derived(self,identifier_new):
        identifier_new=identifier_new
        if identifier_new=="yes" or identifier_new=="YES":
            return True
        elif identifier_new==isnull or identifier_new=="NO" or identifier_new=="no":
            return False

    def availability(self,identifier_new):
        identifier_new=identifier_new
        if identifier_new==1:
            return True
        elif identifier_new==0 or identifier_new==isnull:
            return False


    @check_status
    def process_file(self):
        self.limit = {}
        

        self.ship_imo = self.df_configurations['Value'][0]
        self.ship_name = self.df_configurations['Value'][1]
        self.ship_description = self.df_configurations['Value'][2]
    
        self.data_available_nav = list(self.df_variables[self.df_variables['data_Type']=='N']['Identifer NEW'])
        self.data_available_nav=self.data_available_nav.__add__(list(self.df_variables[self.df_variables['data_Type']=='N+E']['Identifer NEW']))

        self.data_available_engine = list(self.df_variables[self.df_variables['data_Type']=='E']['Identifer NEW'])
        self.data_available_engine=self.data_available_engine.__add__(list(self.df_variables[self.df_variables['data_Type']=='N+E']['Identifer NEW']))

        self.nulli=self.df_variables[self.df_variables['Identifer NEW'] != np.NaN]
        #identifier_mapping = dict(zip(variables_file['Source Identifier'],variables_file['Identifer NEW']))
        self.identifier_mapping = dict(zip(self.nulli['Identifer NEW'],self.nulli['Source Identifier']))
        #identifier_mapping = dict((k,v) for k, v in identifier_mapping.items() if not (type(k) == float and np.isnan(k)))
        self.static = list(self.df_variables[self.df_variables['data_Type']=='static']['Identifer NEW'])
        

        for k, v in self.identifier_mapping.items():
            if type(v) == float and np.isnan(v):

                self.identifier_mapping[k]=k
        if(self.identifier_mapping[np.NaN]):
            del self.identifier_mapping[np.NaN]
        

        for i in range(0, len(self.df_variables['Identifer NEW'])):   #Fetches column Identifier_NEW from
            if self.df_variables['data_Type'][i] != 'static':               #variables_file checks if type is 'static'   #converts into dictionary
                self.limit[self.df_variables['Identifer NEW'][i]] = {  
                    
                    
                    'name':self.df_variables['Identifer NEW'][i],
                    'unit':self.df_variables['Units'][i],
                    'category':self.df_variables['Category'][i],
                    'subcategory':self.df_variables['SubCategory'][i],
                    'variable':self.df_variables['Variable'][i],
                    'input':self.df_variables['Input'][i],
                    'output':self.df_variables['Output'][i],
                    'var_type':self.df_variables['var_type'][i],
                    'identifier_old':self.df_variables['Identifer OLD'][i],
                    'Derived':self.derived(self.df_variables['Derived'][i]),
                    'Daily Availability':self.derived(self.df_variables['Daily Availability'][i]),
                    'availabe_for_groups':self.availability(self.df_variables['AVAILABLE FOR GROUPS'][i]),
                    'dependent':self.availability(self.df_variables['DEPENDENT?'][i]),
                    'group_selection':{
                        "groupnumber": 1,
                        "availability_code":1,
                        "block_number":1
                    },
                    'limits':{
                    'type': self.df_variables['Limit Type'][i],
                    'oplow': self.df_variables['OP Low'][i],
                    'ophigh': self.df_variables['OP High'][i],
                    'olmin': self.df_variables['OL LOW'][i],
                    'olmax': self.df_variables['OL High'][i]
                    
                    }
                }
           
        self.limit = dict((k,v) for k, v in self.limit.items() if not (type(k) == float and np.isnan(k)))

        

       


    
    @check_status
    def write_configs(self):
        

        ship = Ship(
            ship_imo = self.ship_imo,
            ship_name = self.ship_name,
            ship_description = self.ship_description,
            static_data=self.stat(self.static),
            data_available_nav = self.data_available_nav,
            data_available_engine = self.data_available_engine,
            identifier_mapping = self.identifier_mapping,
            data = self.limit
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


obj=ConfigExtractor(9591301,'F:\Afzal_cs\Internship\ConfiguratorRev_04 A.xlsx',True)
obj.read_files()
obj.process_file()

obj.write_configs()
