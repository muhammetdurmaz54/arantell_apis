import sys
from numpy.core.numeric import outer
from pandas.core.algorithms import factorize

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
# client = MongoClient("mongodb://localhost:27017/aranti")
# db=client.get_database("aranti")
    


# database = db




# connect("aranti")
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
        self.anamoly()
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
        self.df_groups = pd.read_excel(self.file, sheet_name="Groups", header=[0, 1])
        # Convert the headers and sub-headers in the dataframe to MultiIndex headers of the form ('Header', 'SubHeader')
        a = self.df_groups.columns.get_level_values(0).to_series()
        b = a.mask(a.str.startswith('Unnamed')).ffill().fillna('')
        self.df_groups.columns = [b, self.df_groups.columns.get_level_values(1)]
        self.grpnames = pd.read_excel(self.file, sheet_name="GrpDir", engine='openpyxl')
        self.mlcontrol=pd.read_excel(self.file, sheet_name='MLcontrol',skiprows = [0, 1, 2])
        self.anamoly_messages=pd.read_excel(self.file, sheet_name='AnamolyMessages')
        
        

    def stat(self,s):
        self.dest={}
        
        for i,row in self.df_variables.iterrows():
            
            for k in s:
                if row['Identifer NEW']==k:
                    self.dest_2={}
                    self.dest_2['name']=row['Short Names']
                    self.dest_2['value']=row['Static Data']
                    self.dest[k]=self.dest_2
       
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
    
    # Function to get the group selection
    def get_group_selection(self):
        groupsData=[]
        # List of the variables in identifier new other than the ones with data type static
        # Added for convenience
        id_new_1=[self.df_variables['Identifer NEW'][i] for i in range(0, len(self.df_variables['Identifer NEW'])) if self.df_variables['Data Type'][i] != 'static']
        id_new=[]
        for i in id_new_1:
            if pd.isnull(i)==False:
                i=i.strip()
                id_new.append(i)
        # Get the group selection list of dictionaries
        for i in range(0, len(self.df_groups[('', 'id_new')])):
            if pd.isnull(self.df_groups[('', 'id_new')][i])==False:
                ak=self.df_groups[('', 'id_new')][i].strip()
                if ak in id_new:
                    for j in self.df_groups.iloc[:, 4:]:
                        new_index = int(j[0].replace('Group', '')) - 1
                        if self.df_groups[j][i] > 0:
                            newdict = {
                                'name': self.df_groups[('', 'id_new')][i], #Added for convenience
                                'groupname': self.grpnames['Name'][new_index],
                                'groupnumber': j[0].replace('Group',''),
                                'group_availability_code': self.df_groups[j][i],
                                'block_number': self.df_groups[(j[0], 'BLOCK NO')][i]
                            }
                            groupsData.append(newdict)
                                
        del groupsData[1::2]
        
        return groupsData

    def get_ml_list(self):
        ml_dict={}
        
        for i,row in self.mlcontrol.iterrows():
            column_list=[]
            for column in self.mlcontrol.columns:
                if self.mlcontrol[column][i]==1:
                    if i>=8:
                        column_list.append(column)
            
                    ml_dict[self.mlcontrol['Identifier New'][i]]=column_list
        return ml_dict
             
                
    #temporary for stripping category                   
    def category(self,x):
        if pd.isnull(x)==False:
            val=x.strip()
            return val

    def anamoly_dict(self,i):
        temp_dict ={'type_of_anamoly':self.anamoly_messages['Type of anamoly'][i],
                'param/equip':self.anamoly_messages['PARAM /EQPT'][i],
                'alpha':self.anamoly_messages['Alpha'][i],
                'intensity':self.anamoly_messages['IntensityNo'][i],
                'message':self.anamoly_messages['Message'][i],
                'type_of_param':self.anamoly_messages['Type of Param'][i],
                'Single /Group':self.anamoly_messages['Single /Group'][i]}
        return temp_dict

    def anamoly(self):
        self.parameter_anamoly={}
        self.equipment_anamoly={}
        self.outlier_anamoly={}
        for i in range(0,len(self.anamoly_messages['PARAM /EQPT'])):
            if self.anamoly_messages['PARAM /EQPT'][i]=='P' or self.anamoly_messages['PARAM /EQPT'][i]=='p':
                self.parameter_anamoly[self.anamoly_messages['Type of anamoly'][i]]=self.anamoly_dict(i)
            
            if self.anamoly_messages['PARAM /EQPT'][i]=='E/E1' or self.anamoly_messages['PARAM /EQPT'][i]=='e/e1':
                self.equipment_anamoly[self.anamoly_messages['Type of anamoly'][i]]=self.anamoly_dict(i)

            if pd.isnull(self.anamoly_messages['PARAM /EQPT'][i])==True or self.anamoly_messages['PARAM /EQPT'][i]=='':
                self.outlier_anamoly[self.anamoly_messages['Type of anamoly'][i]]=self.anamoly_dict(i)
    

    def input_output(self,input):
        spl=None
        if pd.isnull(input)==False:
            input=input.replace(',','  ')
            spl=input.split()
        return spl
    
    def sister_list(self,input):
        spl=None
        if pd.isnull(input)==False:
            input=input.replace(',','  ')
            spl=input.split()
        for i in range(0,len(spl)):
            spl[i]=int(spl[i])
        return spl
        #     for i in spl:
        #         i=i.replace(',','  ')
        #         i=str(i)
        #         print(i)
        #         list_var.append(i)
        # print(list_var)
        # print(len(list_var))
        # exit()



    def create_indices(self):
        indices={}
        for i in range(0, len(self.df_variables['Identifer NEW'])):   #Fetches column Identifier_NEW from
            if pd.isnull(self.df_variables['Param or Eqpt'][i])==False and type(self.df_variables['Param or Eqpt'][i])==str and pd.isnull(self.df_variables['Identifer NEW'][i])==False:
                if self.df_variables['Param or Eqpt'][i].strip() == 'T2&SPE' or self.df_variables['Param or Eqpt'][i].strip() == 'INDX':
                    indices[self.df_variables['Identifer NEW'][i]]={
                        'name':self.df_variables['Identifer NEW'][i],
                        'unit':self.df_variables['Units'][i],
                        'category':self.category(self.df_variables['Category'][i]),
                        'subcategory':self.df_variables['SubCategory'][i],
                        'variable':self.df_variables['Variable'][i],
                        'short_names':self.df_variables['Short Names'][i],
                        'source_idetifier':self.df_variables['Source Identifier'][i],
                        'static_data': self.df_variables['Static Data'][i],
                        'input':self.input_output(self.df_variables['Input'][i]),
                        'output':self.input_output(self.df_variables['Output'][i]),
                        'var_type':self.df_variables['Param or Eqpt'][i],    #p=parameter, E=equipment, E1=psuedo equipment or notional equipment
                        'identifier_old':self.df_variables['Identifer OLD'][i],
                        'Derived':self.derived(self.df_variables['Derived'][i]),
                        'Daily Availability':self.derived(self.df_variables['Daily Availability'][i]),
                        'availabe_for_groups':self.availability(self.df_variables['AVAILABLE FOR GROUPS'][i]),
                        'dependent':self.availability(self.df_variables['DEPENDENT?'][i]),
                               #create funtion to get groups if 1 it is single parameter,if 2 split and look for 20,if 3 split and look for 30 ....
                        'limits':{
                        'type': self.df_variables['Limit Type'][i],
                        'oplow': self.df_variables['OP Low'][i],
                        'ophigh': self.df_variables['OP High'][i],
                        'olmin': self.df_variables['OL LOW'][i],
                        'olmax': self.df_variables['OL High'][i]                  
                    }
                }
                
        return indices 

    # @check_status
    def process_file(self):
        self.data = {}
        # get group selection
        self.groupsData = self.get_group_selection()
        

        self.ship_imo = self.df_configurations['Value'][0]
        self.ship_name = self.df_configurations['Value'][1]
        self.ship_description = self.df_configurations['Value'][2]
        self.sister_vessel_list=self.sister_list(self.df_configurations['Value'][3])
        self.similar_vessel_list=self.sister_list(self.df_configurations['Value'][4])
        
    
        self.data_available_nav = list(self.df_variables[self.df_variables['Data Type']=='N']['Identifer NEW'].str.strip())
        self.data_available_nav=self.data_available_nav.__add__(list(self.df_variables[self.df_variables['Data Type']=='N+E']['Identifer NEW']))

        self.data_available_engine = list(self.df_variables[self.df_variables['Data Type']=='E']['Identifer NEW'].str.strip())
        self.data_available_engine=self.data_available_engine.__add__(list(self.df_variables[self.df_variables['Data Type']=='N+E']['Identifer NEW']))

        self.nulli=self.df_variables[self.df_variables['Identifer NEW'] != np.NaN]
        #identifier_mapping = dict(zip(variables_file['Source Identifier'],variables_file['Identifer NEW']))
        self.identifier_mapping = dict(zip(self.nulli['Identifer NEW'],self.nulli['Source Identifier']))
        #identifier_mapping = dict((k,v) for k, v in identifier_mapping.items() if not (type(k) == float and np.isnan(k)))
        self.static = list(self.df_variables[self.df_variables['Data Type']=='static']['Identifer NEW'])    
        self.ais_api=list(self.df_variables[self.df_variables['Data Type']=='AIS/API']['Identifer NEW'])    
        self.ais_api=self.ais_api.__add__(list(self.df_variables[self.df_variables['Data Type']=='API/AIS']['Identifer NEW']))
        self.ais_api=self.ais_api.__add__(list(self.df_variables[self.df_variables['Data Type']=='API']['Identifer NEW']))
        self.ais_api=self.ais_api.__add__(list(self.df_variables[self.df_variables['Data Type']=='AIS']['Identifer NEW']))
        self.calculated=list(self.df_variables[self.df_variables['Derived'].str.strip()=='YES']['Identifer NEW']) 

        for k, v in self.identifier_mapping.items():
            if type(v) == float and np.isnan(v):
                self.identifier_mapping[k]=str(k).strip()
        if(self.identifier_mapping[np.NaN]):
            del self.identifier_mapping[np.NaN]       
        self.identifier_mapping = { x.translate({32:None}) : y for x, y in self.identifier_mapping.items()}
        

        for i in range(0, len(self.df_variables['Identifer NEW'])):   #Fetches column Identifier_NEW from
            if self.df_variables['Data Type'][i] != 'static':#variables_file checks if type is 'static'   #converts into dictionary
                self.newList = []
                # Only add specific groups
                for elem in self.groupsData:
                    if pd.isnull(self.df_variables['Identifer NEW'][i])==False:
                        if elem['name'] == self.df_variables['Identifer NEW'][i].strip():
                            self.newList.append(elem)
               
                self.data[self.df_variables['Identifer NEW'][i]] = {  
                    
                    
                    'name':self.df_variables['Identifer NEW'][i],
                    'unit':self.df_variables['Units'][i],
                    'category':self.category(self.df_variables['Category'][i]),
                    'subcategory':self.df_variables['SubCategory'][i],
                    'variable':self.df_variables['Variable'][i],
                    'short_names':self.df_variables['Short Names'][i],
                    'source_idetifier':self.df_variables['Source Identifier'][i],
                    'static_data': self.df_variables['Static Data'][i],
                    'input':self.df_variables['Input'][i],
                    'output':self.df_variables['Output'][i],
                    'var_type':self.df_variables['Param or Eqpt'][i],    #p=parameter, E=equipment, E1=psuedo equipment or notional equipment
                    'identifier_old':self.df_variables['Identifer OLD'][i],
                    'Derived':self.derived(self.df_variables['Derived'][i]),
                    'Daily Availability':self.derived(self.df_variables['Daily Availability'][i]),
                    'availabe_for_groups':self.availability(self.df_variables['AVAILABLE FOR GROUPS'][i]),
                    'dependent':self.availability(self.df_variables['DEPENDENT?'][i]),
                    'group_selection':self.newList,        #create funtion to get groups if 1 it is single parameter,if 2 split and look for 20,if 3 split and look for 30 ....
                    'limits':{
                    'type': self.df_variables['Limit Type'][i],
                    'oplow': self.df_variables['OP Low'][i],
                    'ophigh': self.df_variables['OP High'][i],
                    'olmin': self.df_variables['OL LOW'][i],
                    'olmax': self.df_variables['OL High'][i]                  
                    }
                }
           
        self.data = dict((k,v) for k, v in self.data.items() if not (type(k) == float and np.isnan(k)))
        self.data = { x.translate({32:None}) : y for x, y in self.data.items()}
        

       


    
    # @check_status
    def write_configs(self):
        database=self.db.get_database("aranti")
        ship_collection=database.get_collection("ship")
        ship_imos=ship_collection.distinct("ship_imo")
        # ship = Ship(
        #     ship_imo = self.ship_imo,
        #     ship_name = self.ship_name,
        #     ship_description = self.ship_description,
        #     static_data=self.stat(self.static),
        #     data_available_nav = self.data_available_nav,
        #     data_available_engine = self.data_available_engine,
        #     ais_api_data=self.ais_api,
        #     calculated_var=self.calculated,
        #     mlcontrol=self.get_ml_list(),
        #     parameter_anamoly=self.parameter_anamoly,
        #     equipment_anamoly=self.equipment_anamoly,
        #     outlier_anamoly=self.outlier_anamoly,
        #     identifier_mapping = self.identifier_mapping,
        #     data = self.data
        # )
        
        ship = {
            "ship_imo" : self.ship_imo,
            "ship_name" : self.ship_name,
            "ship_description" : self.ship_description,
            "sister_vessel_list":self.sister_vessel_list,
            "similar_vessel_list":self.similar_vessel_list,
            "static_data":self.stat(self.static),
            "data_available_nav" :self.data_available_nav,
            "data_available_engine": self.data_available_engine,
            "ais_api_data":self.ais_api,
            "calculated_var":self.calculated,
            "mlcontrol":self.get_ml_list(),
            "parameter_anamoly":self.parameter_anamoly,
            "equipment_anamoly":self.equipment_anamoly,
            "outlier_anamoly":self.outlier_anamoly,
            "identifier_mapping" : self.identifier_mapping,
            "indices_data":self.create_indices(),
            "data" : self.data
        }
      
    

        
        # if self.override:
        #     if not Ship.objects(ship_imo = self.ship_imo):
        #         ship.save()
        #     else:
        #         Ship.objects.get(ship_imo = self.ship_imo).delete()
        #         ship.save()
        # else:
        #     if not Ship.objects(ship_imo = self.ship_imo):
        #         ship.save()
        #     else:
        #         return "Record already exists!"
        if self.override==True:
            if self.ship_imo in ship_imos:
                ship_collection.delete_one({"ship_imo": self.ship_imo})
                return ship_collection.insert_one(ship).inserted_id
                
            else:
                print("ksssssssssssssssssssssssssssss")
                return ship_collection.insert_one(ship).inserted_id
        
        elif self.override==False:
            if self.ship_imo in ship_imos:
                print("Record already exist")
                return "Record already exists!"
            else:
                return ship_collection.insert_one(ship).inserted_id


obj=ConfigExtractor(9591301,'F:\Afzal_cs\Internship\ConfiguratorRev_9591301.xlsx',True)
# obj.read_files()
# obj.anamoly()
# obj.process_file()
# obj.connect()
# obj.write_configs()
#obj.read_configs()
# obj.create_indices()


obj.do_steps()
