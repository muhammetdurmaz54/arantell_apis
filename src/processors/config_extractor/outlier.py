
from logging.config import IDENTIFIER
import sys
from numpy.core.arrayprint import format_float_scientific

from numpy.core.defchararray import isdecimal
from numpy.core.numeric import identity, ones
from numpy.lib.function_base import trim_zeros
from pandas.core.algorithms import factorize
from pandas.io.parquet import FastParquetImpl 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from datetime import datetime
from src.db.schema.ship import Ship 
import numpy as np
import pandas as pd
import re
from pymongo import MongoClient

log = CommonLogger(__name__,debug=True).setup_logger()


client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
    


database = db
ship_configs_collection = database.get_collection("ship")

ship_configs = ship_configs_collection.find({"ship_imo": 9591301})




daily_data_collection = database.get_collection("daily_data")
daily_data = daily_data_collection.find({"ship_imo": 9591301,"ship_name":"RTM COOK"})


class CheckOutlier:
    def __init__(self,configs,main_db,ship_imo):
        self.ship_configs=configs
        self.maindb=main_db
        self.ship_imo=ship_imo
        pass

    def get_ship_configs(self):
        ship_configs_collection = database.get_collection("ship")
        self.ship_configs = ship_configs_collection.find({"ship_imo": 9591301})[0]

    def get_main_data(self):  
        maindb_collection = database.get_collection("Main_db")
        self.maindb = maindb_collection.find({"ship_imo": 9591301})[0]
    
    def return_variable(self,string):
        txt = string
        txt_search=[i for i in re.findall("[a-zA-Z0-9_]+",txt) if not i.isdigit()]
        
        return txt_search

    def base_formula_main_data(self,formula_string):
        try:
            list_var=self.return_variable(formula_string)
            static_data=self.ship_configs['static_data']
            main_data=self.maindb['processed_daily_data']
            temp_dict={}
        
            for i in list_var:
                temp_dict[i]=0
                if i in static_data and pd.isnull(static_data[i])==False:
                    temp_dict[i]=static_data[i]['value']
                elif i in main_data and pd.isnull(main_data[i]['processed'])==False:
                    temp_dict[i]=main_data[i]['processed']
                else:
                    temp_dict[i]=None   
            return eval(self.eval_val_replacer(temp_dict, formula_string))
        except:
            return None
        
        
        
    def eval_val_replacer(self,temp_dict, string):
        for key in temp_dict.keys():
            string = re.sub(r"\b" + key + r"\b", str(temp_dict[key]), string, flags=re.I)
        return string


    # def Outlierlimitcheck(self,identifier,identifier_value):     #later onemore parameter to be sent as fomula_var and this will contain the variable which is present in limits and not present in static_data for ex:(w_rel_dir_i + 10) for this we send identifier,identifier_value,w_dir_rel_i
    #     self.identifier=identifier                              #later to be added in operationallimit too 
    #     identifier_value=identifier_value
    #     oplow=self.ship_configs['data'][self.identifier]['limits']['oplow'] 
    #     ophigh=self.ship_configs['data'][self.identifier]['limits']['ophigh']                                    #rpm with % 
    #     olmin=self.ship_configs['data'][self.identifier]['limits']['olmin']
    #     olmax=self.ship_configs['data'][self.identifier]['limits']['olmax']
    #     limit_val=self.ship_configs['data'][self.identifier]['limits']['type']
    #     olmin_outlier=self.outlier_min(identifier,identifier_value,oplow,olmin,limit_val)
        
        


    def outlier_min(self,identifier,identifier_val,opmin,olmin,limit_val,zscore,data_availabe):
        if (pd.isnull(data_availabe)==False or data_availabe!=None) and data_availabe==True:
            if pd.isnull(limit_val) or (limit_val!=1 and limit_val!=2 and limit_val!=3):
                limit_val=2
            if limit_val==1:
                if pd.isnull(olmin):
                    return None,None
                else:
                    if type(olmin)==int or type(olmin)==float:
                        if identifier_val>=olmin:
                            return True,olmin
                        else:
                            return False,olmin
                    elif type(olmin)==str:
                        olmin_length=len(olmin.split())
                        olmin_split=olmin.split()
                        if olmin_length==1 and ("%" not in olmin):
                            olmin_val=self.base_formula_main_data(olmin)
                            if pd.isnull(olmin_val)==False:
                                if identifier_val>=olmin_val:
                                    return True,olmin_val
                                else:
                                    return False,olmin_val
                            else:
                                return None,None
                        elif olmin_length>1:
                            olmin_val=self.base_formula_main_data(olmin)
                            if pd.isnull(olmin_val)==False:
                                if identifier_val>=olmin_val:
                                    return True,olmin_val
                                else:
                                    return False,olmin_val
                            else:
                                return None,None

                        elif olmin_length==1 and ("%" in olmin):
                            if pd.isnull(opmin):
                                return None,None
                            else:
                                if type(opmin)==int or type(opmin)==float:
                                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                                    olmin_val=opmin*olmin_val_percentage
                                    if identifier_val>=olmin_val:
                                        return True,olmin_val
                                    else:
                                        return False,olmin_val
                                elif type(opmin)==str:
                                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                                    opmin_val=self.base_formula_main_data(opmin)
                                    if pd.isnull(opmin_val):
                                        return None,None
                                    else:
                                        olmin_val=opmin_val*olmin_val_percentage
                                        if identifier_val>=olmin_val:
                                            return True,olmin_val
                                        else:
                                            return False,olmin_val
                                else:
                                    return None,None
                        else:
                            return None,None
                    else:
                        return None,None
            elif limit_val==2:
                if pd.isnull(olmin):
                    return zscore,None
                    
                else:
                    if type(olmin)==int or type(olmin)==float:
                        if identifier_val>=olmin:
                            return True,olmin
                        else:
                            return False,None
                    elif type(olmin)==str:
                        olmin_length=len(olmin.split())
                        olmin_split=olmin.split()
                        if olmin_length==1 and ("%" not in olmin):
                            olmin_val=self.base_formula_main_data(olmin)
                            if pd.isnull(olmin_val)==False:
                                if identifier_val>=olmin_val:
                                    return True,olmin_val
                                else:
                                    return False,olmin_val
                            else:
                                return None,None
                        elif olmin_length>1:
                            olmin_val=self.base_formula_main_data(olmin)
                            if pd.isnull(olmin_val)==False:
                                if identifier_val>=olmin_val:
                                    return True,olmin_val
                                else:
                                    return False,olmin_val
                            else:
                                return None,None

                        elif olmin_length==1 and ("%" in olmin):
                            if pd.isnull(opmin):
                                return None,None
                            else:
                                if type(opmin)==int or type(opmin)==float:
                                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                                    olmin_val=opmin*olmin_val_percentage
                                    if identifier_val>=olmin_val:
                                        return True,olmin_val
                                    else:
                                        return False,olmin_val
                                elif type(opmin)==str:
                                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                                    opmin_val=self.base_formula_main_data(opmin)
                                    if pd.isnull(opmin_val):
                                        return None,None
                                    else:
                                        olmin_val=opmin_val*olmin_val_percentage
                                        if identifier_val>=olmin_val:
                                            return True,olmin_val
                                        else:
                                            return False,olmin_val
                                else:
                                    return None,None

                        else:
                            return None,None
                    else:
                        return None,None
            
            elif limit_val==3:
                
                if type(olmin)==int or type(olmin)==float:
                    if identifier_val>=olmin:
                        outlier_min=True
                        final_olmin=olmin
                    else:
                        outlier_min=False
                        final_olmin=olmin
                elif type(olmin)==str:
                    olmin_length=len(olmin.split())
                    olmin_split=olmin.split()
                    if olmin_length==1 and ("%" not in olmin):
                        olmin_val=self.base_formula_main_data(olmin)
                        if pd.isnull(olmin_val)==False:
                            if identifier_val>=olmin_val:
                                outlier_min=True
                                final_olmin=olmin_val
                            else:
                                outlier_min=False
                                final_olmin=olmin_val
                        else:
                            outlier_min=None
                            final_olmin=None
                    elif olmin_length>1:
                        olmin_val=self.base_formula_main_data(olmin)
                        if pd.isnull(olmin_val)==False:
                            if identifier_val>=olmin_val:
                                outlier_min=True
                                final_olmin=olmin_val
                            else:
                                outlier_min=False
                                final_olmin=olmin_val
                        else:
                            outlier_min=None
                            final_olmin=None                        
                    elif olmin_length==1 and ("%" in olmin):
                        if pd.isnull(opmin):
                            outlier_min=None
                            final_olmin=None
                        else:
                            if type(opmin)==int or type(opmin)==float:
                                olmin_val_percentage=float(olmin_split[0][:-1])/100
                                olmin_val=opmin*olmin_val_percentage
                                if identifier_val>=olmin_val:
                                    outlier_min=True
                                    final_olmin=olmin_val
                                else:
                                    outlier_min=False
                                    final_olmin=olmin_val
                            elif type(opmin)==str:
                                olmin_val_percentage=float(olmin_split[0][:-1])/100
                                opmin_val=self.base_formula_main_data(opmin)
                                if pd.isnull(opmin_val):
                                    outlier_min=None
                                    final_olmin=None
                                else:
                                    olmin_val=opmin_val*olmin_val_percentage
                                    if identifier_val>=olmin_val:
                                        outlier_min=True
                                        final_olmin=olmin_val
                                    else:
                                        outlier_min=False
                                        final_olmin=olmin_val
                            else:
                                outlier_min=None
                                final_olmin=None

                    else:
                        outlier_min=None
                        final_olmin=None
                else:
                    outlier_min=None
                    final_olmin=None

                if outlier_min==True and zscore==True:
                    return True,final_olmin
                # elif outlier_min==False and zscore==False:
                #     return False,final_olmin
                else:
                    return False,final_olmin
        
        else:
            return None,None

    def outlier_max(self,identifier,identifier_val,opmax,olmax,limit_val,zscore,data_availabe):
        if (pd.isnull(data_availabe)==False or data_availabe!=None) and data_availabe==True:
            if pd.isnull(limit_val) or (limit_val!=1 and limit_val!=2 and limit_val!=3):
                limit_val=2
            if limit_val==1:
                if pd.isnull(olmax):
                    return None,None
                else:
                    if type(olmax)==int or type(olmax)==float:
                        if identifier_val<=olmax:
                            return True,olmax
                        else:
                            return False,olmax
                    elif type(olmax)==str:
                        olmin_length=len(olmax.split())
                        olmin_split=olmax.split()
                        if olmin_length==1 and ("%" not in olmax):
                            olmin_val=self.base_formula_main_data(olmax)
                            if pd.isnull(olmin_val)==False:
                                if identifier_val<=olmin_val:
                                    return True,olmin_val
                                else:
                                    return False,olmin_val
                            else:
                                return None,None
                        elif olmin_length>1:
                            olmin_val=self.base_formula_main_data(olmax)
                            if pd.isnull(olmin_val)==False:
                                if identifier_val<=olmin_val:
                                    return True,olmin_val
                                else:
                                    return False,olmin_val
                            else:
                                return None,None

                        elif olmin_length==1 and ("%" in olmax):
                            if pd.isnull(opmax):
                                return None,None
                            else:
                                if type(opmax)==int or type(opmax)==float:
                                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                                    olmin_val=opmax*olmin_val_percentage
                                    if identifier_val<=olmin_val:
                                        return True,olmin_val
                                    else:
                                        return False,olmin_val
                                elif type(opmax)==str:
                                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                                    opmin_val=self.base_formula_main_data(opmax)
                                    if pd.isnull(opmin_val):
                                        return None,None
                                    else:
                                        olmin_val=opmin_val*olmin_val_percentage
                                        if identifier_val<=olmin_val:
                                            return True,olmin_val
                                        else:
                                            return False,olmin_val
                                else:
                                    return None,None
                        else:
                            return None,None
                    else:
                        return None,None
            elif limit_val==2:
                if pd.isnull(olmax):
                    return zscore,None
                    
                else:
                    if type(olmax)==int or type(olmax)==float:
                        if identifier_val<=olmax:
                            return True,olmax
                        else:
                            return None,None
                    elif type(olmax)==str:
                        olmin_length=len(olmax.split())
                        olmin_split=olmax.split()
                        if olmin_length==1 and ("%" not in olmax):
                            olmin_val=self.base_formula_main_data(olmax)
                            if pd.isnull(olmin_val)==False:
                                if identifier_val<=olmin_val:
                                    return True,olmin_val
                                else:
                                    return False,olmin_val
                            else:
                                return None,None
                        elif olmin_length>1:
                            olmin_val=self.base_formula_main_data(olmax)
                            if pd.isnull(olmin_val)==False:
                                if identifier_val<=olmin_val:
                                    return True,olmin_val
                                else:
                                    return False,olmin_val
                            else:
                                return None,None

                        elif olmin_length==1 and ("%" in olmax):
                            if pd.isnull(opmax):
                                return None,None
                            else:
                                if type(opmax)==int or type(opmax)==float:
                                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                                    olmin_val=opmax*olmin_val_percentage
                                    if identifier_val<=olmin_val:
                                        return True,olmin_val
                                    else:
                                        return False,olmin_val
                                elif type(opmax)==str:
                                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                                    opmin_val=self.base_formula_main_data(opmax)
                                    if pd.isnull(opmin_val):
                                        return None,None
                                    else:
                                        olmin_val=opmin_val*olmin_val_percentage
                                        if identifier_val<=olmin_val:
                                            return True,olmin_val
                                        else:
                                            return False,olmin_val
                                else:
                                    return None,None

                        else:
                            return None,None
                    else:
                        return None,None
            
            elif limit_val==3:
                
                if type(olmax)==int or type(olmax)==float:
                    if identifier_val>=olmax:
                        outlier_min=True
                        final_olmin=olmax
                    else:
                        outlier_min=False
                        final_olmin=olmax
                elif type(olmax)==str:
                    olmin_length=len(olmax.split())
                    olmin_split=olmax.split()
                    if olmin_length==1 and ("%" not in olmax):
                        olmin_val=self.base_formula_main_data(olmax)
                        if pd.isnull(olmin_val)==False:
                            if identifier_val<=olmin_val:
                                outlier_min=True
                                final_olmin=olmin_val
                            else:
                                outlier_min=False
                                final_olmin=olmin_val
                        else:
                            outlier_min=None
                            final_olmin=None
                    elif olmin_length>1:
                        olmin_val=self.base_formula_main_data(olmax)
                        if pd.isnull(olmin_val)==False:
                            if identifier_val<=olmin_val:
                                outlier_min=True
                                final_olmin=olmin_val
                            else:
                                outlier_min=False
                                final_olmin=olmin_val
                        else:
                            outlier_min=None
                            final_olmin=None                        
                    elif olmin_length==1 and ("%" in olmax):
                        if pd.isnull(opmax):
                            outlier_min=None
                            final_olmin=None
                        else:
                            if type(opmax)==int or type(opmax)==float:
                                olmin_val_percentage=float(olmin_split[0][:-1])/100
                                olmin_val=opmax*olmin_val_percentage
                                if identifier_val<=olmin_val:
                                    outlier_min=True
                                    final_olmin=olmin_val
                                else:
                                    outlier_min=False
                                    final_olmin=olmin_val
                            elif type(opmax)==str:
                                olmin_val_percentage=float(olmin_split[0][:-1])/100
                                opmin_val=self.base_formula_main_data(opmax)
                                if pd.isnull(opmin_val):
                                    outlier_min=None
                                    final_olmin=None
                                else:
                                    olmin_val=opmin_val*olmin_val_percentage
                                    if identifier_val<=olmin_val:
                                        outlier_min=True
                                        final_olmin=olmin_val
                                    else:
                                        outlier_min=False
                                        final_olmin=olmin_val
                            else:
                                outlier_min=None
                                final_olmin=None

                    else:
                        outlier_min=None
                        final_olmin=None
                else:
                    outlier_min=None
                    final_olmin=None

                if outlier_min==True and zscore==True:
                    return True,final_olmin
                # elif outlier_min==False and zscore==False:
                #     return False,final_olmin
                else:
                    return False,final_olmin
        else:
            return None,None


    def final_outlier(self,outlier_min,outlier_max):
        if pd.isnull(outlier_min)==False and pd.isnull(outlier_max)==False:
            if outlier_max==True and outlier_min==True:
                return True
            
            else:
                return False
        else:
            return None
        

    def operational_min(self,identifier,identifier_val,opmin,limit_val,zscore,data_availabe):
        if (pd.isnull(data_availabe)==False or data_availabe!=None) and data_availabe==True:
            if pd.isnull(limit_val) or (limit_val!=1 and limit_val!=2 and limit_val!=3):
                limit_val=2
            if limit_val==1:
                if pd.isnull(opmin):
                    return None,None
                else:
                    if type(opmin)==int or type(opmin)==float:
                        if identifier_val>=opmin:
                            return True,opmin
                        else:
                            return False,opmin
                    elif type(opmin)==str:
                        olmin_val=self.base_formula_main_data(opmin)
                        if pd.isnull(olmin_val)==False:
                            if identifier_val>=olmin_val:
                                return True,olmin_val
                            else:
                                return False,olmin_val
                        else:
                            return None,None
                    else:
                        return None,None


            if limit_val==2:
                if pd.isnull(opmin):
                    return zscore,None
                else:
                    if type(opmin)==int or type(opmin)==float:
                        if identifier_val>=opmin:
                            return True,opmin
                        else:
                            return False,opmin
                    elif type(opmin)==str:
                        olmin_val=self.base_formula_main_data(opmin)
                        if pd.isnull(olmin_val)==False:
                            if identifier_val>=olmin_val:
                                return True,olmin_val
                            else:
                                return False,olmin_val
                        else:
                            return None,None
                    else:
                        return None,None
            
            if limit_val==3:
                if type(opmin)==int or type(opmin)==float:
                    if identifier_val>=opmin:
                        operational_min=True
                        final_opmin=opmin
                    else:
                        operational_min=False
                        final_opmin=opmin
                elif type(opmin)==str:
                    olmin_val=self.base_formula_main_data(opmin)
                    if pd.isnull(olmin_val)==False:
                        if identifier_val>=olmin_val:
                            operational_min=True
                            final_opmin=olmin_val
                        else:
                            operational_min=False
                            final_opmin=olmin_val
                            return False,olmin_val
                    else:
                        operational_min=None
                        final_opmin=None
                else:
                    operational_min=None
                    final_opmin=None
                    
                if operational_min==True and zscore==True:
                    return True,final_opmin
                # elif operational_min==False and zscore==False:
                #     return False,final_opmin
                else:
                    return False,final_opmin
            else:
                return None,None
        
        else:
            return None,None

    def operational_max(self,identifier,identifier_val,opmax,limit_val,zscore,data_availabe):
        if (pd.isnull(data_availabe)==False or data_availabe!=None) and data_availabe==True:
            if pd.isnull(limit_val) or (limit_val!=1 and limit_val!=2 and limit_val!=3):
                limit_val=2
            
            if limit_val==1:
                if pd.isnull(opmax):
                    return None,None
                else:
                    if type(opmax)==int or type(opmax)==float:
                        if identifier_val<=opmax:
                            return True,opmax
                        else:
                            return False,opmax
                    elif type(opmax)==str:
                        olmin_val=self.base_formula_main_data(opmax)
                        if pd.isnull(olmin_val)==False:
                            if identifier_val<=olmin_val:
                                return True,olmin_val
                            else:
                                return False,olmin_val
                        else:
                            return None,None
                    else:
                        return None,None


            if limit_val==2:
                if pd.isnull(opmax):
                    return zscore,None
                else:
                    if type(opmax)==int or type(opmax)==float:
                        if identifier_val<=opmax:
                            return True,opmax
                        else:
                            return False,opmax
                    elif type(opmax)==str:
                        olmin_val=self.base_formula_main_data(opmax)
                        if pd.isnull(olmin_val)==False:
                            if identifier_val<=olmin_val:
                                return True,olmin_val
                            else:
                                return False,olmin_val
                        else:
                            return None,None
                    else:
                        return None,None
            
            if limit_val==3:
                if type(opmax)==int or type(opmax)==float:
                    if identifier_val<=opmax:
                        operational_min=True
                        final_opmin=opmax
                    else:
                        operational_min=False
                        final_opmin=opmax
                elif type(opmax)==str:
                    olmin_val=self.base_formula_main_data(opmax)
                    if pd.isnull(olmin_val)==False:
                        if identifier_val<=olmin_val:
                            operational_min=True
                            final_opmin=olmin_val
                        else:
                            operational_min=False
                            final_opmin=olmin_val
                            return False,olmin_val
                    else:
                        operational_min=None
                        final_opmin=None
                else:
                    operational_min=None
                    final_opmin=None
                    
                if operational_min==True and zscore==True:
                    return True,final_opmin
                # elif operational_min==False and zscore==False:
                #     return False,final_opmin
                else:
                    return False,final_opmin
            else:
                return None,None
        else:
            return None,None


# obj=CheckOutlier("gds","kh")
# obj.get_ship_configs()
# obj.get_main_data()
# # print(obj.Outlierlimitcheck("vsl_load",14*3))
# # print(obj.operational_limit("main_hsdo",0))

# print(obj.base_formula_main_data("cplsfo_cons"))