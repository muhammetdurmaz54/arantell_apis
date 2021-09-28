
from logging.config import IDENTIFIER
import sys

from numpy.core.defchararray import isdecimal
from numpy.core.numeric import identity
from numpy.lib.function_base import trim_zeros
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
    def __init__(self,configs,main_db):
        self.ship_configs=configs
        self.maindb=main_db
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
        
        
        
    def eval_val_replacer(self,temp_dict, string):
        for key in temp_dict.keys():
            string = re.sub(r"\b" + key + r"\b", str(temp_dict[key]), string, flags=re.I)
        return string


    def Outlierlimitcheck(self,identifier,identifier_value):     #later onemore parameter to be sent as fomula_var and this will contain the variable which is present in limits and not present in static_data for ex:(w_rel_dir_i + 10) for this we send identifier,identifier_value,w_dir_rel_i
        self.identifier=identifier                              #later to be added in operationallimit too 
        identifier_value=identifier_value
        oplow=self.ship_configs['data'][self.identifier]['limits']['oplow'] 
        ophigh=self.ship_configs['data'][self.identifier]['limits']['ophigh']                                    #rpm with % 
        olmin=self.ship_configs['data'][self.identifier]['limits']['olmin']
        olmax=self.ship_configs['data'][self.identifier]['limits']['olmax']

        if pd.isnull(olmax) and pd.isnull(olmin):               #if both are null then not checked
            return "not checked"       
                       
        elif pd.isnull(olmax) and pd.isnull(olmin)==False:                        #if olmax is null and olmin contains some value
            if type(olmin)==int or type(olmin)==float:                                    #if olmin is int direct comparison
                if identifier_value>=olmin:
                    return True
                else:
                    return False    #done
            
            elif type(olmin)==str:                                    #if olmin is string
                olmin_split=olmin.split()
                olmin_len=len(olmin_split)
                if (olmin_len==1) and ("%" not in olmin_split[0]):
                    val=self.base_formula_main_data(olmin)
                    if identifier_value>=val:
                        return True
                    else:
                        return False    #done
                
                elif (olmin_len==1) and ("%" in olmin_split[0]):
                    olmin_split_percentage=float(olmin_split[0][:-1])
                    if type(oplow)==str:                                #oplow can be str or int if olmin is a percentage value(this is for string)
                        oplow_split=oplow.split()
                        oplow_len=len(oplow_split)
                        val=self.base_formula_main_data(oplow)
                        olmin_val=(val*olmin_split_percentage)/100
                        if identifier_value>=olmin_val:
                            return True
                        else: 
                            return False    #done
 
                    elif type(oplow)==int or type(oplow)==float:           #if oplow is int or float
                        olmin_val=(oplow*olmin_split_percentage)/100
                        if identifier_value>=olmin_val:
                            return True
                        else:
                            return False   #done
                    
                elif olmin_len>1 or olmin_len>2:
                    val=self.base_formula_main_data(olmin)
                    if identifier_value>=val:
                        return True
                    else:
                        return False  #done

        
        elif pd.isnull(olmax)==False and pd.isnull(olmin):                          #if olmin is null and olmax contains some value
            if type(olmax)==int or type(olmax)==float:
                if identifier_value<=olmax:
                    return True
                else:
                    return False  #done
                                        
                
            elif type(olmax)==str:                                               #if olmin float(percentage which is either 110% or 120%)
                olmax_split=olmax.split()
                olmax_len=len(olmax_split)
                if (olmax_len==1) and ("%" not in olmax_split[0]):
                    val=self.base_formula_main_data(olmax)
                    if identifier_value<=val:
                        return True
                    else:
                        return False  #done 
                
                elif (olmax_len==1) and ("%" in olmax_split[0]):
                    olmax_split_percentage=float(olmax_split[0][:-1])
                    if type(ophigh)==str:                                #oplow can be str or int if olmin is a percentage value(this is for string)
                        ophigh_split=ophigh.split()
                        ophigh_len=len(ophigh_split)
                        val=self.base_formula_main_data(ophigh)
                        olmax_val=(val*olmax_split_percentage)/100
                        if identifier_value<=olmax_val:
                            return True
                        else:
                            return False   #done 
                    
                    elif type(ophigh)==int or type(ophigh)==float:           #if oplow is int or float
                        olmax_val=(ophigh*olmax_split_percentage)/100
                        if identifier_value<=olmax_val:
                            return True
                        else:
                            return False  #done

                elif olmax_len>1 or olmax_len>2:
                    val=self.base_formula_main_data(olmax)
                    if identifier_value<=val:
                        return True
                    else:
                        return False #done 

        elif (pd.isnull(olmax)==False and pd.isnull(olmin)==False):
            if (type(olmax)==int or type(olmax)==float) and (type(olmin)==int or type(olmin)==float):               #if both are int or float directly we get outlier
                if identifier_value<=olmax and identifier_value>=olmin:
                    return True
                else:
                    return False

            elif type(olmax)==str and type(olmin)==str:   #olmax and olmin is str
                olmax_split=olmax.split()
                olmax_len=len(olmax_split)
                olmin_split=olmin.split()
                olmin_len=len(olmin_split)
                if (olmin_len==1 and olmax_len==1) and (("%" not in olmin_split[0]) and ("%" not in olmax_split[0])):
                    olmin_val=self.base_formula_main_data(olmin)
                    olmax_val=self.base_formula_main_data(olmax)
                    if identifier_value<=olmax_val and identifier_value>=olmin_val:
                        return True
                    else:
                        return False

                elif (olmin_len==1 and olmax_len==1) and (("%" in olmin_split[0]) and ("%" in olmax_split[0])):
                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                    olmax_val_percentage=float(olmax_split[0][:-1])/100
                    if type(ophigh)==str and type(oplow)==str:
                        oplowsplit=oplow.split()
                        oplow_len=len(oplowsplit)
                        ophigh_split=ophigh.split()
                        ophigh_len=len(ophigh_split)
                        
                        oplow_val=self.base_formula_main_data(oplow)*olmin_val_percentage
                        ophigh_val=self.base_formula_main_data(ophigh)*olmax_val_percentage
                        if identifier_value>=oplow_val and identifier_value<=ophigh_val:
                            return True
                        else:
                            return False  #extra conditions will come here


                    elif (type(ophigh)==int or type(ophigh)==float) and (type(oplow)==int or type(oplow)==float):
                        valolmax=olmax_val_percentage*ophigh
                        valolmin=olmin_val_percentage*olmin
                        if identifier_value>=valolmin and identifier_value<=valolmax:
                            return True
                        else:
                            return False
                    
                    elif (type(ophigh)==str) and (type(oplow)==int or type(oplow)==float):
                        ophigh_split=ophigh.split()
                        ophigh_len=len(ophigh_split)
                        oplow_val=oplow*olmin_val_percentage
        
                        val=self.base_formula_main_data(ophigh)*olmax_val_percentage
                        if identifier_value<=val and identifier_value>=oplow_val:
                            return True
                        else:
                            return False
                        
                    elif (type(ophigh)==int or type(ophigh)==float) and (type(oplow)==str):
                        oplow_split=oplow.split()
                        oplow_len=len(oplow_split)
                        ophigh_val=ophigh*olmax_val_percentage
                
                        oplow_val=self.base_formula_main_data(oplow)*olmin_val_percentage
                        if identifier_value<=ophigh_val and identifier_value>=oplow_val:
                            return True
                        else:
                            return False
                        

                elif (olmax_len==1 and olmin_len==1) and (("%" in olmax_split[0]) and ("%" not in olmin_split[0])):
                    olmin_val=self.base_formula_main_data(olmin)
                    olmax_val_percentage=float(olmax_split[0][:-1])/100
                    if type(ophigh)==str:
                        ophigh_split=ophigh.split()
                        ophigh_len=len(ophigh_split)
                        
                        ophigh_val=self.base_formula_main_data(ophigh) * olmax_val_percentage
                        if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                            return True
                        else:
                            return False
                        
                    elif type(ophigh)==int or type(ophigh)==float:
                        olmax_val=ophigh*olmax_val_percentage
                        if identifier_value>=olmin_val and identifier_value<=olmax_val:
                            return True
                        else:
                            return False

                elif (olmax_len==1 and olmin_len==1) and (("%" in olmin_split[0]) and ("%" not in olmax_split[0])):
                    ophigh_val=self.base_formula_main_data(olmax)
                    olmin_val_percentage=float(olmin_split[0][-1])/100
                    if type(oplow)==str:
                        oplow_split=oplow.split()
                        oplow_len=len(oplow_split)  
                        
                        olmin_val=self.base_formula_main_data(oplow) * olmin_val_percentage
                        if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                            return True
                        else:
                            return False
                        
                    elif type(oplow)==int or type(oplow)==float:
                        olmin_val=oplow*olmin_val_percentage
                        if identifier_value>=olmin_val and identifier_value<=ophigh_val:
                            return True
                        else:
                            return False  

                elif olmin_len==1 and (olmax_len>1 or olmax_len>2) :  
                    olmax_val=self.base_formula_main_data(olmax)
                    if "%" not in olmin_split[0]:
                        olmin_val=self.base_formula_main_data(olmin)
                        
                        
                        if identifier_value<=olmax_val and identifier_value>=olmin_val:
                            return True
                        else:
                            return False
                            
                    elif "%" in olmin_split[0]:
                        olmin_val_percentage=float(olmin_split[0][-1])/100                   

                        if type(oplow)==str:
                            oplow_split=oplow.split()
                            oplow_len=len(oplow_split)  
                            
                            olmin_val=self.base_formula_main_data(oplow) * olmin_val_percentage
                            if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                            
                        elif type(oplow)==int or type(oplow)==float:
                            olmin_val=oplow*olmin_val_percentage
                            if identifier_value>=olmin_val and identifier_value<=olmax_val:
                                return True
                            else:
                                return False



                elif (olmin_len>1 or olmin_len>2) and olmax_len==1:
                    if "%" not in olmax_split[0]:
                        olmax_val=self.base_formula_main_data(olmax)
                        
                        olmin_val=self.base_formula_main_data(olmin) 
                        if identifier_value<=olmax_val and identifier_value>=olmin_val:
                            return True
                        else:
                            return False
                            
                    elif "%" in olmax_split[0]:
                        olmax_val_percentage=float(olmax_split[0][:-1])/100
                        self.golab_1=self.base_formula_main_data(olmin)
                       
                        if type(ophigh)==str:
                            ophigh_split=ophigh.split()
                            ophigh_len=len(ophigh_split)  
                            
                            olmax_val=self.base_formula_main_data(ophigh) * olmax_val_percentage
                            if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                return True
                            else:
                                return False
                            
                        elif type(ophigh)==int or type(ophigh)==float:
                            olmax_val=ophigh*olmax_val_percentage
                            if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                return True
                            else:
                                return False

                            


                elif (olmin_len>1 or olmin_len>2) and (olmax_len>1 or olmax_len>2):
                    self.olmin_len_three_val=self.base_formula_main_data(olmin)
                    self.olmax_len_three_val=self.base_formula_main_data(olmax)
                    
                    if identifier_value>=self.olmin_len_three_val and identifier_value<=self.olmax_len_three_val:
                        return True
                    else:
                        return False



            elif (type(olmax)==str) and (type(olmin)==int or type(olmin)==float):
                olmax_split=olmax.split()
                olmax_len=len(olmax_split)
                if (olmax_len==1) and ("%" not in olmax_split[0]):
                    olmax_val=self.base_formula_main_data(olmax)
                    if identifier_value<=olmax_val and identifier_value>=olmin:
                        return True
                    else:
                        return False
                elif (olmax_len==1) and ("%" in olmax_split[0]):
                    olmax_val_percentage=float(olmax_split[0][:-1])/100
                    if type(ophigh)==str:
                        ophigh_split=ophigh.split()
                        ophigh_len=len(ophigh_split)  
                        
                        olmax_val=self.base_formula_main_data(ophigh) * olmax_val_percentage
                        if identifier_value<=olmax_val and identifier_value>=olmin:
                            return True
                        else:
                            return False
                        
                    elif type(ophigh)==int or type(ophigh)==float:
                        olmax_val=ophigh*olmax_val_percentage
                        if identifier_value<=olmax_val and identifier_value>=olmin:
                            return True
                        else:
                            return False

                elif olmax_len>1 or olmax_len>2 :
                    
                    olmax_val=self.base_formula_main_data(olmax) 
                    if identifier_value<=olmax_val and identifier_value>=olmin:
                        return True
                    else:
                        return False

            elif (type(olmax)==int or type(olmax)==float) and (type(olmin)==str):
                olmin_split=olmin.split()
                olmin_len=len(olmin_split)
                if (olmin_len==1) and ("%" not in olmin_split[0]):
                    olmin_val=self.base_formula_main_data(olmin)
                    if identifier_value<=olmax and identifier_value>=olmin_val:
                        return True
                    else:
                        return False
                
                elif (olmin_len==1) and ("%" in olmin_split[0]):
                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                    if type(oplow)==str:
                        oplow_split=oplow.split()
                        oplow_len=len(oplow_split)  
                        
                        olmin_val=self.base_formula_main_data(oplow)
                        if identifier_value<=olmax and identifier_value>=olmin_val:
                            return True
                        else:
                            return False
                        
                    elif type(oplow)==int or type(oplow)==float:
                        olmin_val=oplow*olmin_val_percentage
                        if identifier_value>=olmin_val and identifier_value<=olmax:
                            return True
                        else:
                            return False
                elif olmin_len>1 or olmin_len>2:
                    
                    olmin_val=self.base_formula_main_data(olmin)
                    if identifier_value<=olmax and identifier_value>=olmin_val:
                        return True
                    else:
                        return False
                        
        else:
            return "not checked"


    def operational_limit(self,identifier,identifier_value):
        identifier=identifier
        identifier_value=identifier_value
        oplow=self.ship_configs['data'][identifier]['limits']['oplow'] 
        ophigh=self.ship_configs['data'][identifier]['limits']['ophigh']

        
        
        if pd.isnull(oplow) and pd.isnull(ophigh):
            return "not checked"
        

        elif pd.isnull(oplow) and pd.isnull(ophigh)==False:
            if (type(ophigh)==int or type(ophigh)==float):
                if identifier_value<=ophigh:
                    return True
                else:
                    return False
            
            elif type(ophigh)==str:
                ophigh_split=ophigh.split()
                ophigh_len=len(ophigh_split)
                
                ophigh_val=self.base_formula_main_data(ophigh)
                if identifier_value<=ophigh_val:
                    return True
                else:
                    return False
               

        elif pd.isnull(oplow)==False and pd.isnull(ophigh):
            if type(oplow)==int or type(oplow)==float:
                if identifier_value>=oplow:
                    return True
                else:
                    return False
            
            elif type(oplow)==str:
                oplow_split=oplow.split()
                oplow_len=len(oplow_split)
                
                oplow_val=self.base_formula_main_data(oplow)
                if identifier_value>=oplow_val:
                    return True
                else:
                    return False
                

        elif pd.isnull(oplow)==False and pd.isnull(ophigh)==False:
            if (type(oplow)==int or type(oplow)==float) and type(ophigh)==str:
                ophigh_split=ophigh.split()
                ophigh_len=len(ophigh_split)
                
                ophigh_val=self.base_formula_main_data(ophigh)
                if identifier_value<=ophigh_val and identifier_value>=oplow:
                    return True
                else:
                    return False
                
            elif (type(ophigh)==int or type(ophigh)==float) and type(oplow)==str:
                oplow_split=oplow.split()
                oplow_len=len(oplow_split)
                
                oplow_val=self.base_formula_main_data(oplow)
                if identifier_value>=oplow_val and identifier_value<=ophigh:
                    return True
                else:
                    return False
                
        
            elif type(oplow)==str and type(ophigh)==str:
                oplowsplit=oplow.split()
                oplow_len=len(oplowsplit)
                ophigh_split=ophigh.split()
                ophigh_len=len(ophigh_split)
                
                oplow_val=self.base_formula_main_data(oplow)
                ophigh_val=self.base_formula_main_data(ophigh)
                if identifier_value>=oplow_val and identifier_value<=ophigh_val:
                    return True
                else:
                    return False
                

            elif (type(oplow)==int or type(oplow)==float) and (type(ophigh)==int or type(ophigh)==float):
                if identifier_value<=ophigh and identifier_value>=oplow:
                    return True
                else:
                    return False

        else:
            return "not checked"






# obj=CheckOutlier("gds","kh")
# obj.get_ship_configs()
# obj.get_main_data()
# # print(obj.Outlierlimitcheck("vsl_load",14*3))
# # print(obj.operational_limit("main_hsdo",0))

# print(obj.base_formula_main_data("cplsfo_cons"))