from logging.config import IDENTIFIER
import sys
import os
from dotenv import load_dotenv
load_dotenv()
from numpy.core.defchararray import isdecimal
from numpy.core.numeric import identity
from numpy.lib.function_base import trim_zeros
from pandas.io.parquet import FastParquetImpl 
# sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from datetime import datetime
from src.db.schema.ship import Ship 
import numpy as np
import pandas as pd
from pymongo import MongoClient

log = CommonLogger(__name__,debug=True).setup_logger()


# client = MongoClient("mongodb://localhost:27017/aranti")
# db=client.get_database("aranti")
client = MongoClient(os.getenv("MONGODB_ATLAS"))
db=client.get_database("aranti")


database = db
ship_configs_collection = database.get_collection("ship")

ship_configs = ship_configs_collection.find({"ship_imo": 9591301})




daily_data_collection = database.get_collection("daily_data")
daily_data = daily_data_collection.find({"ship_imo": 9591301,"ship_name":"RTM COOK"})


class CheckOutlier:
    def __init__(self,configs):
        self.ship_configs=configs
        pass

    def get_ship_configs(self):
        ship_configs_collection = database.get_collection("ship")
        self.ship_configs = ship_configs_collection.find({"ship_imo": 9591301})[0]


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
                    return False
            
            elif type(olmin)==str:                                    #if olmin is string
                olmin_split=olmin.split()
                olmin_len=len(olmin_split)
                if (olmin_len==1) and ("%" not in olmin_split[0]):
                    val=self.ship_configs['static_data'][olmin]
                    if identifier_value>=val:
                        return True
                    else:
                        return False
                
                elif (olmin_len==1) and ("%" in olmin_split[0]):
                    olmin_split_percentage=float(olmin_split[0][:-1])
                    if type(oplow)==str:                                #oplow can be str or int if olmin is a percentage value(this is for string)
                        oplow_split=oplow.split()
                        oplow_len=len(oplow_split)
                        if oplow_len==1:
                            val=self.ship_configs['static_data'][oplow]
                            olmin_val=(val*olmin_split_percentage)/100
                            if identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                        elif oplow_len==3:
                            for i in oplow_split:
                                if i=="+":
                                    val=self.ship_configs['static_data'][oplow_split[0]] + float(oplow_split[2])
                                    value=(val*olmin_split_percentage)/100
                                    if identifier_value>=value:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    val=self.ship_configs['static_data'][oplow_split[0]] - float(oplow_split[2])
                                    value=(val*olmin_split_percentage)/100
                                    if identifier_value>=value:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    val=self.ship_configs['static_data'][oplow_split[0]] * float(oplow_split[2])
                                    value=(val*olmin_split_percentage)/100
                                    if identifier_value>=value:
                                        return False
                                    else:
                                        return False
                                elif i=="/":
                                    val=self.ship_configs['static_data'][oplow_split[0]] / float(oplow_split[2])
                                    value=(val*olmin_split_percentage)/100
                                    if identifier_value>=value:
                                        return True
                                    else:
                                        return False

                    elif type(oplow)==int or type(oplow)==float:           #if oplow is int or float
                        olmin_val=(oplow*olmin_split_percentage)/100
                        if identifier_value>=olmin_val:
                            return True
                        else:
                            return False
                    
                elif olmin_len==3:
                    for i in olmin_split:
                        if i=="+":
                            val=self.ship_configs['static_data'][olmin_split[0]] + float(olmin_split[2])
                            if identifier_value>=val:
                                return True
                            else:
                                return False
                        elif i=="-":
                            val=self.ship_configs['static_data'][olmin_split[0]]-float(olmin_split[2])
                            if identifier_value>=val:
                                return True
                            else:
                                return False
                        elif i=="*":
                            val=self.ship_configs['static_data'][olmin_split[0]]*float(olmin_split[2])
                            if identifier_value>=val:
                                return True
                            else:
                                return False
                        elif i=="/":
                            val=self.ship_configs['static_data'][olmin_split[0]]/float(olmin_split[2])
                            if identifier_value>=val:
                                return True
                            else:
                                return False
                elif olmin_len==5:
                    if olmin_split[1]=="+" and olmin_split[3]=="+":
                        val=self.ship_configs['static_data'][olmin_split[0]] + self.ship_configs['static_data'][olmin_split[2]] + self.ship_configs['static_data'][olmin_split[4]]
                        if identifier_value>=val:
                            return True
                        else:
                            return False
                    elif olmin_split[1]=="-" and olmin_split[3]=="-":
                        val=self.ship_configs['static_data'][olmin_split[0]] - self.ship_configs['static_data'][olmin_split[2]] - self.ship_configs['static_data'][olmin_split[4]]
                        if identifier_value>=val:
                            return True
                        else:
                            return False
                    elif olmin_split[1]=="*" and olmin_split[3]=="*":
                        val=self.ship_configs['static_data'][olmin_split[0]] * self.ship_configs['static_data'][olmin_split[2]] * self.ship_configs['static_data'][olmin_split[4]]
                        if identifier_value>=val:
                            return True
                        else:
                            return False
                    


        
        elif pd.isnull(olmax)==False and pd.isnull(olmin):                          #if olmin is null and olmax contains some value
            if type(olmax)==int or type(olmax)==float:
                if identifier_value<=olmax:
                    return True
                else:
                    return False
                                        
                
            elif type(olmax)==str:                                               #if olmin float(percentage which is either 110% or 120%)
                olmax_split=olmax.split()
                olmax_len=len(olmax_split)
                if (olmax_len==1) and ("%" not in olmax_split[0]):
                    val=self.ship_configs['static_data'][olmax]
                    if identifier_value<=val:
                        return True
                    else:
                        return False
                
                elif (olmax_len==1) and ("%" in olmax_split[0]):
                    olmax_split_percentage=float(olmax_split[0][:-1])
                    if type(ophigh)==str:                                #oplow can be str or int if olmin is a percentage value(this is for string)
                        ophigh_split=ophigh.split()
                        ophigh_len=len(ophigh_split)
                        if ophigh_len==1:
                            val=self.ship_configs['static_data'][ophigh]
                            olmax_val=(val*olmax_split_percentage)/100
                            if identifier_value<=olmax_val:
                                
                                return True
                            else:
                                return False
                        elif ophigh_len==3:
                            for i in ophigh_split:
                                if i=="+":
                                    val=self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2])
                                    value=(val*olmax_split_percentage)/100
                                    if identifier_value<=value:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    val=self.ship_configs['static_data'][ophigh_split[0]]-float(ophigh_split[2])
                                    value=(val*olmax_split_percentage)/100
                                    if identifier_value<=value:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    val=self.ship_configs['static_data'][ophigh_split[0]]*float(ophigh_split[2])
                                    value=(val*olmax_split_percentage)/100
                                    if identifier_value<=value:
                                        return True
                                    else:
                                        return False
                                elif i=="/":
                                    val=self.ship_configs['static_data'][ophigh_split[0]]/float(ophigh_split[2])
                                    value=(val*olmax_split_percentage)/100
                                    if identifier_value<=value:
                                        return True
                                    else:
                                        return False
                    
                    elif type(ophigh)==int or type(ophigh)==float:           #if oplow is int or float
                        olmax_val=(ophigh*olmax_split_percentage)/100
                        if identifier_value<=olmax_val:
                            return True
                        else:
                            return False


                elif olmax_len==3:
                    for i in olmax_split:
                        if i=="+":
                            value=self.ship_configs['static_data'][olmax_split[0]] + float(olmax_split[2])
                            if identifier_value<=value:
                                return True
                            else:
                                return False
                        elif i=="-":
                            value=self.ship_configs['static_data'][olmax_split[0]] - float(olmax_split[2])
                            if identifier_value<=value:
                                return True
                            else:
                                return False
                        elif i=="*":
                            value=self.ship_configs['static_data'][olmax_split[0]] * float(olmax_split[2])
                            if identifier_value<=value:
                                return True
                            else:
                                return False
                        elif i=="/":
                            value=self.ship_configs['static_data'][olmax_split[0]] / float(olmax_split[2])
                            if identifier_value<=value:
                                return True
                            else:
                                return False
                elif olmax_len==5:
                    if olmax_split[1]=="+" and olmax_split[3]=="+":
                        val=self.ship_configs['static_data'][olmax_split[0]] + self.ship_configs['static_data'][olmax_split[2]] + self.ship_configs['static_data'][olmax_split[4]]
                        if identifier_value<=val:
                            return True
                        else:
                            return False
                    elif olmax_split[1]=="-" and olmax_split[3]=="-":
                        val=self.ship_configs['static_data'][olmax_split[0]] - self.ship_configs['static_data'][olmax_split[2]] - self.ship_configs['static_data'][olmax_split[4]]
                        if identifier_value<=val:
                            return True
                        else:
                            return False
                    elif olmax_split[1]=="*" and olmax_split[3]=="*":
                        val=self.ship_configs['static_data'][olmax_split[0]] * self.ship_configs['static_data'][olmax_split[2]] * self.ship_configs['static_data'][olmax_split[4]]
                        if identifier_value<=val:
                            
                            return True
                        else:
                            return False    
        


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
                    olmin_val=self.ship_configs['static_data'][olmin]
                    olmax_val=self.ship_configs['static_data'][olmax]
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
                        if oplow_len==1 and ophigh_len==1:
                            oplow_val=self.ship_configs['static_data'][oplow]*olmin_val_percentage
                            ophigh_val=self.ship_configs['static_data'][ophigh]*olmax_val_percentage
                            if identifier_value>=oplow_val and identifier_value<=ophigh_val:
                                return True
                            else:
                                return False
                        elif oplow_len==1 and ophigh_len==3:
                            oplow_val=self.ship_configs['static_data'][oplow]*olmin_val_percentage
                            for i in ophigh_split:
                                if i=="+":
                                    val=(self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2])) * olmax_val_percentage
                                    if identifier_value>=oplow_val and identifier_value<=val:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    val=(self.ship_configs['static_data'][ophigh_split[0]]-float(ophigh_split[2])) *olmax_val_percentage
                                    if identifier_value>=oplow_val and identifier_value<=val:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    val=(self.ship_configs['static_data'][ophigh_split[0]]*float(ophigh_split[2])) * olmax_val_percentage
                                    if identifier_value>=oplow_val and identifier_value<=val:
                                        return True
                                    else:
                                        return False
                                elif i=="/":
                                    val=(self.ship_configs['static_data'][ophigh_split[0]]/float(ophigh_split[2])) * olmax_val_percentage
                                    if identifier_value>=oplow_val and identifier_value<=val:
                                        return True
                                    else:
                                        return False
                        elif oplow_len==3 and ophigh_len==1:
                            ophigh_val=self.ship_configs['static_data'][ophigh]*olmax_val_percentage
                            for i in oplowsplit:
                                if i=="+":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]] + float(oplowsplit[2])) * olmin_val_percentage
                                    if identifier_value>=val and identifier_value<=ophigh_val:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]]-float(oplowsplit[2])) * olmin_val_percentage
                                    if identifier_value>=val and identifier_value<=ophigh_val:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]]*float(oplowsplit[2])) * olmin_val_percentage
                                    if identifier_value>=val and identifier_value<=ophigh_val:
                                        return True
                                    else:
                                        return False
                                elif i=="/":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]]/float(oplowsplit[2])) * olmin_val_percentage
                                    if identifier_value>=val and identifier_value<=ophigh_val:
                                        return True
                                    else:
                                        return False
                        elif oplow_len==3 and ophigh_len==3:
                            self.oplow_val_len_three=0
                            self.ophigh_val_len_three=0
                            for i in oplowsplit:
                                if i=="+":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]] + float(oplowsplit[2])) * olmin_val_percentage
                                    self.oplow_val_len_three=val
                                elif i=="-":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]]-float(oplowsplit[2])) * olmin_val_percentage
                                    self.oplow_val_len_three=val
                                elif i=="*":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]]*float(oplowsplit[2])) * olmin_val_percentage
                                    self.oplow_val_len_three=val
                                elif i=="/":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]]/float(oplowsplit[2])) *olmin_val_percentage
                                    self.oplow_val_len_three=val
                                return self.oplow_val_len_three
                            for j in ophigh_split:     
                                if j=="+":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]] + float(oplowsplit[2])) * olmax_val_percentage
                                    self.ophigh_val_len_three=val
                                elif j=="-":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]]-float(oplowsplit[2])) * olmax_val_percentage
                                    self.ophigh_val_len_three=val
                                elif j=="*":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]]*float(oplowsplit[2])) * olmax_val_percentage
                                    self.ophigh_val_len_three=val
                                elif j=="/":
                                    val=(self.ship_configs['static_data'][oplowsplit[0]]/float(oplowsplit[2])) * olmax_val_percentage
                                    self.ophigh_val_len_three=val
                                return self.ophigh_val_len_three
                            if identifier_value<=self.ophigh_val_len_three and identifier_value>=self.oplow_val_len_three:
                                return True
                            else:
                                return False


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
                        if ophigh_len==1:
                            val=self.ship_configs['static_data'][ophigh]*olmax_val_percentage
                            if identifier_value<=val and identifier_value>=oplow_val:
                                return True
                            else:
                                return False
                        elif ophigh_len==3:
                            for i in ophigh_split:
                                if i=="+":
                                    olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2])) * olmax_val_percentage
                                    if identifier_value<=olmax_val and identifier_value>=oplow_val:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] - float(ophigh_split[2]))* olmax_val_percentage
                                    if identifier_value<=olmax_val and identifier_value>=oplow_val:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] * float(ophigh_split[2]))* olmax_val_percentage
                                    if identifier_value<=olmax_val and identifier_value>=oplow_val:
                                        return True
                                    else:
                                        return False
                                elif i=="/":
                                    olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] / float(ophigh_split[2]))* olmax_val_percentage
                                    if identifier_value<=olmax_val and identifier_value>=oplow_val:
                                        return True
                                    else:
                                        return False
                    elif (type(ophigh)==int or type(ophigh)==float) and (type(oplow)==str):
                        oplow_split=oplow.split()
                        oplow_len=len(oplow_split)
                        ophigh_val=ophigh*olmax_val_percentage
                        if oplow_len==1:
                            oplow_val=self.ship_configs['static_data'][ophigh]*olmin_val_percentage
                            if identifier_value<=ophigh_val and identifier_value>=oplow_val:
                                return True
                            else:
                                return False
                        elif oplow_len==3:
                            for i in oplow_split:
                                if i=="+":
                                    oplow_val=(self.ship_configs['static_data'][oplow_split[0]] + float(oplow_split[2])) * olmin_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=oplow_val:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    oplow_val=(self.ship_configs['static_data'][oplow_split[0]] - float(oplow_split[2]))* olmin_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=oplow_val:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    oplow_val=(self.ship_configs['static_data'][oplow_split[0]] * float(oplow_split[2]))* olmin_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=oplow_val:
                                        return True
                                    else:
                                        return False
                                elif i=="/":
                                    oplow_val=(self.ship_configs['static_data'][oplow_split[0]] / float(oplow_split[2]))* olmin_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=oplow_val:
                                        return True
                                    else:
                                        return False

                elif (olmax_len==1 and olmin_len==1) and (("%" in olmax_split[0]) and ("%" not in olmin_split[0])):
                    olmin_val=self.ship_configs['static_data'][olmin]
                    olmax_val_percentage=float(olmax_split[0][:-1])/100
                    if type(ophigh)==str:
                        ophigh_split=ophigh.split()
                        ophigh_len=len(ophigh_split)
                        if ophigh_len==1:
                            ophigh_val=self.ship_configs['static_data'][ophigh] * olmax_val_percentage
                            if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                        if ophigh_len==3:
                            for i in ophigh_split:
                                if i=="+":
                                    ophigh_val=(self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2]))* olmax_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    ophigh_val=(self.ship_configs['static_data'][ophigh_split[0]] - float(ophigh_split[2]))* olmax_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    ophigh_val=(self.ship_configs['static_data'][ophigh_split[0]] * float(ophigh_split[2]))* olmax_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                                        return True
                                    else:
                                        return False
                                elif i=="/":
                                    ophigh_val=(self.ship_configs['static_data'][ophigh_split[0]] / float(ophigh_split[2]))* olmax_val_percentage
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
                    ophigh_val=self.ship_configs['static_data'][olmax]
                    olmin_val_percentage=float(olmin_split[0][-1])/100
                    if type(oplow)==str:
                        oplow_split=oplow.split()
                        oplow_len=len(oplow_split)  
                        if oplow_len==1:
                            olmin_val=self.ship_configs['static_data'][oplow] * olmin_val_percentage
                            if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                        if oplow_len==3:
                            for i in oplow_split:
                                if i=="+":
                                    olmin_val=(self.ship_configs['static_data'][oplow_split[0]] + float(oplow_split[2]))* olmin_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    olmin_val=(self.ship_configs['static_data'][oplow_split[0]] - float(oplow_split[2]))* olmin_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    olmin_val=(self.ship_configs['static_data'][oplow_split[0]] * float(oplow_split[2]))* olmin_val_percentage
                                    if identifier_value<=ophigh_val and identifier_value>=olmin_val:
                                        return True
                                    else:
                                        return False
                                elif i=="/":
                                    olmin_val=(self.ship_configs['static_data'][oplow_split[0]] / float(oplow_split[2]))* olmin_val_percentage
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

                elif olmin_len==1 and olmax_len==3:  
                    if "%" not in olmin_split[0]:
                        olmin_val=self.ship_configs['static_data'][olmin]
                        for i in olmax_split:
                            if i=="+":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] + float(olmax_split[2])
                                if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                            elif i=="-":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] - float(olmax_split[2])
                                if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                            elif i=="*":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] * float(olmax_split[2])
                                if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                            elif i=="/":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] / float(olmax_split[2])
                                if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                    elif "%" in olmin_split[0]:
                        olmin_val_percentage=float(olmin_split[0][-1])/100
                        self.golab=0
                        for i in olmax_split:
                            if i=="+":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] + float(olmax_split[2])
                                self.golab=olmax_val
                            elif i=="-":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] - float(olmax_split[2])
                                self.golab=olmax_val
                            elif i=="*":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] * float(olmax_split[2])
                                self.golab=olmax_val
                            elif i=="/":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] / float(olmax_split[2])
                                self.golab=olmax_val
                            return self.golab


                        if type(oplow)==str:
                            oplow_split=oplow.split()
                            oplow_len=len(oplow_split)  
                            if oplow_len==1:
                                olmin_val=self.ship_configs['static_data'][oplow] * olmin_val_percentage
                                if identifier_value<=self.golab and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                            if oplow_len==3:
                                for i in oplow_split:
                                    if i=="+":
                                        olmin_val=(self.ship_configs['static_data'][oplow_split[0]] + float(oplow_split[2]))* olmin_val_percentage
                                        if identifier_value<=self.golab and identifier_value>=olmin_val:
                                            return True
                                        else:
                                            return False
                                    elif i=="-":
                                        olmin_val=(self.ship_configs['static_data'][oplow_split[0]] - float(oplow_split[2]))* olmin_val_percentage
                                        if identifier_value<=self.golab and identifier_value>=olmin_val:
                                            return True
                                        else:
                                            return False
                                    elif i=="*":
                                        olmin_val=(self.ship_configs['static_data'][oplow_split[0]] * float(oplow_split[2]))* olmin_val_percentage
                                        if identifier_value<=self.golab and identifier_value>=olmin_val:
                                            return True
                                        else:
                                            return False
                                    elif i=="/":
                                        olmin_val=(self.ship_configs['static_data'][oplow_split[0]] / float(oplow_split[2]))* olmin_val_percentage
                                        if identifier_value<=self.golab and identifier_value>=olmin_val:
                                            return True
                                        else:
                                            return False
                        elif type(oplow)==int or type(oplow)==float:
                            olmin_val=oplow*olmin_val_percentage
                            if identifier_value>=olmin_val and identifier_value<=self.golab:
                                return True
                            else:
                                return False



                elif olmin_len==3 and olmax_len==1:#now here
                    if "%" not in olmax_split[0]:
                        olmax_val=self.ship_configs['static_data'][olmax]
                        for i in olmin_split:
                            if i=="+":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] + float(olmin_split[2])
                                if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                            elif i=="-":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] - float(olmin_split[2])
                                if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                            elif i=="*":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] * float(olmin_split[2])
                                if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                            elif i=="/":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] / float(olmin_split[2])
                                if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                    elif "%" in olmax_split[0]:
                        olmax_val_percentage=float(olmax_split[0][:-1])/100
                        self.golab_1=0
                        for i in olmin_split:
                            if i=="+":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] + float(olmin_split[2])
                                self.golab_1=olmin_val
                            elif i=="-":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] - float(olmin_split[2])
                                self.golab_1=olmin_val
                            elif i=="*":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] * float(olmin_split[2])
                                self.golab_1=olmin_val
                            elif i=="/":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] / float(olmin_split[2])
                                self.golab_1=olmin_val
                            return self.golab_1
                        if type(ophigh)==str:
                            ophigh_split=ophigh.split()
                            ophigh_len=len(ophigh_split)  
                            if ophigh_len==1:
                                olmax_val=self.ship_configs['static_data'][ophigh] * olmax_val_percentage
                                if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                    return True
                                else:
                                    return False
                            if ophigh_len==3:
                                for i in ophigh_split:
                                    if i=="+":
                                        olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2]))* olmax_val_percentage
                                        if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                            return True
                                        else:
                                            return False
                                    elif i=="-":
                                        olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] - float(ophigh_split[2]))* olmax_val_percentage
                                        if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                            return True
                                        else:
                                            return False
                                    elif i=="*":
                                        olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] * float(ophigh_split[2]))* olmax_val_percentage
                                        if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                            return True
                                        else:
                                            return False
                                    elif i=="/":
                                        olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] / float(ophigh_split[2]))* olmax_val_percentage
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

                            


                elif olmin_len==3 and olmax_len==3:
                    self.olmin_len_three_val=0
                    self.olmax_len_three_val=0
                    for i in olmin_split:
                        if i=="+":
                            self.olmin_len_three_val=self.ship_configs['static_data'][olmin_split[0]] + float(olmin_split[2])
                            
                        elif i=="-":
                            self.olmin_len_three_val=self.ship_configs['static_data'][olmin_split[0]] - float(olmin_split[2])
                            
                        elif i=="*":
                            self.olmin_len_three_val=self.ship_configs['static_data'][olmin_split[0]] * float(olmin_split[2])
                            
                        elif i=="/":
                            self.olmin_len_three_val=self.ship_configs['static_data'][olmin_split[0]] / float(olmin_split[2])
                        return self.olmin_len_three_val
                    for j in olmax_split:
                        if j=="+":
                            self.olmin_len_three_val=self.ship_configs['static_data'][olmin_split[0]] + float(olmin_split[2])
                            
                        elif j=="-":
                            self.olmin_len_three_val=self.ship_configs['static_data'][olmin_split[0]] - float(olmin_split[2])
                            
                        elif j=="*":
                            self.olmin_len_three_val=self.ship_configs['static_data'][olmin_split[0]] * float(olmin_split[2])
                            
                        elif j=="/":
                            self.olmin_len_three_val=self.ship_configs['static_data'][olmin_split[0]] / float(olmin_split[2])
                        return self.olmax_len_three_val
                    if identifier_value>=self.olmin_len_three_val and identifier_value<=self.olmax_len_three_val:
                        return True
                    else:
                        return False

                elif olmax_len==5 and olmin_len==1:
                    if "%" not in olmin_split[0]:
                        olmin_val=self.ship_configs['static_data'][olmin]
                        
                    elif "%" in olmin_split[0]:
                        olmin_val_percentage=float(olmin_split[0][-1])/100
                        self.golab=0
                        for i in olmax_split:
                            if olmax_split[1]=="+" and olmax_split[3]=="+":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] + self.ship_configs['static_data'][olmax_split[2]] + self.ship_configs['static_data'][olmax_split[4]]
                                self.golab=olmax_val
                            elif olmax_split[1]=="-" and olmax_split[3]=="-":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] - self.ship_configs['static_data'][olmax_split[2]] - self.ship_configs['static_data'][olmax_split[4]]
                                self.golab=olmax_val
                            elif olmax_split[1]=="*" and olmax_split[3]=="*":
                                olmax_val=self.ship_configs['static_data'][olmax_split[0]] * self.ship_configs['static_data'][olmax_split[2]] * self.ship_configs['static_data'][olmax_split[4]]
                                self.golab=olmax_val
                            return self.golab

                        if type(oplow)==str:
                            oplow_split=oplow.split()
                            oplow_len=len(oplow_split)  
                            if oplow_len==1:
                                olmin_val=self.ship_configs['static_data'][oplow] * olmin_val_percentage
                                if identifier_value<=self.golab and identifier_value>=olmin_val:
                                    return True
                                else:
                                    return False
                            if oplow_len==3:
                                for i in oplow_split:
                                    if i=="+":
                                        olmin_val=(self.ship_configs['static_data'][oplow_split[0]] + float(oplow_split[2]))* olmin_val_percentage
                                        if identifier_value<=self.golab and identifier_value>=olmin_val:
                                            return True
                                        else:
                                            return False
                                    elif i=="-":
                                        olmin_val=(self.ship_configs['static_data'][oplow_split[0]] - float(oplow_split[2]))* olmin_val_percentage
                                        if identifier_value<=self.golab and identifier_value>=olmin_val:
                                            return True
                                        else:
                                            return False
                                    elif i=="*":
                                        olmin_val=(self.ship_configs['static_data'][oplow_split[0]] * float(oplow_split[2]))* olmin_val_percentage
                                        if identifier_value<=self.golab and identifier_value>=olmin_val:
                                            return True
                                        else:
                                            return False
                                    elif i=="/":
                                        olmin_val=(self.ship_configs['static_data'][oplow_split[0]] / float(oplow_split[2]))* olmin_val_percentage
                                        if identifier_value<=self.golab and identifier_value>=olmin_val:
                                            return True
                                        else:
                                            return False
                        elif type(oplow)==int or type(oplow)==float:
                            olmin_val=oplow*olmin_val_percentage
                            if identifier_value>=olmin_val and identifier_value<=self.golab:
                                return True
                            else:
                                return False

                elif olmax_len==1 and olmin_len==5:
                    if "%" not in olmax_split[0]:
                        olmax_val=self.ship_configs['static_data'][olmax]
                        if olmin_split[1]=="+" and olmin_split[3]=="+":
                            olmin_val=self.ship_configs['static_data'][olmin_split[0]] + self.ship_configs['static_data'][olmin_split[2]] + self.ship_configs['static_data'][olmin_split[4]]
                            if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                        elif olmin_split[1]=="-" and olmin_split[3]=="-":
                            olmin_val=self.ship_configs['static_data'][olmin_split[0]] - self.ship_configs['static_data'][olmin_split[2]] - self.ship_configs['static_data'][olmin_split[4]]
                            if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                        elif olmin_split[1]=="*" and olmin_split[3]=="*":
                            olmin_val=self.ship_configs['static_data'][olmin_split[0]] * self.ship_configs['static_data'][olmin_split[2]] * self.ship_configs['static_data'][olmin_split[4]]
                            if identifier_value<=olmax_val and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                    elif "%" in olmax_split[0]:
                        olmax_val_percentage=float(olmax_split[0][:-1])/100
                        self.golab_1=0
                        for i in olmin_split:
                            if olmin_split[1]=="+" and olmin_split[3]=="+":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] + self.ship_configs['static_data'][olmin_split[2]] + self.ship_configs['static_data'][olmin_split[4]]
                                self.golab_1=olmin_val
                            elif olmin_split[1]=="-" and olmin_split[3]=="-":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] - self.ship_configs['static_data'][olmin_split[2]] - self.ship_configs['static_data'][olmin_split[4]]
                                self.golab_1=olmin_val
                            elif olmin_split[1]=="*" and olmin_split[3]=="*":
                                olmin_val=self.ship_configs['static_data'][olmin_split[0]] * self.ship_configs['static_data'][olmin_split[2]] * self.ship_configs['static_data'][olmin_split[4]]
                                self.golab_1=olmin_val
                            return self.golab_1

                        if type(ophigh)==str:
                            ophigh_split=ophigh.split()
                            ophigh_len=len(ophigh_split)  
                            if ophigh_len==1:
                                olmax_val=self.ship_configs['static_data'][ophigh] * olmax_val_percentage
                                if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                    return True
                                else:
                                    return False
                            if ophigh_len==3:
                                for i in ophigh_split:
                                    if i=="+":
                                        olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2]))* olmax_val_percentage
                                        if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                            return True
                                        else:
                                            return False
                                    elif i=="-":
                                        olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] - float(ophigh_split[2]))* olmax_val_percentage
                                        if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                            return True
                                        else:
                                            return False
                                    elif i=="*":
                                        olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] * float(ophigh_split[2]))* olmax_val_percentage
                                        if identifier_value<=olmax_val and identifier_value>=self.golab_1:
                                            return True
                                        else:
                                            return False
                                    elif i=="/":
                                        olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] / float(ophigh_split[2]))* olmax_val_percentage
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

                elif olmax_len==5 and olmin_len==3:
                    self.golab_1=0
                    self.golab=0
                    for i in olmax_split:
                        if olmax_split[1]=="+" and olmax_split[3]=="+":
                            self.golab_1=self.ship_configs['static_data'][olmax_split[0]] + self.ship_configs['static_data'][olmax_split[2]] + self.ship_configs['static_data'][olmax_split[4]]
                            return self.golab_1
                        elif olmax_split[1]=="-" and olmax_split[3]=="-":
                            self.golab_1=self.ship_configs['static_data'][olmax_split[0]] - self.ship_configs['static_data'][olmax_split[2]] - self.ship_configs['static_data'][olmax_split[4]]
                            return self.golab_1
                        elif olmax_split[1]=="*" and olmax_split[3]=="*":
                            self.golab_1=self.ship_configs['static_data'][olmax_split[0]] * self.ship_configs['static_data'][olmax_split[2]] * self.ship_configs['static_data'][olmax_split[4]]                           
                            return self.golab_1
                    
                    for i in olmin_split:
                        if i=="+":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] + float(olmin_split[2])
                            return self.golab
                        elif i=="-":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] - float(olmin_split[2])
                            return self.golab
                        elif i=="*":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] * float(olmin_split[2])
                            return self.golab
                        elif i=="/":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] / float(olmin_split[2])
                            return self.golab
                    if identifier_value>=self.golab and identifier_value<=self.golab_1:
                        return True
                    else:
                        return False 
                
                elif olmax_len==3 and olmin_len==5:
                    self.golab_1=0
                    self.golab=0
                    for j in olmax_split:
                        if j=="+":
                            self.golab_1=self.ship_configs['static_data'][olmin_split[0]] + float(olmin_split[2])
                            return self.golab_1
                        elif j=="-":
                            self.golab_1=self.ship_configs['static_data'][olmin_split[0]] - float(olmin_split[2])
                            return self.golab_1
                        elif j=="*":
                            self.golab_1=self.ship_configs['static_data'][olmin_split[0]] * float(olmin_split[2])
                            return self.golab_1
                        elif j=="/":
                            self.golab_1=self.ship_configs['static_data'][olmin_split[0]] / float(olmin_split[2])
                            return self.golab_1
                    
                    for i in olmin_split:
                        if olmin_split[1]=="+" and olmin_split[3]=="+":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] + self.ship_configs['static_data'][olmin_split[2]] + self.ship_configs['static_data'][olmin_split[4]]
                            return self.golab
                        elif olmin_split[1]=="-" and olmin_split[3]=="-":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] - self.ship_configs['static_data'][olmin_split[2]] - self.ship_configs['static_data'][olmin_split[4]]
                            return self.golab
                            
                        elif olmin_split[1]=="*" and olmin_split[3]=="*":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] * self.ship_configs['static_data'][olmin_split[2]] * self.ship_configs['static_data'][olmin_split[4]]
                            return self.golab
                    if identifier_value>=self.golab and identifier_value<=self.golab_1:
                        return True
                    else:
                        return False
                
                elif olmax_len==5 and olmin_len==5:
                    self.golab_1=0
                    self.golab=0
                    for i in olmax_split:
                        if olmax_split[1]=="+" and olmax_split[3]=="+":
                            self.golab_1=self.ship_configs['static_data'][olmax_split[0]] + self.ship_configs['static_data'][olmax_split[2]] + self.ship_configs['static_data'][olmax_split[4]]
                            return self.golab_1
                        elif olmax_split[1]=="-" and olmax_split[3]=="-":
                            self.golab_1=self.ship_configs['static_data'][olmax_split[0]] - self.ship_configs['static_data'][olmax_split[2]] - self.ship_configs['static_data'][olmax_split[4]]
                            return self.golab_1
                        elif olmax_split[1]=="*" and olmax_split[3]=="*":
                            self.golab_1=self.ship_configs['static_data'][olmax_split[0]] * self.ship_configs['static_data'][olmax_split[2]] * self.ship_configs['static_data'][olmax_split[4]]                           
                            return self.golab_1

                    for i in olmin_split:
                        if olmin_split[1]=="+" and olmin_split[3]=="+":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] + self.ship_configs['static_data'][olmin_split[2]] + self.ship_configs['static_data'][olmin_split[4]]
                            return self.golab
                        elif olmin_split[1]=="-" and olmin_split[3]=="-":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] - self.ship_configs['static_data'][olmin_split[2]] - self.ship_configs['static_data'][olmin_split[4]]
                            return self.golab
                            
                        elif olmin_split[1]=="*" and olmin_split[3]=="*":
                            self.golab=self.ship_configs['static_data'][olmin_split[0]] * self.ship_configs['static_data'][olmin_split[2]] * self.ship_configs['static_data'][olmin_split[4]]
                            return self.golab
                    
                    if identifier_value>=self.golab and identifier_value<=self.golab_1:
                        return True
                    else:
                        return False
                        



            elif (type(olmax)==str) and (type(olmin)==int or type(olmin)==float):
                olmax_split=olmax.split()
                olmax_len=len(olmax_split)
                if (olmax_len==1) and ("%" not in olmax_split[0]):
                    olmax_val=self.ship_configs['static_data'][olmax]
                    if identifier_value<=olmax_val and identifier_value>=olmin:
                        return True
                    else:
                        return False
                elif (olmax_len==1) and ("%" in olmax_split[0]):
                    olmax_val_percentage=float(olmax_split[0][:-1])/100
                    if type(ophigh)==str:
                        ophigh_split=ophigh.split()
                        ophigh_len=len(ophigh_split)  
                        if ophigh_len==1:
                            olmax_val=self.ship_configs['static_data'][ophigh] * olmax_val_percentage
                            if identifier_value<=olmax_val and identifier_value>=olmin:
                                return True
                            else:
                                return False
                        if ophigh_len==3:
                            for i in ophigh_split:
                                if i=="+":
                                    olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2]))* olmax_val_percentage
                                    if identifier_value<=olmax_val and identifier_value>=olmin:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] - float(ophigh_split[2]))* olmax_val_percentage
                                    if identifier_value<=olmax_val and identifier_value>=olmin:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] * float(ophigh_split[2]))* olmax_val_percentage
                                    if identifier_value<=olmax_val and identifier_value>=olmin:
                                        return True
                                    else:
                                        return False
                                elif i=="/":
                                    olmax_val=(self.ship_configs['static_data'][ophigh_split[0]] / float(ophigh_split[2]))* olmax_val_percentage
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

                elif olmax_len==3:
                    for i in olmax_split:
                        if i=="+":
                            olmax_val=self.ship_configs['static_data'][olmax_split[0]] + float(olmax_split[2])
                            if identifier_value<=olmax_val and identifier_value>=olmin:
                                return True
                            else:
                                return False
                        elif i=="-":
                            olmax_val=self.ship_configs['static_data'][olmax_split[0]] - float(olmax_split[2])
                            if identifier_value<=olmax_val and identifier_value>=olmin:
                                return True
                            else:
                                return False
                        elif i=="*":
                            olmax_val=self.ship_configs['static_data'][olmax_split[0]] * float(olmax_split[2])
                            if identifier_value<=olmax_val and identifier_value>=olmin:
                                return True
                            else:
                                return False
                        elif i=="/":
                            olmax_val=self.ship_configs['static_data'][olmax_split[0]] / float(olmax_split[2])
                            if identifier_value<=olmax_val and identifier_value>=olmin:
                                return True
                            else:
                                return False
                elif olmax_len==5:
                    if olmax_split[1]=="+" and olmax_split[3]=="+":
                        olmax_val=self.ship_configs['static_data'][olmax_split[0]] + self.ship_configs['static_data'][olmax_split[2]] + self.ship_configs['static_data'][olmax_split[4]]
                        if identifier_value<=olmax_val and identifier_value>=olmin:
                            return True
                        else:
                            return False
                    elif olmax_split[1]=="-" and olmax_split[3]=="-":
                        olmax_val=self.ship_configs['static_data'][olmax_split[0]] - self.ship_configs['static_data'][olmax_split[2]] - self.ship_configs['static_data'][olmax_split[4]]
                        if identifier_value<=olmax_val and identifier_value>=olmin:
                            return True
                        else:
                            return False
                    elif olmax_split[1]=="*" and olmax_split[3]=="*":
                        olmax_val=self.ship_configs['static_data'][olmax_split[0]] * self.ship_configs['static_data'][olmax_split[2]] * self.ship_configs['static_data'][olmax_split[4]]
                        if identifier_value<=olmax_val and identifier_value>=olmin:
                            return True
                        else:
                            return False

            elif (type(olmax)==int or type(olmax)==float) and (type(olmin)==str):
                olmin_split=olmin.split()
                olmin_len=len(olmin_split)
                if (olmin_len==1) and ("%" not in olmin_split[0]):
                    olmin_val=self.ship_configs['static_data'][olmin]
                    if identifier_value<=olmax and identifier_value>=olmin_val:
                        return True
                    else:
                        return False
                
                elif (olmin_len==1) and ("%" in olmin_split[0]):
                    olmin_val_percentage=float(olmin_split[0][:-1])/100
                    if type(oplow)==str:
                        oplow_split=oplow.split()
                        oplow_len=len(oplow_split)  
                        if oplow_len==1:
                            olmin_val=self.ship_configs['static_data'][oplow] * olmin_val_percentage
                            if identifier_value<=olmax and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                        if oplow_len==3:
                            for i in oplow_split:
                                if i=="+":
                                    olmin_val=(self.ship_configs['static_data'][oplow_split[0]] + float(oplow_split[2]))* olmin_val_percentage
                                    if identifier_value<=olmax and identifier_value>=olmin_val:
                                        return True
                                    else:
                                        return False
                                elif i=="-":
                                    olmin_val=(self.ship_configs['static_data'][oplow_split[0]] - float(oplow_split[2]))* olmin_val_percentage
                                    if identifier_value<=olmax and identifier_value>=olmin_val:
                                        return True
                                    else:
                                        return False
                                elif i=="*":
                                    olmin_val=(self.ship_configs['static_data'][oplow_split[0]] * float(oplow_split[2]))* olmin_val_percentage
                                    if identifier_value<=olmax and identifier_value>=olmin_val:
                                        return True
                                    else:
                                        return False
                                elif i=="/":
                                    olmin_val=(self.ship_configs['static_data'][oplow_split[0]] / float(oplow_split[2]))* olmin_val_percentage
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
                elif olmin_len==3:
                    for i in olmin_split:
                        if i=="+":
                            olmin_val=self.ship_configs['static_data'][olmin_split[0]] + float(olmin_split[2])
                            if identifier_value<=olmax and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                        elif i=="-":
                            olmin_val=self.ship_configs['static_data'][olmin_split[0]] - float(olmin_split[2])
                            if identifier_value<=olmax and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                        elif i=="*":
                            olmin_val=self.ship_configs['static_data'][olmin_split[0]] * float(olmin_split[2])
                            if identifier_value<=olmax and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                        elif i=="/":
                            olmin_val=self.ship_configs['static_data'][olmin_split[0]] / float(olmin_split[2])
                            if identifier_value<=olmax and identifier_value>=olmin_val:
                                return True
                            else:
                                return False
                elif olmin_len==5:
                    if olmin_split[1]=="+" and olmin_split[3]=="+":
                        olmin_val=self.ship_configs['static_data'][olmin_split[0]] + self.ship_configs['static_data'][olmin_split[2]] + self.ship_configs['static_data'][olmin_split[4]]
                        if identifier_value<=olmax and identifier_value>=olmin_val:
                            return True
                        else:
                            return False
                    elif olmin_split[1]=="-" and olmin_split[3]=="-":
                        olmin_val=self.ship_configs['static_data'][olmin_split[0]] - self.ship_configs['static_data'][olmin_split[2]] - self.ship_configs['static_data'][olmin_split[4]]
                        if identifier_value<=olmax and identifier_value>=olmin_val:
                            return True
                        else:
                            return False
                    elif olmin_split[1]=="*" and olmin_split[3]=="*":
                        olmin_val=self.ship_configs['static_data'][olmin_split[0]] * self.ship_configs['static_data'][olmin_split[2]] * self.ship_configs['static_data'][olmin_split[4]]
                        if identifier_value<=olmax and identifier_value>=olmin_val:
                            return True
                        else:
                            return False


            
        




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
                if ophigh_len==1:
                    ophigh_val=self.ship_configs['static_data'][ophigh]
                    if identifier_value<=ophigh_val:
                        return True
                    else:
                        return False
                elif ophigh_len==3:
                    for i in ophigh_split:
                        if i=="+":
                            ophigh_val=self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2])
                            if identifier_value<=ophigh_val:
                                return True
                            else:
                                return False
                        elif i=="-":
                            ophigh_val=self.ship_configs['static_data'][ophigh_split[0]] - float(ophigh_split[2])
                            if identifier_value<=ophigh_val:
                                return True
                            else:
                                return False
                        elif i=="*":
                            ophigh_val=self.ship_configs['static_data'][ophigh_split[0]] * float(ophigh_split[2])
                            if identifier_value<=ophigh_val:
                                return True
                            else:
                                return False
                        elif i=="/":
                            ophigh_val=self.ship_configs['static_data'][ophigh_split[0]] / float(ophigh_split[2])
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
                if oplow_len==1:
                    oplow_val=self.ship_configs['static_data'][oplow]
                    if identifier_value>=oplow_val:
                        return True
                    else:
                        return False
                elif oplow_len==3:
                    for i in oplow_split:
                        if i=="+":
                            oplow_val=self.ship_configs['static_data'][oplow_split[0]] + float(oplow_split[2])
                            if identifier_value>=oplow_val:
                                return True
                            else:
                                return False
                        elif i=="-":
                            oplow_val=self.ship_configs['static_data'][oplow_split[0]] - float(oplow_split[2])
                            if identifier_value>=oplow_val:
                                return True
                            else:
                                return False
                        elif i=="*":
                            oplow_val=self.ship_configs['static_data'][oplow_split[0]] * float(oplow_split[2])
                            if identifier_value>=oplow_val:
                                return True
                            else:
                                return False
                        elif i=="/":
                            oplow_val=self.ship_configs['static_data'][oplow_split[0]] / float(oplow_split[2])
                            if identifier_value>=oplow_val:
                                return True
                            else:
                                return False

        elif pd.isnull(oplow)==False and pd.isnull(ophigh)==False:
            if (type(oplow)==int or type(oplow)==float) and type(ophigh)==str:
                ophigh_split=ophigh.split()
                ophigh_len=len(ophigh_split)
                if ophigh_len==1:
                    ophigh_val=self.ship_configs['static_data'][ophigh]
                    if identifier_value<=ophigh_val and identifier_value>=oplow:
                        return True
                    else:
                        return False
                elif ophigh_len==3:
                    for i in ophigh_split:
                        if i=="+":
                            ophigh_val=self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2])
                            if identifier_value<=ophigh_val and identifier_value>=oplow:
                                return True
                            else:
                                return False
                        elif i=="-":
                            ophigh_val=self.ship_configs['static_data'][ophigh_split[0]] - float(ophigh_split[2])
                            if identifier_value<=ophigh_val and identifier_value>=oplow:
                                return True
                            else:
                                return False
                        elif i=="*":
                            ophigh_val=self.ship_configs['static_data'][ophigh_split[0]] * float(ophigh_split[2])
                            if identifier_value<=ophigh_val and identifier_value>=oplow:
                                return True
                            else:
                                return False
                        elif i=="/":
                            ophigh_val=self.ship_configs['static_data'][ophigh_split[0]] / float(ophigh_split[2])
                            if identifier_value<=ophigh_val and identifier_value>=oplow:
                                return True
                            else:
                                return False

            elif (type(ophigh)==int or type(ophigh)==float) and type(oplow)==str:
                oplow_split=oplow.split()
                oplow_len=len(oplow_split)
                if oplow_len==1:
                    oplow_val=self.ship_configs['static_data'][oplow]
                    if identifier_value>=oplow_val and identifier_value<=ophigh:
                        return True
                    else:
                        return False
                elif oplow_len==3:
                    for i in oplow_split:
                        if i=="+":
                            oplow_val=self.ship_configs['static_data'][oplow_split[0]] + float(oplow_split[2])
                            if identifier_value>=oplow_val and identifier_value<=ophigh:
                                return True
                            else:
                                return False
                        elif i=="-":
                            oplow_val=self.ship_configs['static_data'][oplow_split[0]] - float(oplow_split[2])
                            if identifier_value>=oplow_val and identifier_value<=ophigh:
                                return True
                            else:
                                return False
                        elif i=="*":
                            oplow_val=self.ship_configs['static_data'][oplow_split[0]] * float(oplow_split[2])
                            if identifier_value>=oplow_val and identifier_value<=ophigh:
                                return True
                            else:
                                return False
                        elif i=="/":
                            oplow_val=self.ship_configs['static_data'][oplow_split[0]] / float(oplow_split[2])
                            if identifier_value>=oplow_val and identifier_value<=ophigh:
                                return True
                            else:
                                return False
        
            elif type(oplow)==str and type(ophigh)==str:
                oplowsplit=oplow.split()
                oplow_len=len(oplowsplit)
                ophigh_split=ophigh.split()
                ophigh_len=len(ophigh_split)
                if oplow_len==1 and ophigh_len==1:
                    oplow_val=self.ship_configs['static_data'][oplow]
                    ophigh_val=self.ship_configs['static_data'][ophigh]
                    if identifier_value>=oplow_val and identifier_value<=ophigh_val:
                        return True
                    else:
                        return False
                elif oplow_len==1 and ophigh_len==3:
                    oplow_val=self.ship_configs['static_data'][oplow]
                    for i in ophigh_split:
                        if i=="+":
                            val=self.ship_configs['static_data'][ophigh_split[0]] + float(ophigh_split[2])
                            if identifier_value>=oplow_val and identifier_value<=val:
                                return True
                            else:
                                return False
                        elif i=="-":
                            val=self.ship_configs['static_data'][ophigh_split[0]]-float(ophigh_split[2])
                            if identifier_value>=oplow_val and identifier_value<=val:
                                return True
                            else:
                                return False
                        elif i=="*":
                            val=self.ship_configs['static_data'][ophigh_split[0]]*float(ophigh_split[2])
                            if identifier_value>=oplow_val and identifier_value<=val:
                                return True
                            else:
                                return False
                        elif i=="/":
                            val=self.ship_configs['static_data'][ophigh_split[0]]/float(ophigh_split[2])
                            if identifier_value>=oplow_val and identifier_value<=val:
                                return True
                            else:
                                return False
                elif oplow_len==3 and ophigh_len==1:
                    ophigh_val=self.ship_configs['static_data'][ophigh]
                    for i in oplowsplit:
                        if i=="+":
                            val=self.ship_configs['static_data'][oplowsplit[0]] + float(oplowsplit[2])
                            if identifier_value>=val and identifier_value<=ophigh_val:
                                return True
                            else:
                                return False
                        elif i=="-":
                            val=self.ship_configs['static_data'][oplowsplit[0]]-float(oplowsplit[2])
                            if identifier_value>=val and identifier_value<=ophigh_val:
                                return True
                            else:
                                return False
                        elif i=="*":
                            val=self.ship_configs['static_data'][oplowsplit[0]]*float(oplowsplit[2])
                            if identifier_value>=val and identifier_value<=ophigh_val:
                                return True
                            else:
                                return False
                        elif i=="/":
                            val=self.ship_configs['static_data'][oplowsplit[0]]/float(oplowsplit[2])
                            if identifier_value>=val and identifier_value<=ophigh_val:
                                return True
                            else:
                                return False
                elif oplow_len==3 and ophigh_len==3:
                    self.oplow_val_len_three=0
                    self.ophigh_val_len_three=0
                    for i in oplowsplit:
                        if i=="+":
                            val=self.ship_configs['static_data'][oplowsplit[0]] + float(oplowsplit[2])
                            self.oplow_val_len_three=val
                        elif i=="-":
                            val=self.ship_configs['static_data'][oplowsplit[0]]-float(oplowsplit[2])
                            self.oplow_val_len_three=val
                        elif i=="*":
                            val=self.ship_configs['static_data'][oplowsplit[0]]*float(oplowsplit[2])
                            self.oplow_val_len_three=val
                        elif i=="/":
                            val=self.ship_configs['static_data'][oplowsplit[0]]/float(oplowsplit[2])
                            self.oplow_val_len_three=val
                        return self.oplow_val_len_three
                    for j in ophigh_split:     
                        if j=="+":
                            val=self.ship_configs['static_data'][oplowsplit[0]] + float(oplowsplit[2])
                            self.ophigh_val_len_three=val
                        elif j=="-":
                            val=self.ship_configs['static_data'][oplowsplit[0]]-float(oplowsplit[2])
                            self.ophigh_val_len_three=val
                        elif j=="*":
                            val=self.ship_configs['static_data'][oplowsplit[0]]*float(oplowsplit[2])
                            self.ophigh_val_len_three=val
                        elif j=="/":
                            val=self.ship_configs['static_data'][oplowsplit[0]]/float(oplowsplit[2])
                            self.ophigh_val_len_three=val
                        return self.ophigh_val_len_three
                    if identifier_value<=self.ophigh_val_len_three and identifier_value>=self.oplow_val_len_three:
                        return True
                    else:
                        return False

            elif (type(oplow)==int or type(oplow)==float) and (type(ophigh)==int or type(ophigh)==float):
                if identifier_value<=ophigh and identifier_value>=oplow:
                    return True
                else:
                    return False








"""obj=CheckOutlier("gds")
obj.get_ship_configs()
obj.get_daily_data()
print(obj.Outlierlimitcheck("displ",10))
print(obj.operational_limit("draft_mean",22))"""

