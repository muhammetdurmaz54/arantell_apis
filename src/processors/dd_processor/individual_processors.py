import sys 
sys.path.insert(1,"Project folder path")
from mongoengine import *
from src.db.schema.ship import Ship 
from src.db.schema.ddschema import DailyData
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv

"""client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
database = db
ship_configs_collection = database.get_collection("ship")
ship_configs = ship_configs_collection.find({"ship_imo": 9591301})
daily_data_collection = database.get_collection("daily_data")
daily_data = daily_data_collection.find({"ship_imo": 9591301,"ship_name":"RTM COOOK"})"""
#connect("aranti")


class IndividualProcessors():


    def __init__(self,configs,dd):
        self.ship_configs = configs
        self.daily_data= dd
        pass
    
    """def get_ship_configs(self):
        ship_configs_collection = database.get_collection("ship")
        
        self.ship_configs = ship_configs_collection.find({"ship_imo": 9591301})[0]
    
    def get_daily_data(self):
        daily_data_collection =database.get_collection("daily_data")
        self.daily_data = daily_data_collection.find({"ship_imo": 9591301})[0]"""

    def rpm_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['reported'] = self.daily_data['data']['rpm']                      
        base_dict['processed'] = base_dict['reported']
        #base_dict['z_score'] = self.ship_stats
        
        ophigh=self.ship_configs['data']['rpm']['limits']['ophigh']                                    #rpm with % 
        ophigh_val=self.ship_configs['static_data'][ophigh]
        oplow=self.ship_configs['data']['rpm']['limits']['oplow']
        olmax=self.ship_configs['data']['rpm']['limits']['olmax']
        maxlim=olmax*ophigh_val
        if float(base_dict['reported'])<=maxlim:
            base_dict["isoutlier"] = False
        else:
            base_dict["isoutlier"] = True
                
        return base_dict


    

    def dft_fwd_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['reported'] = self.daily_data['data']['dft_fwd']
        base_dict['processed'] = base_dict['reported'] 
        ophigh=self.ship_configs['data']['dft_fwd']['limits']['ophigh']                                       #dft_fwd with % 
        ophigh_val=self.ship_configs['static_data'][ophigh]
        oplow=self.ship_configs['data']['dft_fwd']['limits']['oplow']
        olmax=self.ship_configs['data']['dft_fwd']['limits']['olmax']
        maxlim=olmax*ophigh_val
        if base_dict['reported']<=maxlim:
            base_dict["isoutlier"] = False 
        else:
            base_dict["isoutlier"]= True
        return base_dict
                        
    def dft_aft_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['reported'] = self.daily_data['data']['dft_aft']
        base_dict['processed'] = base_dict['reported']
        ophigh=self.ship_configs['data']['dft_aft']['limits']['ophigh']
        ophigh_val=self.ship_configs['static_data'][ophigh]                                                         #dft_aft with %
        oplow=self.ship_configs['data']['dft_aft']['limits']['oplow']
        olmax=self.ship_configs['data']['dft_aft']['limits']['olmax']
        maxlim=olmax*ophigh_val
        if base_dict['reported']<=maxlim:
            base_dict["isoutlier"] = False
            
        else:
            base_dict["isoutlier"] = True
        return base_dict
                    

    def draft_mean_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['processed'] = (self.daily_data['data']['dft_aft']+self.daily_data['data']['dft_fwd'])/2
        ophigh=self.ship_configs['data']['draft_mean']['limits']['ophigh']                                               #dftmean with %
        ophigh_val=self.ship_configs['static_data'][ophigh]
        oplow=self.ship_configs['data']['draft_mean']['limits']['oplow']
        olmax=self.ship_configs['data']['draft_mean']['limits']['olmax']
        maxlim=olmax*ophigh_val
        if base_dict['processed']<=maxlim:
            base_dict["isoutlier"] = False
        else:
            base_dict["isoutlier"] = True
                    
        return base_dict


        

    def trim_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['processed'] = self.daily_data['data']['dft_aft']-i['data']['dft_fwd']
        ophigh=self.ship_configs['data']['trim']['limits']['ophigh']
        oplow=self.ship_configs['data']['trim']['limits']['oplow']
        olmax=self.ship_configs['data']['trim']['limits']['olmax']
        if base_dict['processed']<=olmax:
            base_dict["isoutlier"] = False
        else:
            base_dict["isoutlier"] = True
        return base_dict        

    def displ_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['reported'] = self.daily_data['data']['displ']
        base_dict['processed'] = base_dict['reported']
        ophigh=self.ship_configs['data']['displ']['limits']['ophigh']
        oplow=self.ship_configs['data']['displ']['limits']['oplow']
        #olmax=self.ship_configs['data']['displ']['limits']['olmax']     right now using hard formula
        olmax=self.ship_configs['static_data']['ship_lenlbp']*j['static_data']['ship_beam']*j['static_data']['ship_maxsummerdft']
        if base_dict['reported']<=olmax:
            base_dict["isoutlier"] = Falsee   
        else:
            base_dict["isoutlier"] = True
        return base_dict    
        
    def cpress_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['reported'] = self.daily_data['data']['cpress']
        base_dict['processed'] = base_dict['reported']
        ophigh=self.ship_configs['data']['cpress']['limits']['ophigh']
        oplow=self.ship_configs['data']['cpress']['limits']['oplow']
        olmax=self.ship_configs['data']['cpress']['limits']['olmax']
        olmin=self.ship_configs['data']['cpress']['limits']['olmin']
    
        if ((base_dict['reported']<=olmax) & (base_dict['reported']>=olmin)):
            base_dict["isoutlier"] = False                  
        else:
            base_dict["isoutlier"] = True
        return base_dict            
            
    # lat and long left for now
    def dst_last_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['reported'] = self.daily_data['data']['dst_last']
        base_dict['processed'] = base_dict['reported']
        ophigh=self.ship_configs['data']['dst_last']['limits']['ophigh']
        oplow=self.ship_configs['data']['dst_last']['limits']['oplow']
        #olmax=self.ship_configs['data']['dst_last']['limits']['olmax']  using hard formula
        olmax=self.ship_configs['static_data']['ship_maxspeed']*24
        olmin=self.ship_configs['data']['dst_last']['limits']['olmin']
        if base_dict['reported']<=olmax:
            base_dict["isoutlier"] = False   
        else:
            base_dict["isoutlier"] = True
        return base_dict    

    def stm_hrs_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['reported'] = self.daily_data['data']['stm_hrs']
        base_dict['processed'] = base_dict['reported']
        ophigh=self.ship_configs['data']['stm_hrs']['limits']['ophigh']
        oplow=self.ship_configs['data']['stm_hrs']['limits']['oplow']
        olmax=self.ship_configs['data']['stm_hrs']['limits']['olmax']  
        olmin=self.ship_configs['data']['stm_hrs']['limits']['olmin']
        if ((base_dict['reported']<=olmax) & (base_dict['reported']>=olmin)):
            base_dict["isoutlier"] = False                                    
        else:
            base_dict["isoutlier"] = True
        return base_dict            

    def speed_sog_processor(self,base_dict):
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['processed'] = self.daily_data['data']['dst_last']/i['data']['stm_hrs']
        ophigh=self.ship_configs['data']['speed_sog']['limits']['ophigh']
        oplow=self.ship_configs['data']['speed_sog']['limits']['oplow']
        #olmax=self.ship_configs['data']['speed_sog']['limits']['olmax']    using formula
        olmax= j['static_data'][j['data']['speed_sog']['limits']['olmax']]
        olmin=self.ship_configs['data']['speed_sog']['limits']['olmin']
        if base_dict['processed']<=olmax:
            base_dict["isoutlier"] = False                                    
        else:
            base_dict["isoutlier"] = True
        return base_dict    



    def speed_processor(self,base_dict):
        base_dict['processed'] = int(base_dict['reported'])       #not doone
        base_dict['z_score'] = self.ship_stats
        return base_dict


"""obj=IndividualProcessors(ship_configs,daily_data)



obj.get_ship_configs()
obj.get_daily_data()

obj.rpm_processor()"""
