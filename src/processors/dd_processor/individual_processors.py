import sys 
sys.path.insert(1,"arantell_apis-main")
#from mongoengine import *
from src.db.schema.ship import Ship 
from src.db.schema.ddschema import DailyData
import numpy as np


from pymongo import MongoClient
from dotenv import load_dotenv


#not done=(lat,long,speed_stw)


"""client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
    


database = db
ship_configs_collection = database.get_collection("ship")

ship_configs = ship_configs_collection.find({"ship_imo": 9591301})[0]




daily_data_collection = database.get_collection("daily_data")
daily_data = daily_data_collection.find({"ship_imo": 9591301,"ship_name":"RTM COOK"})"""

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
        
    
    def to_beaufort(self,x):
    #Takes number(in knots scale) as input, converts into beaufort scale
        beaufort = {0: 1,
                1: 3,
                2: 6,
                3: 10,
                4: 16,
                5: 21,
                6: 27,
                7: 33,
                8: 40,
                9: 47,
                10: 55,
                11: 63,
                12: 1000}  # max 1000kts

        for k, v in beaufort.items():
            if int(x) <= v:
                return k

    def to_degree(self,x):
    #converts degrees in 16 scale format to angles
        degrees = {'N': '0',
                'E': '90',
                'S': '180',
                'W': '270',
                'NE': '45',
                'SE': '135',
                'SW': '225',
                'NW': '315',
                'NNE': '22',
                'ENE': '67',
                'ESE': '112',
                'SSE': '157',
                'SSW': '202',
                'WSW': '247',
                'WNW': '292',
                'NNW': '337'}

        for k, v in degrees.items():
            if x == k:
                return int(v)

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
        base_dict['processed'] = self.daily_data['data']['dft_aft']-self.daily_data['data']['dft_fwd']
        
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
        olmax=self.ship_configs['static_data']['ship_lenlbp']*self.ship_configs['static_data']['ship_beam']*self.ship_configs['static_data']['ship_maxsummerdft']
        if base_dict['reported']<=olmax:
            base_dict["isoutlier"] = False   
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
        base_dict['processed'] = self.daily_data['data']['dst_last']/self.daily_data['data']['stm_hrs']
        
        ophigh=self.ship_configs['data']['speed_sog']['limits']['ophigh']
        oplow=self.ship_configs['data']['speed_sog']['limits']['oplow']
        #olmax=self.ship_configs['data']['speed_sog']['limits']['olmax']    using formula
        olmax= self.ship_configs['static_data'][self.ship_configs['data']['speed_sog']['limits']['olmax']]
        olmin=self.ship_configs['data']['speed_sog']['limits']['olmin']
        if base_dict['processed']<=olmax:
            base_dict["isoutlier"] = False                                    
        else:
            base_dict["isoutlier"] = True
        return base_dict    


    def vessel_head_processor(self,base_dict):     #vessel_head=rel_deg1
        base_dict=base_dict
        base_dict["isoutlier"] = False
        base_dict['reported']=self.daily_data['data']['vessel_head']
        base_dict['processed']=base_dict['reported']

        ophigh=self.ship_configs['data']['vessel_head']['limits']['ophigh']
        oplow=self.ship_configs['data']['vessel_head']['limits']['oplow']
        olmax= self.ship_configs['data']['vessel_head']['limits']['olmax']
        olmin=self.ship_configs['data']['vessel_head']['limits']['olmin']
        """if base_dict['processed']<=olmax and base_dict['processed']>=olmin:
            base_dict["isoutlier"] = False                                    
        else:
            base_dict["isoutlier"] = True"""
        return base_dict    

    def w_force_processor(self,base_dict):
        base_dict=base_dict
        base_dict['isoutlier']=False
        base_dict['reported']=self.daily_data['data']['w_force']
        base_dict['processed']=base_dict['reported']

        ophigh=self.ship_configs['data']['w_force']['limits']['ophigh']
        oplow=self.ship_configs['data']['w_force']['limits']['oplow']
        olmax= self.ship_configs['data']['w_force']['limits']['olmax']
        olmin=self.ship_configs['data']['w_force']['limits']['olmin']
        """if base_dict['processed']<=olmax:
            base_dict["isoutlier"] = False                                    
        else:
            base_dict["isoutlier"] = True"""
        return base_dict 

 
    def w_dir_processor(self,base_dict):
        base_dict=base_dict
        base_dict['isoutlier']=False
        base_dict['reported']=self.to_degree(self.daily_data['data']['w_dir'])
        base_dict['processed']=base_dict['reported']
        ophigh=self.ship_configs['data']['w_dir']['limits']['ophigh']
        oplow=self.ship_configs['data']['w_dir']['limits']['oplow']
        olmax= self.ship_configs['data']['w_dir']['limits']['olmax']
        olmin=self.ship_configs['data']['w_dir']['limits']['olmin']
        """if base_dict['processed']<=olmax and base_dict['processed']>=olmin:
            base_dict["isoutlier"] = False                                    
        else:
            base_dict["isoutlier"] = True"""
        return base_dict         
        
    """def w_dir_rel_processor(self,base_dict):
        base_dict=base_dict
        base_dict['isoutlier']=False
        wind_dir_deg=self.to_degree(self.daily_data['data']['w_dir'])
        base_dict['processed']=wind_dir_deg - self.daily_data['data']['vessel_head']

        ophigh=self.ship_configs['data']['w_dir_rel']['limits']['ophigh']
        oplow=self.ship_configs['data']['w_dir_rel']['limits']['oplow']
        olmax= self.ship_configs['data']['w_dir_rel']['limits']['olmax']
        olmin=self.ship_configs['data']['w_dir_rel']['limits']['olmin']
        if base_dict['processed']<=olmax and base_dict['processed']>=olmin:
            base_dict["isoutlier"] = False                                    
        else:
            base_dict["isoutlier"] = True
        print(wind_dir_deg - self.daily_data['data']['vessel_head'])
        return base_dict  """          
        

    def speed_processor(self,base_dict):
        base_dict['processed'] = int(base_dict['reported'])       #not doone
        base_dict['z_score'] = self.ship_stats
        return base_dict


"""obj=IndividualProcessors(ship_configs,daily_data)



obj.get_ship_configs()
obj.get_daily_data()"""
#obj.w_dir_processor()
