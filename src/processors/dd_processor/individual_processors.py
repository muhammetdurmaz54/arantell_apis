from datetime import date
import sys
import pandas

from pandas.core import base 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
#from mongoengine import *
from src.db.schema.ship import Ship 
from src.processors.config_extractor.outlier import CheckOutlier
from src.db.schema.ddschema import DailyData
import numpy as np
import math

from pymongo import MongoClient
from dotenv import load_dotenv

import os
import re

from tqdm import tqdm
#from pysimplelog import Logger
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import string
#import statsmodels.api as sm



client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
    


database = db
ship_configs_collection = database.get_collection("ship")

ship_configs = ship_configs_collection.find({"ship_imo": 9591301})[0]




daily_data_collection = database.get_collection("daily_data")
daily_data = daily_data_collection.find({"ship_imo": 9591301,"ship_name":"RTM COOK"})

#connect("aranti")


class IndividualProcessors():


    def __init__(self,configs,dd):
        self.ship_configs = configs
        self.daily_data= dd
        pass
    
    def get_ship_configs(self):
        ship_configs_collection = database.get_collection("ship")
        
        self.ship_configs = ship_configs_collection.find({"ship_imo": 9591301})[0]
    
    def get_daily_data(self):
        daily_data_collection =database.get_collection("daily_data")
        self.daily_data = daily_data_collection.find({"ship_imo": 9591301})[0]
    
    def get_outlier(self,identifier,identifier_value):
        identifier=identifier
        identifier_value=identifier_value
        check_outlier=CheckOutlier(configs=self.ship_configs)
        within_outlier_limit=check_outlier.Outlierlimitcheck(identifier,identifier_value)
        return within_outlier_limit

    def get_operational_outlier(self,identifier,identifier_value):
        identifier=identifier
        identifier_value=identifier_value
        check_outlier=CheckOutlier(configs=self.ship_configs)
        within_operational_limit=check_outlier.operational_limit(identifier,identifier_value)
        return within_operational_limit
        
    
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

    
    def process_sea_st(self,x,nullvalue):
        seaStates = {'CALM GLASSY': 0,
                    'CALM RIPPLED': 1,
                    'SMOOTH': 2,
                    'SLIGHT': 3,
                    'MODERATE': 4,
                    'ROUGH': 5,
                    'VERY ROUGH': 6,
                    'HIGH': 7,
                    'VERY HIGH': 8,
                    'PHENOMENAL': 9}

        x = str(x.upper().strip())
        try:
            if x == "":
                return nullvalue
            if re.search('[\d\.]$', x):
                num = re.sub("[^\d\.]", "", x)
                if num.count('.') > 1:
                    return nullvalue
                elif num == '.':
                    return nullvalue
                else:
                    num = float(num)
                    if 0 <= num <= 9:
                        return str(round(num))
            elif re.search('^\d*$', x):  # numbers
                num = re.sub("[^\d]", "", x)
                if num == "":
                    return nullvalue
                elif 0 <= int(num) <= 9:
                    return num
                else:
                    return nullvalue
            elif len(x) < 3:
                return nullvalue
            else:
                x, _ = process.extractOne(x, list(seaStates.keys()), score_cutoff=0)
                # print("Fuzzying sea states- Results Maching \nTotal rows:", len(score),
                #       "\n100%:", round((score.count(100) / len(score) * 100), 2),
                #       "%, 90%-100%:", round((sum(i for i in score if 90 <= i < 100)) / len(score), 2),
                #       "%, 80%-90%:", round((sum(i for i in score if 80 <= i < 90)) / len(score), 2),
                #       "%, 00-80%:", round((sum(i for i in score if i < 60)) / len(score), 2), "%",
                #       )
                return seaStates.get(x, None)
        except:
            return nullvalue

    def rpm_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="RPM"
        
        base_dict['reported'] = self.daily_data['data']['rpm']                      
        base_dict['processed'] = base_dict['reported']
        #base_dict['z_score'] = self.ship_stats
        base_dict['within_outlier_limits']=self.get_outlier("rpm",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("rpm",base_dict['processed'])
        
        return base_dict


    def dft_fwd_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Draft Fwd"
        
        base_dict['reported'] = self.daily_data['data']['dft_fwd']
        base_dict['processed'] = base_dict['reported'] 
        base_dict['within_outlier_limits']=self.get_outlier("dft_fwd",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("dft_fwd",base_dict['processed'])
        
        return base_dict
                        
    def dft_aft_processor(self,base_dict):
        
        base_dict=base_dict
        base_dict["name"]="draft_aft"
        
        base_dict['reported'] = self.daily_data['data']['dft_aft']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("dft_aft",base_dict['processed'])
        base_dict['within_operational_limits']=None   #self.get_operational_outlier("dft_aft",base_dict['processed']) value missing for now in config staticdata

        
        return base_dict
                    

    def draft_mean_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Draft Mean"
        
        base_dict['processed'] = (self.daily_data['data']['dft_aft']+self.daily_data['data']['dft_fwd'])/2
        base_dict['within_outlier_limits']=self.get_outlier("draft_mean",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("draft_mean",base_dict['processed'])
        
                
        return base_dict


        

    def trim_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Trim"
        
        base_dict['processed'] = self.daily_data['data']['dft_aft']-self.daily_data['data']['dft_fwd']
        base_dict['within_outlier_limits']=self.get_outlier("trim",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("trim",base_dict['processed'])

        return base_dict        

    def displ_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Displacement"
        
        base_dict['reported'] = self.daily_data['data']['displ']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("displ",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("displ",base_dict['processed'])
        return base_dict   
        
    def cpress_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Barometric Pressure"
        
        base_dict['reported'] = self.daily_data['data']['cpress']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("cpress",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("cpress",base_dict['processed'])
        return base_dict           
            
    def dst_last_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Distance since last reporting"
        
        base_dict['reported'] = self.daily_data['data']['dst_last']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("dst_last",base_dict['processed'])
        base_dict['within_operational_limits']=None            #self.get_operational_outlier("dst_last",base_dict['processed']) to be done later once api value is included in function
        return base_dict   

    def stm_hrs_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Steaming Hours since last reporting"
        
        base_dict['reported'] = self.daily_data['data']['stm_hrs']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("stm_hrs",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("stm_hrs",base_dict['processed'])
        return base_dict            

    def speed_sog_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Speed On ground"
        
        base_dict['processed'] = self.daily_data['data']['dst_last']/self.daily_data['data']['stm_hrs']
        base_dict['within_outlier_limits']=self.get_outlier("speed_sog",base_dict['processed'])
        base_dict['within_operational_limits']=None            #self.get_operational_outlier("speed_sog",base_dict['processed']) to be done later once api value is included in function
        return base_dict   


    def vessel_head_processor(self,base_dict):     #vessel_head=rel_deg1
        base_dict=base_dict
        base_dict["name"]="Ship Relative Heading"
        
        base_dict['reported']=self.daily_data['data']['vessel_head']
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("vessel_head",base_dict['processed'])
        base_dict['within_operational_limits']=None            #self.get_operational_outlier("vessel_head",base_dict['processed']) to be done later once api value is included in function
        return base_dict    

    def w_force_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Wind Force (Beaufort)"
       
        base_dict['reported']=self.daily_data['data']['w_force']
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("w_force",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("w_force",base_dict['processed'])
        return base_dict 

 
    def w_dir_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Wind Direction (Compass)"
        
        base_dict['reported']=self.to_degree(self.daily_data['data']['w_dir'].strip())
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("w_dir",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("w_dir",base_dict['processed'])
        return base_dict       
        
    def w_dir_rel_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Relative Wind Direction"
      
        wind_dir_deg=self.to_degree(self.daily_data['data']['w_dir'].strip())
        base_dict['processed']=wind_dir_deg - self.daily_data['data']['vessel_head']
        base_dict['within_outlier_limits']=self.get_outlier("w_dir_rel",base_dict['processed'])
        base_dict['within_operational_limits']=None            #self.get_operational_outlier("w_dir_rel",base_dict['processed']) to be done later once api value is included in function
        return base_dict          
        
    def curknots_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Current"
  
        base_dict['reported']=self.daily_data['data']['curknots']
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("curknots",base_dict['processed'])
        base_dict['within_operational_limits']= None                  #self.get_operational_outlier("curknots",base_dict['processed'])
        return base_dict

    def curfavag_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Current Favouring or Against"
       
        base_dict['reported']=self.daily_data['data']['curfavag']
        base_dict['processed']=base_dict['reported']             
        base_dict['within_outlier_limits']=self.get_outlier("curfavag",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("curfavag",base_dict['processed'])
        return base_dict

    def current_dir_rel_processor(self,base_dict):
        base_dict=base_dict
      
        base_dict['name']="Relative Current Direction"
        report=self.daily_data['data']['curfavag']
        if report==None:
            base_dict['processed']="Null"
        elif report=="F" or report=="f" or report=="+":
            base_dict['processed']=0
        elif report=="A" or report=="a" or report=="-":
            base_dict['processed']=180
        else:
            base_dict['processed']=report
        base_dict['within_outlier_limits']=self.get_outlier("current_dir_rel",base_dict['processed'])
        base_dict['within_operational_limits']= None                  #self.get_operational_outlier("current_dir_rel",base_dict['processed'])
        
        return base_dict

    def current_rel_0_processor(self,base_dict):
        base_dict=base_dict
     
        base_dict['name']="Current 0 Degree cos component"
        report=self.daily_data['data']['curfavag']
        curknots=self.daily_data['data']['curknots']
        if report==None :
            base_dict['processed']="Null"
        elif report=="F" or report=="f" or report=="+":
            current_dir_rel=0
            base_dict['processed']=round(curknots*(math.cos(current_dir_rel/57.3)),3)
        elif report=="A" or report=="a" or report=="-":
            current_dir_rel=180
            base_dict['processed']=round(curknots*(math.cos(current_dir_rel/57.3)),3)
        elif type(report)==int or type(report)==float:
            base_dict['processed']=base_dict['processed']=round(curknots*(math.cos(report/57.3)),3)
        base_dict['within_outlier_limits']=self.get_outlier("current_rel_0",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("current_rel_0",base_dict['processed'])

        return base_dict

    def current_rel_90_processor(self,base_dict):
        base_dict=base_dict
      
        base_dict['name']="Current 0 Degree sine component"
        report=self.daily_data['data']['curfavag']
        curknots=self.daily_data['data']['curknots']
        if report==None:
            base_dict['processed']="Null"
        elif report=="F" or report=="f" or report=="+":
            current_dir_rel=0
            base_dict['processed']=round(curknots*(math.sin(current_dir_rel/57.3)),3)
        elif report=="A" or report=="a" or report=="-":
            current_dir_rel=180
            base_dict['processed']=round(curknots*(math.sin(current_dir_rel/57.3)),3)
        elif type(report)==int or type(report)==float:
            base_dict['processed']=base_dict['processed']=round(curknots*(math.sin(report/57.3)),3)
        base_dict['within_outlier_limits']=self.get_outlier("current_rel_90",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("current_rel_90",base_dict['processed'])

        return base_dict

    def swell_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Wave Height"
        val=self.daily_data['data']['swell']
        if type(val)==str:
            base_dict['reported']=self.to_degree(val.strip())
            
        else:
            base_dict['reported']=self.daily_data['data']['swell']
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("swell",base_dict['processed'])
        base_dict['within_operational_limits']=None    #self.get_operational_outlier("swell",base_dict['processed'])
        return base_dict

    def swelldir_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Swell Direction Compass"
      
        base_dict['reported']=self.to_degree(self.daily_data['data']['swelldir'].strip())
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("swelldir",base_dict['processed'])
        base_dict['within_operational_limits']=None    #self.get_operational_outlier("swelldir",base_dict['processed'])
        return base_dict

    def swell_dir_rel_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Relative Swell Direction"
     
        base_dict['reported']=self.daily_data['data']['swell_dir_rel']
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("swell_dir_rel",base_dict['processed'])
        base_dict['within_operational_limits']=None    #self.get_operational_outlier("swell_dir_rel",base_dict['processed'])
        return base_dict

    def speed_stw_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Speed through Water ( Log Speed)"
        base_dict['reported']=self.daily_data['data']['speed_stw']
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("speed_stw",base_dict['processed'])
        base_dict['within_operational_limits']=None    #self.get_operational_outlier("speed_stw",base_dict['processed'])
        return base_dict

    def swell_rel_0_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Swell Direction-Cos Component"
     
        swell_dir_rel=self.daily_data['data']['swell_dir_rel']
        base_dict['processed']=math.cos(swell_dir_rel/57.3)
        base_dict['within_outlier_limits']=self.get_outlier("swell_rel_0",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("swell_rel_0",base_dict['processed'])
        return base_dict

    def swell_rel_90_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Swell Direction-Sine Component"
     
        swell_dir_rel=self.daily_data['data']['swell_dir_rel']
        base_dict['processed']=math.sin(swell_dir_rel/57.3)
        base_dict['within_outlier_limits']=self.get_outlier("swell_rel_90",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("swell_rel_90",base_dict['processed'])
        return base_dict
    
    def w_rel_0_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Relative Wind Direction COS Component"
  
        wind_dir_deg=self.to_degree(self.daily_data['data']['w_dir'].strip())
        w_dir_rel=wind_dir_deg - self.daily_data['data']['vessel_head']           
        base_dict['processed']=math.cos(w_dir_rel/57.3)    
        base_dict['within_outlier_limits']=self.get_outlier("w_rel_0",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("w_rel_0",base_dict['processed'])               
        return base_dict
        
    def w_rel_90_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Relative Wind Direction SIN Component"
     
        wind_dir_deg=self.to_degree(self.daily_data['data']['w_dir'].strip())
        w_dir_rel=wind_dir_deg - self.daily_data['data']['vessel_head']
        base_dict['processed']=math.sin(w_dir_rel/57.3)
        base_dict['within_outlier_limits']=self.get_outlier("w_rel_90",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("w_rel_90",base_dict['processed']) 
        return base_dict

    def rep_dt_processor(self,base_dict):                        
        base_dict=base_dict
        base_dict["name"]="Report Date"
       
        base_dict['reported']=self.daily_data['data']['rep_dt']
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("rep_dt",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("rep_dt",base_dict['processed'])
        return base_dict

    def rep_time_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Report Time"
    
        base_dict['reported']=self.daily_data['data']['rep_time']
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("rep_time",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("rep_time",base_dict['processed'])
        return base_dict

    def utc_gmt_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Report Datetime UTC"
        base_dict['reported'] = self.daily_data['data']['utc_gmt']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("utc_gmt",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("utc_gmt",base_dict['processed'])
        return base_dict

    def rep_per_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Reporting Period"
        base_dict['reported'] = self.daily_data['data']['rep_per']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("rep_per",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("rep_per",base_dict['processed'])
        return base_dict
    
    def clock_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Clocks Advanced/Retarded"
        base_dict['reported'] = self.daily_data['data']['clock']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("clock",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("clock",base_dict['processed'])  #not done both outlier and operational
        return base_dict
    
    def vsl_load_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Vessel Loaded/Ballast"
        base_dict['reported'] = self.daily_data['data']['vsl_load']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("vsl_load",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("vsl_load",base_dict['processed'])  #not done both outlier and operational
        return base_dict
    
    def cargo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Cargo Loaded - Type"
        base_dict['reported'] = self.daily_data['data']['cargo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("cargo",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("cargo",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def cp_hfo_cons_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Charter Party HFO Cons"
        base_dict['reported'] = self.daily_data['data']['cp_hfo_cons']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("cp_hfo_cons",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("cp_hfo_cons",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def cp_do_cons_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Charter Party DO Cons"
        base_dict['reported'] = self.daily_data['data']['cp_do_cons']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("cp_do_cons",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("cp_do_cons",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def sp_speed_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Charter Party Speed"
        base_dict['reported'] = self.daily_data['data']['sp_speed']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("sp_speed",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("sp_speed",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def cp_windspeed_limit_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Charter Party Max Limit Wind Speed"
        base_dict['reported'] = self.daily_data['data']['cp_windspeed_limit']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("cp_windspeed_limit",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("cp_windspeed_limit",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def cp_seastate_limit_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Charter Party Max Limit Sea State"
        base_dict['reported'] = self.daily_data['data']['cp_seastate_limit']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("cp_seastate_limit",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("cp_seastate_limit",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def cp_currentcurrent_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Current Details"
        base_dict['reported'] = self.daily_data['data']['cp_currentcurrent']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("cp_currentcurrent",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("cp_currentcurrent",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def eta_rsn_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Delay In ETA - Reason"
        base_dict['reported'] = self.daily_data['data']['eta_rsn']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("eta_rsn",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("eta_rsn",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def stp_rsn_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Engine Stop - Reason"
        base_dict['reported'] = self.daily_data['data']['stp_rsn']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("stp_rsn",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("stp_rsn",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def stp_frm_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Engine Stop From"
        base_dict['reported'] = self.daily_data['data']['stp_frm']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("stp_frm",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("stp_frm",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def _dt_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Engine Stop To"
        base_dict['reported'] = self.daily_data['data']['_dt']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("_dt",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("_dt",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def stp_time_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Stoppage Time"
        base_dict['reported'] = self.daily_data['data']['stp_time']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("stp_time",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("stp_time",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def red_rsn_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Reduced Speed  - Reason"
        base_dict['reported'] = self.daily_data['data']['red_rsn']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("red_rsn",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("red_rsn",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def red_frm_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Reduced Speed From"
        base_dict['reported'] = self.daily_data['data']['red_frm']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("red_frm",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("red_frm",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def red_to_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Reduced Speed to"
        base_dict['reported'] = self.daily_data['data']['red_to']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("red_to",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("red_to",base_dict['processed'])  #not done both outlier and operational
        return base_dict
    
    def red_time_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Reduced Speed Time"
        base_dict['reported'] = self.daily_data['data']['red_time']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("red_time",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("red_time",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def dev_rsn_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Reason For Deviation"
        base_dict['reported'] = self.daily_data['data']['dev_rsn']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("dev_rsn",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("dev_rsn",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def dev_dst_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Deviation Distance"
        base_dict['reported'] = self.daily_data['data']['dev_dst']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("dev_dst",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("dev_dst",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def red_dst_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Reduced Speed Distance"
        base_dict['reported'] = self.daily_data['data']['red_dst']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("red_dst",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("red_dst",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def amb_tmp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Ambient Temp"
        base_dict['reported'] = self.daily_data['data']['amb_tmp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("amb_tmp",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("amb_tmp",base_dict['processed'])  
        return base_dict
    
    def amb_tmp_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Ambient Temp"
        base_dict['reported'] = 0  #to be read from API/AIS data
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("amb_tmp_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("amb_tmp_i",base_dict['processed'])  
        return base_dict

    def rel_hum_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Relative Humidity %"
        base_dict['reported'] = self.daily_data['data']['rel_hum']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("rel_hum",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("rel_hum",base_dict['processed'])  
        return base_dict

    def cpress_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Barometric Pressure"
        base_dict['reported'] = 0  #to be read from API/AIS data
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("cpress_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("cpress_i",base_dict['processed'])  
        return base_dict

    def weather_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Weather"
        base_dict['reported'] = self.daily_data['data']['weather']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("weather",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("weather",base_dict['processed'])  
        return base_dict

    def sea_st_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Sea State (Number)"
        base_dict['reported'] = self.process_sea_st(self.daily_data['data']['sea_st'],None)
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("sea_st",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("sea_st",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def sea_st_txt_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Sea State(Text)"
        base_dict['reported'] = self.daily_data['data']['sea_st_txt']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("sea_st_txt",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("sea_st_txt",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def w_force_txt_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Wind Force Beaufort (text)"
        base_dict['reported'] = self.daily_data['data']['w_force_txt']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("w_force_txt",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("w_force_txt",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def w_speed_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="wind_speed_i"
        base_dict['reported'] = 0  #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("w_speed_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("w_speed_i",base_dict['processed'])  #not done both outlier and operational
        return base_dict

    def w_dir_deg_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Wind_direction_i"
        base_dict['reported'] = 0  #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("w_dir_deg_i",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("w_dir_deg",base_dict['processed'])  
        return base_dict

    def w_dir_deg_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Wind Direction Compass Deg"
        base_dict['reported'] = self.to_degree(self.daily_data['data']['w_dir_deg'].strip())
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("w_dir_deg",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("w_dir_deg",base_dict['processed'])  
        return base_dict

    def swell_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Swell Wave Height. API/AIS"
        val=0 #to be read from api
        if type(val)==str:
            base_dict['reported']=self.to_degree(val.strip())
        else:
            base_dict['reported']=val
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("swell_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("swell_i",base_dict['processed'])
        return base_dict

    def swelldir_deg_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="SWell Direction Deg"
        val=self.daily_data['data']['swelldir_deg']
        if type(val)==str:
            base_dict['reported']=self.to_degree(val.strip())
        else:
            base_dict['reported']=val
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("swelldir_deg",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("swelldir_deg",base_dict['processed'])
        return base_dict

    def swelldir_deg_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="SWell Direction Deg API"
        val="NE" #to be read from api
        if type(val)==str:
            base_dict['reported']=self.to_degree(val.strip())
        else:
            base_dict['reported']=val
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("swelldir_deg_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("swelldir_deg_i",base_dict['processed'])
        return base_dict

    def vessel_head_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="VesselRelativeHeading from AIS"
        
        base_dict['reported']=0  #to be read from api
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("vessel_head_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("vessel_head_i",base_dict['processed']) 
        return base_dict

    def w_dir_rel_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Relative Wind Direction from AIS/API"
      
        wind_dir_deg="NE"      #to be read from api
        if type(wind_dir_deg)==str:
            wind_dir_deg_i=self.to_degree(wind_dir_deg.strip())
        else:
            wind_dir_deg_i=wind_dir_deg
        base_dict['processed']=wind_dir_deg_i - self.daily_data['data']['vessel_head']
        base_dict['within_outlier_limits']=self.get_outlier("w_dir_rel_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("w_dir_rel_i",base_dict['processed'])
        return base_dict

    def swell_dir_rel_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']="Relative Swell Direction from AIS/API"
     
        base_dict['reported']=40 #to be read from api
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("swell_dir_rel_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("swell_dir_rel_i",base_dict['processed'])
        return base_dict

    def curknots_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Current from AIS/API"
  
        base_dict['reported']=6     #to be read from api
        base_dict['processed']=base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("curknots_i",base_dict['processed'])
        base_dict['within_operational_limits']= self.get_operational_outlier("curknots_i",base_dict['processed'])
        return base_dict

    def current_dir_rel_i_processor(self,base_dict):
        base_dict=base_dict
      
        base_dict['name']="Relative current direction from AIS/API"
        report="a"  #to be read from api
        if report==None:
            base_dict['processed']="Null"
        elif report=="F" or report=="f" or report=="+":
            base_dict['processed']=0
        elif report=="A" or report=="a" or report=="-":
            base_dict['processed']=180
        else:
            base_dict['processed']=report
        base_dict['within_outlier_limits']=self.get_outlier("current_dir_rel_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("current_dir_rel_i",base_dict['processed'])
        return base_dict

    def dst_last_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Distance since last reporting"
        
        base_dict['reported'] = 0  #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("dst_last_i",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("dst_last_i",base_dict['processed']) 
        return base_dict

    def stm_tot_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Total Steaming Time"
        
        base_dict['reported'] = self.daily_data['data']['stm_tot']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("stm_tot",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("stm_tot",base_dict['processed']) 
        return base_dict

    def dst_tot_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Total Distance Till Date"
        
        base_dict['reported'] = self.daily_data['data']['dst_tot']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("dst_tot",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("dst_tot",base_dict['processed']) 
        return base_dict

    def speed_ship_sog_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Speed Over Ground (Ship report)"
        
        base_dict['reported'] = self.daily_data['data']['speed_ship_sog']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("speed_ship_sog",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("speed_ship_sog",base_dict['processed']) 
        return base_dict

    def speed_sog_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]="Speed On Ground from AIS"
        base_dict['reported']=0 #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("speed_sog_i",base_dict['processed'])
        base_dict['within_operational_limits']=None            #self.get_operational_outlier("speed_sog_i",base_dict['processed']) to be done later once api value is included in function
        return base_dict

    def speed_stw_calc_processor(self,base_dict):
        base_dict=base_dict
        base_dict['name']=self.ship_configs['data']['speed_stw_calc']['variable']
       
        speed_sog=self.daily_data['data']['dst_last']/self.daily_data['data']['stm_hrs']
        report=self.daily_data['data']['curfavag']
        curknots=self.daily_data['data']['curknots']
        if report==None:
            base_dict['processed']=speed_sog
        elif report=="F" or report=="f" or report=="+":
            current_dir_rel=0
            base_dict['processed']=speed_sog-round(curknots*(math.cos(current_dir_rel/57.3)),3)
        elif report=="A" or report=="a" or report=="-":
            current_dir_rel=180
            base_dict['processed']=speed_sog-round(curknots*(math.cos(current_dir_rel/57.3)),3)
        else:
            current_dir_rel=self.daily_data['data']['current_dir_rel']
            base_dict['processed']=speed_sog-round(curknots*(math.cos(current_dir_rel/57.3)),3)
        base_dict['within_outlier_limits']=self.get_outlier("speed_stw_calc",base_dict['processed'])
        base_dict['within_operational_limits']=None    #self.get_operational_outlier("speed_stw_calc",base_dict['processed'])
        return base_dict

    def speed_stw_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['speed_stw_i']['variable']
        base_dict['reported']=0 #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("speed_stw_i",base_dict['processed'])
        base_dict['within_operational_limits']=None            #self.get_operational_outlier("speed_stw_i",base_dict['processed']) to be done later once api value is included in function
        return base_dict

    def real_slip_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['real_slip']['variable']
        base_dict['reported']=(self.daily_data['data']['edist']-self.daily_data['data']['odist'])/self.daily_data['data']['edist']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("real_slip",base_dict['processed'])
        base_dict['within_operational_limits']=None            #self.get_operational_outlier("real_slip",base_dict['processed']) to be done later once api value is included in function
        return base_dict

    def real_slip_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['real_slip_i']['variable']
        base_dict['reported']=0 #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("real_slip_i",base_dict['processed'])
        base_dict['within_operational_limits']=None            #self.get_operational_outlier("real_slip_i",base_dict['processed']) to be done later once api value is included in function
        return base_dict

    def dst_togo_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['dst_togo']['variable']
        base_dict['reported']=self.daily_data['data']['dst_togo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("dst_togo",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("dst_togo",base_dict['processed']) 
        return base_dict

    def stmtogo_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['stmtogo']['variable']
        base_dict['reported']=self.daily_data['data']['stmtogo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("stmtogo",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("stmtogo",base_dict['processed']) 
        return base_dict

    def last_port_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['last_port']['variable']
        base_dict['reported']=self.daily_data['data']['last_port']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("last_port",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("last_port",base_dict['processed']) 
        return base_dict

    def lat_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['lat']['variable']
        base_dict['reported']=self.daily_data['data']['lat']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("lat",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("lat",base_dict['processed']) 
        return base_dict

    def long_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['long']['variable']
        base_dict['reported']=self.daily_data['data']['long']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("long",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("long",base_dict['processed']) 
        return base_dict

    def nextport_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['nextport']['variable']
        base_dict['reported']=self.daily_data['data']['nextport'].strip()
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("nextport",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("nextport",base_dict['processed']) 
        return base_dict

    def estLat_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['estLat']['variable']
        base_dict['reported']=self.daily_data['data']['estLat']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("estLat",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("estLat",base_dict['processed']) 
        return base_dict

    def estLong_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['estLong']['variable']
        base_dict['reported']=self.daily_data['data']['estLong']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("estLong",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("estLong",base_dict['processed']) 
        return base_dict

    def eta_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['eta']['variable']
        base_dict['reported']=self.daily_data['data']['eta']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("eta",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("eta",base_dict['processed']) 
        return base_dict

    def main_lsfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['main_lsfo']['variable']
        base_dict['reported']=self.daily_data['data']['main_lsfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("main_lsfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("main_lsfo",base_dict['processed']) 
        return base_dict

    def main_hsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['main_hsdo']['variable']
        base_dict['reported']=self.daily_data['data']['main_hsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("main_hsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("main_hsdo",base_dict['processed']) 
        return base_dict

    def main_lsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['main_lsdo']['variable']
        base_dict['reported']=self.daily_data['data']['main_lsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("main_lsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("main_lsdo",base_dict['processed']) 
        return base_dict

    def main_hfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['main_hfo']['variable']
        base_dict['reported']=self.daily_data['data']['main_hfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("main_hfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("main_hfo",base_dict['processed']) 
        return base_dict

    def main_do_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['main_do']['variable']
        base_dict['reported']=self.daily_data['data']['main_do']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("main_do",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("main_do",base_dict['processed']) 
        return base_dict

    def main_fuel_per_dst_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['main_fuel_per_dst']['variable']
        
        base_dict['processed'] = self.daily_data['data']['main_hfo']/self.daily_data['data']['dst_last']
        base_dict['within_outlier_limits']=None #self.get_outlier("main_fuel_per_dst",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("main_fuel_per_dst",base_dict['processed']) 
        return base_dict

    def main_total_processor(self,base_dict):
        base_dict=base_dict
        base_dict["name"]=self.ship_configs['data']['main_total']['variable']
        
        base_dict['processed'] = self.daily_data['data']['main_lsfo']+self.daily_data['data']['main_hsdo']+self.daily_data['data']['main_lsdo']+self.daily_data['data']['main_hfo']+self.daily_data['data']['main_do']
        base_dict['within_outlier_limits']=None #self.get_outlier("main_total",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("main_total",base_dict['processed']) 
        return base_dict

    def gen_lsfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['gen_lsfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("gen_lsfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("gen_lsfo",base_dict['processed']) 
        return base_dict

    def gen_hsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['gen_hsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("gen_hsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("gen_hsdo",base_dict['processed']) 
        return base_dict

    def gen_lsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['gen_lsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("gen_lsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("gen_lsdo",base_dict['processed']) 
        return base_dict

    def gen_hfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['gen_hfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("gen_hfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("gen_hfo",base_dict['processed']) 
        return base_dict

    def gen_do_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['gen_do']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("gen_do",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("gen_do",base_dict['processed']) 
        return base_dict

    def boiler_lsfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['boiler_lsfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("boiler_lsfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("boiler_lsfo",base_dict['processed']) 
        return base_dict

    def boiler_hsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['boiler_hsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("boiler_hsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("boiler_hsdo",base_dict['processed']) 
        return base_dict

    def boiler_lsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['boiler_lsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("boiler_lsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("boiler_lsdo",base_dict['processed']) 
        return base_dict

    def boiler_hfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['boiler_hfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("boiler_hfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("boiler_hfo",base_dict['processed']) 
        return base_dict

    def boiler_do_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['boiler_do']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("boiler_do",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("boiler_do",base_dict['processed']) 
        return base_dict

    def avg_lsfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['avg_lsfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("avg_lsfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("avg_lsfo",base_dict['processed']) 
        return base_dict

    def avg_hsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['avg_hsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("avg_hsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("avg_hsdo",base_dict['processed']) 
        return base_dict

    def avg_lsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['avg_lsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("avg_lsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("avg_lsdo",base_dict['processed']) 
        return base_dict

    def avf_hfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['avf_hfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("avf_hfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("avf_hfo",base_dict['processed']) 
        return base_dict

    def avg_do_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['avg_do']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("avg_do",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("avg_do",base_dict['processed']) 
        return base_dict

    def ch_lsfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ch_lsfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ch_lsfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ch_lsfo",base_dict['processed']) 
        return base_dict

    def ch_hsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ch_hsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ch_hsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ch_hsdo",base_dict['processed']) 
        return base_dict

    def ch_lsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ch_lsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ch_lsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ch_lsdo",base_dict['processed']) 
        return base_dict

    def ch_hfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ch_hfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ch_hfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ch_hfo",base_dict['processed']) 
        return base_dict

    def ch_do_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ch_do']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ch_do",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ch_do",base_dict['processed']) 
        return base_dict

    def ig_lsfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ig_lsfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ig_lsfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ig_lsfo",base_dict['processed']) 
        return base_dict

    def ig_hsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ig_hsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ig_hsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ig_hsdo",base_dict['processed']) 
        return base_dict

    def ig_lsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ig_lsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ig_lsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ig_lsdo",base_dict['processed']) 
        return base_dict 

    def ig_hfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ig_hfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ig_hfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ig_hfo",base_dict['processed']) 
        return base_dict

    def ig_do_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ig_do']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ig_do",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ig_do",base_dict['processed']) 
        return base_dict

    def tc_lsfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc_lsfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc_lsfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc_lsfo",base_dict['processed']) 
        return base_dict

    def tc_hsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc_hsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc_hsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc_hsdo",base_dict['processed']) 
        return base_dict

    def tc_lsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc_lsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc_lsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc_lsdo",base_dict['processed']) 
        return base_dict

    def tc_hfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc_hfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc_hfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc_hfo",base_dict['processed']) 
        return base_dict

    def tc_do_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc_do']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc_do",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc_do",base_dict['processed']) 
        return base_dict

    def rob_lsfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['rob_lsfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("rob_lsfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("rob_lsfo",base_dict['processed']) 
        return base_dict

    def rob_hsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['rob_hsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("rob_hsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("rob_hsdo",base_dict['processed']) 
        return base_dict

    def rob_lsdo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['rob_lsdo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("rob_lsdo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("rob_lsdo",base_dict['processed']) 
        return base_dict 

    def rob_hfo_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['rob_hfo']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("rob_hfo",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("rob_hfo",base_dict['processed']) 
        return base_dict

    def rob_do_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['rob_do']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("rob_do",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("rob_do",base_dict['processed']) 
        return base_dict

    def stm_hrd_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['stm_hrd']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("stm_hrd",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("stm_hrd",base_dict['processed']) 
        return base_dict

    def bridge_ctrl_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['bridge_ctrl']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("bridge_ctrl",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("bridge_ctrl",base_dict['processed']) 
        return base_dict

    def goi_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['goi']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("goi",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("goi",base_dict['processed']) 
        return base_dict

    def foi_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['foi']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("foi",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("foi",base_dict['processed']) 
        return base_dict

    def eli_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['eli']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("eli",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("eli",base_dict['processed']) 
        return base_dict

    def es_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['es']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("es",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("es",base_dict['processed']) 
        return base_dict

    def es_calc_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['es_calc']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("es_calc",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("es_calc",base_dict['processed']) 
        return base_dict


    def edist_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['edist']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("edist",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("edist",base_dict['processed']) 
        return base_dict

    def odist_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['odist']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("odist",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("odist",base_dict['processed']) 
        return base_dict

    def pwr_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwr']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwr",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwr",base_dict['processed']) 
        return base_dict

    def slip_o_processor(self,base_dict):
        base_dict=base_dict
        base_dict['processed'] = ((self.daily_data['data']['edist']-self.daily_data['data']['odist'])/self.daily_data['data']['edist'])*100
        base_dict['within_outlier_limits']=None #self.get_outlier("slip_o",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("slip_o",base_dict['processed']) 
        return base_dict

    def pwr_perc_processor(self,base_dict):
        base_dict=base_dict
        try:
            base_dict['reported']=self.daily_data['data']['pwr_perc']
            base_dict['processed']=base_dict['reported']
            if (base_dict['processed']>=((self.daily_data['data']['pwr']/self.ship_configs['static_data']['ship_enginemaxpower'])*100)-1) and (base_dict['processed']<=((self.daily_data['data']['pwr']/self.ship_configs['static_data']['ship_enginemaxpower'])*100)+1):
                base_dict['within_outlier_limits']=True #self.get_outlier("slip_o",base_dict['processed'])
                base_dict['within_operational_limits']=True #self.get_operational_outlier("slip_o",base_dict['processed']) 
            else:
                base_dict['within_outlier_limits']=False #self.get_outlier("slip_o",base_dict['processed'])
                base_dict['within_operational_limits']=False #self.get_operational_outlier("slip_o",base_dict['processed']) 
            return base_dict

        except:
            base_dict['processed']=(self.daily_data['data']['pwr']/self.ship_configs['static_data']['ship_enginemaxpower'])*100
            if (base_dict['processed']>=((self.daily_data['data']['pwr']/self.ship_configs['static_data']['ship_enginemaxpower'])*100)-1) and (base_dict['processed']<=((self.daily_data['data']['pwr']/self.ship_configs['static_data']['ship_enginemaxpower'])*100)+1):
                base_dict['within_outlier_limits']=True #self.get_outlier("slip_o",base_dict['processed'])
                base_dict['within_operational_limits']=True #self.get_operational_outlier("slip_o",base_dict['processed']) 
            else:
                base_dict['within_outlier_limits']=False #self.get_outlier("slip_o",base_dict['processed'])
                base_dict['within_operational_limits']=False #self.get_operational_outlier("slip_o",base_dict['processed']) 
            return base_dict

    def fo_serv_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['fo_serv_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("fo_serv_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("fo_serv_temp",base_dict['processed']) 
        return base_dict

    def fo_sucin_pres1_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['fo_sucin_pres1']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("fo_sucin_pres1",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("fo_sucin_pres1",base_dict['processed']) 
        return base_dict

    def fo_in_pres1_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['fo_in_pres1']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("fo_in_pres1",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("fo_in_pres1",base_dict['processed']) 
        return base_dict

    def fo_in_pres2_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['fo_in_pres2']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("fo_in_pres2",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("fo_in_pres2",base_dict['processed']) 
        return base_dict

    def fo_circ_pres1_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['fo_circ_pres1']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("fo_circ_pres1",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("fo_circ_pres1",base_dict['processed']) 
        return base_dict

    def fo_circ_pres2_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['fo_circ_pres2']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("fo_circ_pres2",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("fo_circ_pres2",base_dict['processed']) 
        return base_dict

    def fo_in_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['fo_in_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("fo_in_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("fo_in_temp",base_dict['processed']) 
        return base_dict

    def fo_in_visc_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['fo_in_visc']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("fo_in_visc",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("fo_in_visc",base_dict['processed']) 
        return base_dict

    def comp_pres1_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres1']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres1",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres1",base_dict['processed']) 
        return base_dict

    def peak_pres1_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres1']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres1",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres1",base_dict['processed']) 
        return base_dict

    def ext_temp1_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp1']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp1",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp1",base_dict['processed']) 
        return base_dict

    def jwme_out_temp1_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp1']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp1",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp1",base_dict['processed']) 
        return base_dict

    def pwme_out_temp1_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp1']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp1",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp1",base_dict['processed']) 
        return base_dict

    def comp_pres2_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres2']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres2",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres2",base_dict['processed']) 
        return base_dict

    def peak_pres2_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres2']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres2",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres2",base_dict['processed']) 
        return base_dict

    def ext_temp2_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp2']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp2",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp2",base_dict['processed']) 
        return base_dict

    def jwme_out_temp2_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp2']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp2",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp2",base_dict['processed']) 
        return base_dict

    def pwme_out_temp2_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp2']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp2",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp2",base_dict['processed']) 
        return base_dict

    def comp_pres3_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres3']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres3",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres3",base_dict['processed']) 
        return base_dict

    def peak_pres3_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres3']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres3",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres3",base_dict['processed']) 
        return base_dict

    def ext_temp3_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp3']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp3",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp3",base_dict['processed']) 
        return base_dict

    def jwme_out_temp3_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp3']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp3",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp3",base_dict['processed']) 
        return base_dict

    def pwme_out_temp3_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp3']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp3",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp3",base_dict['processed']) 
        return base_dict

    def comp_pres4_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres4']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres4",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres4",base_dict['processed']) 
        return base_dict

    def peak_pres4_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres4']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres4",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres4",base_dict['processed']) 
        return base_dict

    def ext_temp4_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp4']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp4",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp4",base_dict['processed']) 
        return base_dict

    def jwme_out_temp4_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp4']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp4",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp4",base_dict['processed']) 
        return base_dict

    def pwme_out_temp4_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp4']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp4",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp4",base_dict['processed']) 
        return base_dict
    
    def comp_pres5_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres5']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres5",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres5",base_dict['processed']) 
        return base_dict

    def peak_pres5_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres5']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres5",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres5",base_dict['processed']) 
        return base_dict

    def ext_temp5_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp5']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp5",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp5",base_dict['processed']) 
        return base_dict

    def jwme_out_temp5_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp5']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp5",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp5",base_dict['processed']) 
        return base_dict

    def pwme_out_temp5_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp5']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp5",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp5",base_dict['processed']) 
        return base_dict

    def comp_pres6_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres6']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres6",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres6",base_dict['processed']) 
        return base_dict

    def peak_pres6_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres6']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres6",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres6",base_dict['processed']) 
        return base_dict

    def ext_temp6_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp6']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp6",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp6",base_dict['processed']) 
        return base_dict

    def jwme_out_temp6_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp6']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp6",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp6",base_dict['processed']) 
        return base_dict

    def pwme_out_temp6_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp6']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp6",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp6",base_dict['processed']) 
        return base_dict

    def comp_pres7_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres7']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres7",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres7",base_dict['processed']) 
        return base_dict

    def peak_pres7_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres7']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres7",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres7",base_dict['processed']) 
        return base_dict

    def ext_temp7_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp7']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp7",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp7",base_dict['processed']) 
        return base_dict

    def jwme_out_temp7_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp7']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp7",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp7",base_dict['processed']) 
        return base_dict

    def pwme_out_temp7_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp7']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp7",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp7",base_dict['processed']) 
        return base_dict

    def comp_pres8_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres8']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres8",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres8",base_dict['processed']) 
        return base_dict

    def peak_pres8_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres8']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres8",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres8",base_dict['processed']) 
        return base_dict

    def ext_temp8_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp8']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp8",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp8",base_dict['processed']) 
        return base_dict

    def jwme_out_temp8_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp8']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp8",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp8",base_dict['processed']) 
        return base_dict

    def pwme_out_temp8_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp8']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp8",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp8",base_dict['processed']) 
        return base_dict

    def comp_pres9_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres9']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres9",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres9",base_dict['processed']) 
        return base_dict

    def peak_pres9_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres9']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres9",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres9",base_dict['processed']) 
        return base_dict

    def ext_temp9_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp9']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp9",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp9",base_dict['processed']) 
        return base_dict

    def jwme_out_temp9_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp9']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp9",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp9",base_dict['processed']) 
        return base_dict

    def pwme_out_temp9_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp9']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp9",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp9",base_dict['processed']) 
        return base_dict

    def comp_pres10_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres10']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres10",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres10",base_dict['processed']) 
        return base_dict

    def peak_pres10_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres10']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres10",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres10",base_dict['processed']) 
        return base_dict

    def ext_temp10_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp10']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp10",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp10",base_dict['processed']) 
        return base_dict

    def jwme_out_temp10_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp10']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp10",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp10",base_dict['processed']) 
        return base_dict

    def pwme_out_temp10_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp10']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp10",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp10",base_dict['processed']) 
        return base_dict

    def comp_pres11_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres11']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres11",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres11",base_dict['processed']) 
        return base_dict

    def peak_pres11_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres11']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres11",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres11",base_dict['processed']) 
        return base_dict

    def ext_temp11_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp11']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp11",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp11",base_dict['processed']) 
        return base_dict

    def jwme_out_temp11_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp11']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp11",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp11",base_dict['processed']) 
        return base_dict

    def pwme_out_temp11_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp11']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp11",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp11",base_dict['processed']) 
        return base_dict

    def comp_pres12_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['comp_pres12']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("comp_pres12",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_pres12",base_dict['processed']) 
        return base_dict

    def peak_pres12_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['peak_pres12']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("peak_pres12",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_pres12",base_dict['processed']) 
        return base_dict

    def ext_temp12_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_temp12']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_temp12",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_temp12",base_dict['processed']) 
        return base_dict

    def jwme_out_temp12_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_temp12']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp12",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp12",base_dict['processed']) 
        return base_dict

    def pwme_out_temp12_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_temp12']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp12",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp12",base_dict['processed']) 
        return base_dict

    def lome_in_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['lome_in_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("lome_in_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("lome_in_temp",base_dict['processed']) 
        return base_dict

    def peak_presavg_processor(self,base_dict):
        base_dict=base_dict
        peak_pres={}
        peak_str="peak_pres"
        for i in range(1,13):
            peak_temp_str=peak_str + str(i)
            if peak_temp_str in self.daily_data['data']: 
                peak_pres[peak_temp_str]=self.daily_data['data'][peak_temp_str]
        if len(peak_pres)>0:
            base_dict['processed'] = sum(peak_pres.values())/len(peak_pres.values())
            base_dict['within_outlier_limits']=None #self.get_outlier("peak_presavg",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_presavg",base_dict['processed']) 
        return base_dict

    def peak_presmax_no_processor(self,base_dict):
        base_dict=base_dict
        peak_pres={}
        peak_str="peak_pres"
        for i in range(1,13):
            peak_temp_str=peak_str + str(i)
            if peak_temp_str in self.daily_data['data']: 
                peak_pres[peak_temp_str]=self.daily_data['data'][peak_temp_str]
        if len(peak_pres)>0:
            base_dict['processed'] = max(peak_pres,key=peak_pres.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("peak_presmax_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_presmax_no",base_dict['processed']) 
        return base_dict

    def peak_presmin_no_processor(self,base_dict):
        base_dict=base_dict
        peak_pres={}
        peak_str="peak_pres"
        for i in range(1,13):
            peak_temp_str=peak_str + str(i)
            if peak_temp_str in self.daily_data['data']: 
                peak_pres[peak_temp_str]=self.daily_data['data'][peak_temp_str]
        if len(peak_pres)>0:
            base_dict['processed'] = min(peak_pres,key=peak_pres.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("peak_presmin_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("peak_presmin_no",base_dict['processed']) 
        return base_dict
    
    def peakpres_maxmin_diff_processor(self,base_dict):
        base_dict=base_dict
        peak_pres={}
        peak_str="peak_pres"
        for i in range(1,13):
            peak_temp_str=peak_str + str(i)
            if peak_temp_str in self.daily_data['data']: 
                peak_pres[peak_temp_str]=self.daily_data['data'][peak_temp_str]
        if len(peak_pres)>1:
            max_val=max(peak_pres.values())
            min_val=min(peak_pres.values())
            base_dict['processed'] = max_val-min_val
            base_dict['within_outlier_limits']=None #self.get_outlier("peakpres_maxmin_diff",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("peakpres_maxmin_diff",base_dict['processed']) 
        elif len(peak_pres)>0 and len(peak_pres)==1 :
            base_dict['processed'] = "only 1 value "
            base_dict['within_outlier_limits']=None #self.get_outlier("peakpres_maxmin_diff",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("peakpres_maxmin_diff",base_dict['processed']) 
        return base_dict

    def comp_presavg_processor(self,base_dict):
        base_dict=base_dict
        comp_pres={}
        comp_str="comp_pres"
        for i in range(1,13):
            comp_temp_str=comp_str + str(i)
            if comp_temp_str in self.daily_data['data']: 
                comp_pres[comp_temp_str]=self.daily_data['data'][comp_temp_str]
        
        if len(comp_pres)>0:
            base_dict['processed'] = sum(comp_pres.values())/len(comp_pres)
            base_dict['within_outlier_limits']=None #self.get_outlier("comp_presavg",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_presavg",base_dict['processed'])
        return base_dict

    def comp_presmax_no_processor(self,base_dict):
        base_dict=base_dict
        comp_pres={}
        comp_str="comp_pres"
        for i in range(1,13):
            comp_temp_str=comp_str + str(i)
            if comp_temp_str in self.daily_data['data']: 
                comp_pres[comp_temp_str]=self.daily_data['data'][comp_temp_str]
        
        if len(comp_pres)>0:
            base_dict['processed'] = max(comp_pres,key=comp_pres.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("comp_presmax_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_presmax_no",base_dict['processed'])
        return base_dict

    def comp_presmin_no_processor(self,base_dict):
        base_dict=base_dict
        comp_pres={}
        comp_str="comp_pres"
        for i in range(1,13):
            comp_temp_str=comp_str + str(i)
            if comp_temp_str in self.daily_data['data']: 
                comp_pres[comp_temp_str]=self.daily_data['data'][comp_temp_str]
        
        if len(comp_pres)>0:
            base_dict['processed'] = min(comp_pres,key=comp_pres.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("comp_presmin_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_presmin_no",base_dict['processed'])
        return base_dict

    def comppres_maxmin_diff_processor(self,base_dict):
        base_dict=base_dict
        comp_pres={}
        comp_str="comp_pres"
        for i in range(1,13):
            comp_temp_str=comp_str + str(i)
            if comp_temp_str in self.daily_data['data']: 
                comp_pres[comp_temp_str]=self.daily_data['data'][comp_temp_str]
        
        if len(comp_pres)>1:
            max_val=max(comp_pres.values())
            min_val=min(comp_pres.values())
            base_dict['processed'] = max_val-min_val
            base_dict['within_outlier_limits']=None #self.get_outlier("comp_presmin_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("comp_presmin_no",base_dict['processed'])

        elif len(comp_pres)>0 and len(comp_pres)==1 :
            base_dict['processed'] = "only 1 value "
            base_dict['within_outlier_limits']=None #self.get_outlier("peakpres_maxmin_diff",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("peakpres_maxmin_diff",base_dict['processed']) 
        return base_dict

    def ext_tempavg_processor(self,base_dict):
        base_dict=base_dict
        ext_temp={}
        ext_str="ext_temp"
        for i in range(1,13):
            ext_temp_str=ext_str + str(i)
            if ext_temp_str in self.daily_data['data']: 
                ext_temp[ext_temp_str]=self.daily_data['data'][ext_temp_str]
        if len(ext_temp)>0:
            base_dict['processed'] = sum(ext_temp.values())/len(ext_temp)
            base_dict['within_outlier_limits']=None #self.get_outlier("ext_tempavg",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_tempavg",base_dict['processed']) 
        return base_dict

    def ext_max_no_processor(self,base_dict):
        base_dict=base_dict
        ext_temp={}
        ext_str="ext_temp"
        for i in range(1,13):
            ext_temp_str=ext_str + str(i)
            if ext_temp_str in self.daily_data['data']: 
                ext_temp[ext_temp_str]=self.daily_data['data'][ext_temp_str]
        if len(ext_temp)>0:
            base_dict['processed'] = max(ext_temp,key=ext_temp.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("ext_max_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_max_no",base_dict['processed']) 
        return base_dict

    def ext_min_no_processor(self,base_dict):
        base_dict=base_dict
        ext_temp={}
        ext_str="ext_temp"
        for i in range(1,13):
            ext_temp_str=ext_str + str(i)
            if ext_temp_str in self.daily_data['data']: 
                ext_temp[ext_temp_str]=self.daily_data['data'][ext_temp_str]
        if len(ext_temp)>0:
            base_dict['processed'] = min(ext_temp,key=ext_temp.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("ext_min_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_min_no",base_dict['processed']) 
        return base_dict

    def ext_maxmin_diff_processor(self,base_dict):
        base_dict=base_dict
        ext_temp={}
        ext_str="ext_temp"
        for i in range(1,13):
            ext_temp_str=ext_str + str(i)
            if ext_temp_str in self.daily_data['data']: 
                ext_temp[ext_temp_str]=self.daily_data['data'][ext_temp_str]
        if len(ext_temp)>1:
            max_val=max(ext_temp.values())
            min_val=min(ext_temp.values())
            base_dict['processed'] =max_val-min_val
            base_dict['within_outlier_limits']=None #self.get_outlier("ext_maxmin_diff",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_maxmin_diff",base_dict['processed']) 

        elif len(ext_temp)>0 and len(ext_temp)==1 :
            base_dict['processed'] = "only 1 value "
            base_dict['within_outlier_limits']=None #self.get_outlier("peakpres_maxmin_diff",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("peakpres_maxmin_diff",base_dict['processed']) 
        return base_dict

    def ext_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ext_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ext_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ext_pres",base_dict['processed']) 
        return base_dict

    def tc1_rpm_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc1_rpm']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc1_rpm",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc1_rpm",base_dict['processed']) 
        return base_dict
    
    def tc1_extin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc1_extin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc1_extin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc1_extin_temp",base_dict['processed']) 
        return base_dict

    def tc1_extout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc1_extout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc1_extout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc1_extout_temp",base_dict['processed']) 
        return base_dict

    def tc1_pres_drop_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc1_pres_drop']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc1_pres_drop",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc1_pres_drop",base_dict['processed']) 
        return base_dict

    def tc1_loblower_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc1_loblower_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc1_loblower_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc1_loblower_temp",base_dict['processed']) 
        return base_dict

    def tc1_loturbine_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc1_loturbine_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc1_loturbine_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc1_loturbine_temp",base_dict['processed']) 
        return base_dict
    
    def tc2_rpm_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc2_rpm']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc2_rpm",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc2_rpm",base_dict['processed']) 
        return base_dict

    def tc2_extin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc2_extin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc2_extin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc2_extin_temp",base_dict['processed']) 
        return base_dict

    def tc2_extout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc2_extout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc2_extout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc2_extout_temp",base_dict['processed']) 
        return base_dict

    def tc2_pres_drop_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc2_pres_drop']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc2_pres_drop",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc2_pres_drop",base_dict['processed']) 
        return base_dict

    def tc2_loblower_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc2_loblower_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc2_loblower_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc2_loblower_temp",base_dict['processed']) 
        return base_dict

    def tc2_loturbine_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc2_loturbine_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc2_loturbine_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc2_loturbine_temp",base_dict['processed']) 
        return base_dict

    def tc3_rpm_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc3_rpm']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc3_rpm",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc3_rpm",base_dict['processed']) 
        return base_dict

    def tc3_extin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc3_extin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc3_extin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc3_extin_temp",base_dict['processed']) 
        return base_dict

    def tc3_extout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc3_extout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc3_extout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc3_extout_temp",base_dict['processed']) 
        return base_dict

    def tc3_pres_drop_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc3_pres_drop']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc3_pres_drop",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc3_pres_drop",base_dict['processed']) 
        return base_dict

    def tc3_loblower_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc3_loblower_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc3_loblower_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc3_loblower_temp",base_dict['processed']) 
        return base_dict

    def tc3_loturbine_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc3_loturbine_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc3_loturbine_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc3_loturbine_temp",base_dict['processed']) 
        return base_dict

    def tc4_rpm_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc4_rpm']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc4_rpm",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc4_rpm",base_dict['processed']) 
        return base_dict

    def tc4_extin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc4_extin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc4_extin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc4_extin_temp",base_dict['processed']) 
        return base_dict

    def tc4_extout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc4_extout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc4_extout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc4_extout_temp",base_dict['processed']) 
        return base_dict

    def tc4_pres_drop_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc4_pres_drop']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc4_pres_drop",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc4_pres_drop",base_dict['processed']) 
        return base_dict

    def tc4_loblower_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc4_loblower_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc4_loblower_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc4_loblower_temp",base_dict['processed']) 
        return base_dict

    def tc4_loturbine_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['tc4_loturbine_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("tc4_loturbine_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("tc4_loturbine_temp",base_dict['processed']) 
        return base_dict

    def sa_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['sa_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("sa_pres",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("sa_pres",base_dict['processed']) 
        return base_dict

    def sa_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['sa_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("sa_temp",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("sa_temp",base_dict['processed']) 
        return base_dict

    def ac1_pres_drop_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac1_pres_drop']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=self.get_outlier("ac1_pres_drop",base_dict['processed'])
        base_dict['within_operational_limits']=self.get_operational_outlier("ac1_pres_drop",base_dict['processed']) 
        return base_dict

    def ac1_in_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac1_in_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac1_in_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac1_in_temp",base_dict['processed']) 
        return base_dict

    def ac1_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac1_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac1_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac1_swin_temp",base_dict['processed']) 
        return base_dict

    def ac1_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac1_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac1_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac1_swout_temp",base_dict['processed']) 
        return base_dict

    def ac2_pres_drop_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac2_pres_drop']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac2_pres_drop",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac2_pres_drop",base_dict['processed']) 
        return base_dict

    def ac2_in_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac2_in_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac2_in_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac2_in_temp",base_dict['processed']) 
        return base_dict

    def ac2_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac2_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac2_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac2_swin_temp",base_dict['processed']) 
        return base_dict

    def ac2_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac2_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac2_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac2_swout_temp",base_dict['processed']) 
        return base_dict

    def ac3_pres_drop_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac3_pres_drop']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac3_pres_drop",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac3_pres_drop",base_dict['processed']) 
        return base_dict

    def ac3_in_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac3_in_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac3_in_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac3_in_temp",base_dict['processed']) 
        return base_dict

    def ac3_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac3_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac3_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac3_swin_temp",base_dict['processed']) 
        return base_dict

    def ac3_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac3_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac3_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac3_swout_temp",base_dict['processed']) 
        return base_dict

    def ac4_pres_drop_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac4_pres_drop']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac4_pres_drop",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac4_pres_drop",base_dict['processed']) 
        return base_dict

    def ac4_in_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac4_in_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac4_in_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac4_in_temp",base_dict['processed']) 
        return base_dict

    def ac4_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac4_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac4_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac4_swin_temp",base_dict['processed']) 
        return base_dict

    def ac4_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['ac4_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("ac4_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("ac4_swout_temp",base_dict['processed']) 
        return base_dict

    def er_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['er_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("er_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("er_temp",base_dict['processed']) 
        return base_dict

    def er_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['er_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("er_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("er_pres",base_dict['processed']) 
        return base_dict

    def er_hum_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['er_hum']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("er_hum",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("er_hum",base_dict['processed']) 
        return base_dict

    def jwme_in_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_in_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_in_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_in_pres",base_dict['processed']) 
        return base_dict

    def jwme_in_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_in_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_in_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_in_temp",base_dict['processed']) 
        return base_dict

    def jwme_out_tempavg_processor(self,base_dict):
        base_dict=base_dict
        jwme_temp={}
        jwme_str="jwme_out_temp"
        for i in range(1,13):
            jwme_temp_str=jwme_str + str(i)
            if jwme_temp_str in self.daily_data['data']: 
                jwme_temp[jwme_temp_str]=self.daily_data['data'][jwme_temp_str]
        if len(jwme_temp)>0:
            base_dict['processed'] = sum(jwme_temp.values())/len(jwme_temp)
            base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_tempavg",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_tempavg",base_dict['processed']) 
        return base_dict

    def jwme_out_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwme_out_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_pres",base_dict['processed']) 
        return base_dict

    def jwme_out_temp_max_no_processor(self,base_dict):
        base_dict=base_dict
        jwme_temp={}
        jwme_str="jwme_out_temp"
        for i in range(1,13):
            jwme_temp_str=jwme_str + str(i)
            if jwme_temp_str in self.daily_data['data']: 
                jwme_temp[jwme_temp_str]=self.daily_data['data'][jwme_temp_str]
        if len(jwme_temp)>0:
            base_dict['processed'] = max(jwme_temp,key=jwme_temp.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp_max_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp_max_no",base_dict['processed']) 
        return base_dict

    def jwme_out_temp_min_no_processor(self,base_dict):
        base_dict=base_dict
        jwme_temp={}
        jwme_str="jwme_out_temp"
        for i in range(1,13):
            jwme_temp_str=jwme_str + str(i)
            if jwme_temp_str in self.daily_data['data']: 
                jwme_temp[jwme_temp_str]=self.daily_data['data'][jwme_temp_str]
        if len(jwme_temp)>0:
            base_dict['processed'] = min(jwme_temp,key=jwme_temp.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("jwme_out_temp_min_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("jwme_out_temp_min_no",base_dict['processed']) 
        return base_dict

    def jwc1_used_no_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc1_used_no']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc1_used_no",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc1_used_no",base_dict['processed']) 
        return base_dict

    def jwc1_fwin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc1_fwin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc1_fwin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc1_fwin_pres",base_dict['processed']) 
        return base_dict

    def jwc1_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc1_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc1_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc1_swin_temp",base_dict['processed']) 
        return base_dict

    def jwc1_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc1_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc1_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc1_swout_temp",base_dict['processed']) 
        return base_dict

    def jwc1_fwin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc1_fwin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc1_fwin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc1_fwin_temp",base_dict['processed']) 
        return base_dict

    def jwc1_fwout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc1_fwout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc1_fwout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc1_fwout_temp",base_dict['processed']) 
        return base_dict

    def jwc1_fwout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc1_fwout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc1_fwout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc1_fwout_pres",base_dict['processed']) 
        return base_dict

    def jwc1_swin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc1_swin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc1_swin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc1_swin_pres",base_dict['processed']) 
        return base_dict

    def jwc1_swout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc1_swout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc1_swout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc1_swout_pres",base_dict['processed']) 
        return base_dict

    def jwc2_used_no_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc2_used_no']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc2_used_no",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc2_used_no",base_dict['processed']) 
        return base_dict

    def jwc2_fwin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc2_fwin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc2_fwin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc2_fwin_pres",base_dict['processed']) 
        return base_dict

    def jwc2_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc2_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc2_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc2_swin_temp",base_dict['processed']) 
        return base_dict

    def jwc2_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc2_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc2_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc2_swout_temp",base_dict['processed']) 
        return base_dict

    def jwc2_fwin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc2_fwin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc2_fwin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc2_fwin_temp",base_dict['processed']) 
        return base_dict

    def jwc2_fwout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc2_fwout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc2_fwout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc2_fwout_temp",base_dict['processed']) 
        return base_dict

    def jwc2_fwout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc2_fwout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc2_fwout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc2_fwout_pres",base_dict['processed']) 
        return base_dict

    def jwc2_swin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc2_swin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc2_swin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc2_swin_pres",base_dict['processed']) 
        return base_dict

    def jwc2_swout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['jwc2_swout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("jwc2_swout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("jwc2_swout_pres",base_dict['processed']) 
        return base_dict

    def pwme_in_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_in_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_in_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_in_pres",base_dict['processed']) 
        return base_dict

    def pwme_in_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_in_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_in_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_in_temp",base_dict['processed']) 
        return base_dict

    def pwme_out_tempavg_processor(self,base_dict):
        base_dict=base_dict
        pwme_temp={}
        pwme_str="pwme_out_temp"
        for i in range(1,13):
            pwme_temp_str=pwme_str + str(i)
            if pwme_temp_str in self.daily_data['data']: 
                pwme_temp[pwme_temp_str]=self.daily_data['data'][pwme_temp_str]
        if len(pwme_temp)>0:
            base_dict['processed'] = sum(pwme_temp.values())/len(pwme_temp)
            base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_tempavg",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_tempavg",base_dict['processed']) 
        return base_dict

    def pwme_out_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwme_out_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_pres",base_dict['processed']) 
        return base_dict

    def pwme_out_temp_max_no_processor(self,base_dict):
        base_dict=base_dict
        pwme_temp={}
        pwme_str="pwme_out_temp"
        for i in range(1,13):
            pwme_temp_str=pwme_str + str(i)
            if pwme_temp_str in self.daily_data['data']: 
                pwme_temp[pwme_temp_str]=self.daily_data['data'][pwme_temp_str]
        if len(pwme_temp)>0:
            base_dict['processed'] = max(pwme_temp,key=pwme_temp.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp_max_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp_max_no",base_dict['processed']) 
        return base_dict

    def pwme_out_temp_min_no_processor(self,base_dict):
        base_dict=base_dict
        pwme_temp={}
        pwme_str="pwme_out_temp"
        for i in range(1,13):
            pwme_temp_str=pwme_str + str(i)
            if pwme_temp_str in self.daily_data['data']: 
                pwme_temp[pwme_temp_str]=self.daily_data['data'][pwme_temp_str]
        if len(pwme_temp)>0:
            base_dict['processed'] = min(pwme_temp,key=pwme_temp.get)
            base_dict['within_outlier_limits']=None #self.get_outlier("pwme_out_temp_min_no",base_dict['processed'])
            base_dict['within_operational_limits']=None #self.get_operational_outlier("pwme_out_temp_min_no",base_dict['processed']) 
        return base_dict

    def pwc1_used_no_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc1_used_no']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc1_used_no",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc1_used_no",base_dict['processed']) 
        return base_dict

    def pwc1_fwin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc1_fwin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc1_fwin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc1_fwin_pres",base_dict['processed']) 
        return base_dict

    def pwc1_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc1_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc1_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc1_swin_temp",base_dict['processed']) 
        return base_dict

    def pwc1_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc1_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc1_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc1_swout_temp",base_dict['processed']) 
        return base_dict

    def pwc1_fwin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc1_fwin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc1_fwin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc1_fwin_temp",base_dict['processed']) 
        return base_dict

    def pwc1_fwout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc1_fwout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc1_fwout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc1_fwout_temp",base_dict['processed']) 
        return base_dict

    def pwc1_fwout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc1_fwout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc1_fwout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc1_fwout_pres",base_dict['processed']) 
        return base_dict

    def pwc1_swin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc1_swin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc1_swin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc1_swin_pres",base_dict['processed']) 
        return base_dict

    def pwc1_swout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc1_swout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc1_swout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc1_swout_pres",base_dict['processed']) 
        return base_dict

    def pwc2_used_no_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc2_used_no']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc2_used_no",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc2_used_no",base_dict['processed']) 
        return base_dict

    def pwc2_fwin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc2_fwin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc2_fwin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc2_fwin_pres",base_dict['processed']) 
        return base_dict

    def pwc2_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc2_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc2_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc2_swin_temp",base_dict['processed']) 
        return base_dict

    def pwc2_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc2_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc2_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc2_swout_temp",base_dict['processed']) 
        return base_dict

    def pwc2_fwin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc2_fwin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc2_fwin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc2_fwin_temp",base_dict['processed']) 
        return base_dict

    def pwc2_fwout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc2_fwout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc2_fwout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc2_fwout_temp",base_dict['processed']) 
        return base_dict

    def pwc2_fwout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc2_fwout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc2_fwout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc2_fwout_pres",base_dict['processed']) 
        return base_dict

    def pwc2_swin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc2_swin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc2_swin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc2_swin_pres",base_dict['processed']) 
        return base_dict

    def pwc2_swout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['pwc2_swout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("pwc2_swout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("pwc2_swout_pres",base_dict['processed']) 
        return base_dict

    def lome_xhd_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['lome_xhd_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("lome_xhd_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("lome_xhd_pres",base_dict['processed']) 
        return base_dict

    def lome_ib_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['lome_ib_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("lome_ib_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("lome_ib_pres",base_dict['processed']) 
        return base_dict

    def lome_cc_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['lome_cc_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("lome_cc_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("lome_cc_pres",base_dict['processed']) 
        return base_dict

    def lome_in_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['lome_in_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("lome_in_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("lome_in_temp",base_dict['processed']) 
        return base_dict
        
    def lome_out_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['lome_out_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("lome_out_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("lome_out_temp",base_dict['processed']) 
        return base_dict

    def lome_cs_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['lome_cs_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("lome_cs_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("lome_cs_pres",base_dict['processed']) 
        return base_dict

    def loc_used_no_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc_used_no']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc_used_no",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc_used_no",base_dict['processed']) 
        return base_dict

    def loc1_fwin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc1_fwin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc1_fwin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc1_fwin_pres",base_dict['processed']) 
        return base_dict

    def loc1_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc1_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc1_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc1_swin_temp",base_dict['processed']) 
        return base_dict

    def loc1_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc1_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc1_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc1_swout_temp",base_dict['processed']) 
        return base_dict

    def loc1_loin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc1_loin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc1_loin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc1_loin_temp",base_dict['processed']) 
        return base_dict

    def loc1_loout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc1_loout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc1_loout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc1_loout_temp",base_dict['processed']) 
        return base_dict

    def loc1_loout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc1_loout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc1_loout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc1_loout_pres",base_dict['processed']) 
        return base_dict

    def loc1_swin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc1_swin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc1_swin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc1_swin_pres",base_dict['processed']) 
        return base_dict

    def loc1_swout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc1_swout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc1_swout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc1_swout_pres",base_dict['processed']) 
        return base_dict

    def loc2_fwin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc2_fwin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc2_fwin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc2_fwin_pres",base_dict['processed']) 
        return base_dict

    def loc2_swin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc2_swin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc2_swin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc2_swin_temp",base_dict['processed']) 
        return base_dict

    def loc2_swout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc2_swout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc2_swout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc2_swout_temp",base_dict['processed']) 
        return base_dict

    def loc2_loin_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc2_loin_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc2_loin_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc2_loin_temp",base_dict['processed']) 
        return base_dict

    def loc2_loout_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc2_loout_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc2_loout_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc2_loout_temp",base_dict['processed']) 
        return base_dict

    def loc2_loout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc2_loout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc2_loout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc2_loout_pres",base_dict['processed']) 
        return base_dict

    def loc2_swin_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc2_swin_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc2_swin_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc2_swin_pres",base_dict['processed']) 
        return base_dict

    def loc2_swout_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['loc2_swout_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("loc2_swout_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("loc2_swout_pres",base_dict['processed']) 
        return base_dict

    def sw1_used_no_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['sw1_used_no']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("sw1_used_no",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("sw1_used_no",base_dict['processed']) 
        return base_dict

    def sw1_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['sw1_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("sw1_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("sw1_pres",base_dict['processed']) 
        return base_dict

    def sw1_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['sw1_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("sw1_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("sw1_temp",base_dict['processed']) 
        return base_dict

    def sw2_used_no_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['sw2_used_no']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("sw2_used_no",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("sw2_used_no",base_dict['processed']) 
        return base_dict

    def sw2_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['sw2_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("sw2_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("sw2_pres",base_dict['processed']) 
        return base_dict

    def sw2_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['sw2_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("sw2_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("sw2_temp",base_dict['processed']) 
        return base_dict

    def gen_sw_pres_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['gen_sw_pres']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("gen_sw_pres",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("gen_sw_pres",base_dict['processed']) 
        return base_dict

    def gen_sw_temp_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.daily_data['data']['gen_sw_temp']
        base_dict['processed'] = base_dict['reported']
        base_dict['within_outlier_limits']=None #self.get_outlier("gen_sw_temp",base_dict['processed'])
        base_dict['within_operational_limits']=None #self.get_operational_outlier("gen_sw_temp",base_dict['processed']) 
        return base_dict

    #total_pwr,sa_hum,ac1_water_sep,ac2_water_sep,ac3_water_sep,ac4_water_sep, formula to be known

    #to be asked N+E values storing and list to be added in ship configs

    """def speed_processor(self,base_dict):
        base_dict['processed'] = int(base_dict['reported'])       #not done
        base_dict['z_score'] = self.ship_stats
        return base_dict"""

#obj=IndividualProcessors(ship_configs,daily_data)





#obj.get_ship_configs()
#obj.get_daily_data()
#obj.rep_dt_processor()
#list of source identifiers removed=['dev_rsn','dev_dst','red_dst','rel_hum','sea_st_txt','w_dir_deg','gen_lsfo','gen_hsdo','gen_lsdo','gen_hfo','gen_do','boiler_lsfo','boiler_hsdo','boiler_lsdo','boiler_hfo','boiler_do','avg_lsfo','avg_hsdo','avg_lsdo','avf_hfo','avg_do','stm_hrd','rpm_e','ELI','CALC(es_calc)','SLIP_O','PWR%','tchrpm1','SFOC','SCOC','SCCOC','tchrpm2']