from datetime import date
import sys
import pandas
from pandas.core import base
from pandas.core.dtypes.missing import isnull 
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


class IndividualProcessors():


    def __init__(self,configs,dd):
        self.ship_configs = configs
        self.daily_data= dd
        pass
        
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

    def return_variable(self,string):
        "returns the variable(identifiers) in the formula"
        txt = string
        txt_search=[i for i in re.findall("[a-zA-Z0-9_]+",txt) if not i.isdigit()]
        return txt_search

    def base_formula_daily_data(self,formula_string):
        "base formula function that is read from shipconfig. fetches variable data present in shipstatic and dailydata to populate maindb"
        list_var=self.return_variable(formula_string)
        static_data=self.ship_configs['static_data']
        daily_data=self.daily_data['data']
        temp_dict={}
        for i in list_var:
            if i in static_data and pandas.isnull(static_data[i])==False:
                temp_dict[i]=static_data[i]
            elif i in daily_data and pandas.isnull(daily_data[i])==False:
                temp_dict[i]=daily_data[i]
            else:
                temp_dict[i]=None        
        return eval(self.eval_val_replacer(temp_dict, formula_string))
        
    def eval_val_replacer(self,temp_dict, string):
        "replaces the identifier with their value"
        for key in temp_dict.keys():
            string = re.sub(r"\b" + key + r"\b", str(temp_dict[key]), string, flags=re.I)
        return string


    def base_individual_processor(self,identifier,base_dict):
        "base individual processor for those which has a common structure  or repetetive structure and formulas based on daily data availability. not for those which includes degree or beaufort function"
        base_dict=base_dict
        derived=self.ship_configs['data'][identifier]['Derived']
        source_identifier=self.ship_configs['data'][identifier]['source_idetifier']
        static_data=self.ship_configs['data'][identifier]['static_data']
        if derived==False or pandas.isnull(derived):
            if pandas.isnull(source_identifier)==False:
                if identifier in self.daily_data['data']:
                    base_dict['reported'] = self.daily_data['data'][identifier]   
                    if type(base_dict['reported'])==str:
                        base_dict['reported']=base_dict['reported'].strip()
                    else:
                        base_dict['reported']=base_dict['reported'] 
                    base_dict['is_read']=True
                    base_dict['is_processed']=False
                    base_dict['processed'] = base_dict['reported']
                elif identifier not in self.daily_data['data']:
                    base_dict['is_read']=False
                    base_dict['is_processed']=False              
            elif pandas.isnull(source_identifier):
                if pandas.isnull(static_data):
                    base_dict['reported']=None
                    base_dict['is_read']=None
                    base_dict['is_processed']=None
        elif derived==True and pandas.isnull(derived)==False:
            if pandas.isnull(source_identifier):
                if pandas.isnull(static_data)==False:
                    try:
                        base_dict['processed']=self.base_formula_daily_data(static_data) 
                        base_dict['is_read']=False
                        base_dict['is_processed']=True
                    except:
                        base_dict['processed']=None           
                elif pandas.isnull(static_data):
                    base_dict['reported']=None
                    base_dict['is_read']=None
                    base_dict['is_processed']=None
            elif pandas.isnull(source_identifier)==False:
                if identifier in self.daily_data['data']:
                    base_dict['reported'] = self.daily_data['data'][identifier]   
                    if type(base_dict['reported'])==str:
                        base_dict['reported']=base_dict['reported'].strip()
                    else:
                        base_dict['reported']=base_dict['reported'] 
                    base_dict['is_read']=True
                    base_dict['is_processed']=False
                    base_dict['processed'] = base_dict['reported']
                elif identifier not in self.daily_data['data']:
                    base_dict['is_read']=False
                    base_dict['is_processed']=False              
        return base_dict        

    def base_avg_minmax_evaluator(self,string):
        ext_temp={}
        ext_str=string
        me_unit_no=self.ship_configs['static_data']['ship_tot_unit_nos']
        for i in range(1,me_unit_no+1):
            ext_temp_str=ext_str+str(i)
            if ext_temp_str in self.daily_data['data']: 
                ext_temp[ext_temp_str]=self.daily_data['data'][ext_temp_str]
        return ext_temp

    def rpm_processor(self,base_dict):
        return self.base_individual_processor('rpm',base_dict)

    def dft_fwd_processor(self,base_dict):
        return self.base_individual_processor('dft_fwd',base_dict)
                        
    def dft_aft_processor(self,base_dict):
        return self.base_individual_processor('dft_aft',base_dict)             

    def draft_mean_processor(self,base_dict):
        return self.base_individual_processor("draft_mean",base_dict)

    def trim_processor(self,base_dict):
        return self.base_individual_processor("trim",base_dict)      

    def displ_processor(self,base_dict):
        return self.base_individual_processor('displ',base_dict)   
        
    def cpress_processor(self,base_dict):
        return self.base_individual_processor('cpress',base_dict)         
            
    def dst_last_processor(self,base_dict):            
        return self.base_individual_processor('dst_last',base_dict)  

    def stm_hrs_processor(self,base_dict):
        return self.base_individual_processor('stm_hrs',base_dict)            

    def speed_sog_processor(self,base_dict):
        return self.base_individual_processor('speed_sog',base_dict)
    
    def vessel_head_processor(self,base_dict):     #vessel_head=rel_deg1           
        return self.base_individual_processor('vessel_head',base_dict)    

    def w_force_processor(self,base_dict):
        return self.base_individual_processor('w_force',base_dict) 
 
    def w_dir_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.to_degree(self.daily_data['data']['w_dir'].strip())
        base_dict['processed']=base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict       
        
    def w_dir_rel_processor(self,base_dict):
        base_dict=base_dict
        wind_dir_deg=self.to_degree(self.daily_data['data']['w_dir'].strip())
        base_dict['processed']=wind_dir_deg - self.daily_data['data']['vessel_head']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict          
        
    def curknots_processor(self,base_dict):                 
        return self.base_individual_processor('curknots',base_dict)

    def curfavag_processor(self,base_dict):
        return self.base_individual_processor('curfavag',base_dict)

    def current_dir_rel_processor(self,base_dict):
        base_dict=base_dict
        report=self.daily_data['data']['curfavag']
        if report==None:
            base_dict['processed']="Null"
        elif report=="F" or report=="f" or report=="+":
            base_dict['processed']=0
        elif report=="A" or report=="a" or report=="-":
            base_dict['processed']=180
        else:
            base_dict['processed']=report   
        base_dict['is_read']=False
        base_dict['is_processed']=True     
        return base_dict

    def current_rel_0_processor(self,base_dict):
        base_dict=base_dict
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
        base_dict['is_read']=False
        base_dict['is_processed']=True
        return base_dict

    def current_rel_90_processor(self,base_dict):
        base_dict=base_dict
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
        base_dict['is_read']=False
        base_dict['is_processed']=True
        return base_dict

    def swell_processor(self,base_dict):
        return self.base_individual_processor('swell',base_dict)

    def swelldir_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=self.to_degree(self.daily_data['data']['swelldir'].strip())
        base_dict['processed']=base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def swell_dir_rel_processor(self,base_dict):
        return self.base_individual_processor('swell_dir_rel',base_dict)

    def speed_stw_processor(self,base_dict):
        return self.base_individual_processor('speed_stw',base_dict)

    def swell_rel_0_processor(self,base_dict):
        base_dict=base_dict
        swell_dir_rel=self.daily_data['data']['swell_dir_rel']
        base_dict['processed']=math.cos(swell_dir_rel/57.3)
        base_dict['is_read']=False
        base_dict['is_processed']=True
        return base_dict

    def swell_rel_90_processor(self,base_dict):
        base_dict=base_dict
        swell_dir_rel=self.daily_data['data']['swell_dir_rel']
        base_dict['processed']=math.sin(swell_dir_rel/57.3)
        base_dict['is_read']=False
        base_dict['is_processed']=True
        return base_dict
    
    def w_rel_0_processor(self,base_dict):
        base_dict=base_dict
        wind_dir_deg=self.to_degree(self.daily_data['data']['w_dir'].strip())
        w_dir_rel=wind_dir_deg - self.daily_data['data']['vessel_head']           
        base_dict['processed']=math.cos(w_dir_rel/57.3)     
        base_dict['is_read']=False
        base_dict['is_processed']=True
        return base_dict
        
    def w_rel_90_processor(self,base_dict):
        base_dict=base_dict    
        wind_dir_deg=self.to_degree(self.daily_data['data']['w_dir'].strip())
        w_dir_rel=wind_dir_deg - self.daily_data['data']['vessel_head']
        base_dict['processed']=math.sin(w_dir_rel/57.3)
        base_dict['is_read']=False
        base_dict['is_processed']=True
        return base_dict

    def rep_dt_processor(self,base_dict):                        
        return self.base_individual_processor('rep_dt',base_dict)

    def rep_time_processor(self,base_dict):
        return self.base_individual_processor('rep_time',base_dict)

    def utc_gmt_processor(self,base_dict):
        return self.base_individual_processor('utc_gmt',base_dict)

    def rep_per_processor(self,base_dict):
        return self.base_individual_processor('rep_per',base_dict)
    
    def clock_processor(self,base_dict):
        return self.base_individual_processor('clock',base_dict)
    
    def vsl_load_processor(self,base_dict):
        return self.base_individual_processor('vsl_load',base_dict)
    
    def cargo_processor(self,base_dict):
        return self.base_individual_processor('cargo',base_dict)

    def cp_hfo_cons_processor(self,base_dict):
        return self.base_individual_processor('cp_hfo_cons',base_dict)

    def cp_do_cons_processor(self,base_dict):
        return self.base_individual_processor('cp_do_cons',base_dict)

    def sp_speed_processor(self,base_dict):
        return self.base_individual_processor('sp_speed',base_dict)

    def cp_windspeed_limit_processor(self,base_dict):
        return self.base_individual_processor('cp_windspeed_limit',base_dict)

    def cp_seastate_limit_processor(self,base_dict):
        return self.base_individual_processor('cp_seastate_limit',base_dict)

    def cp_currentcurrent_processor(self,base_dict):
        return self.base_individual_processor('cp_currentcurrent',base_dict)

    def eta_rsn_processor(self,base_dict):
        return self.base_individual_processor('eta_rsn',base_dict)

    def stp_rsn_processor(self,base_dict):
        return self.base_individual_processor('stp_rsn',base_dict)

    def stp_frm_processor(self,base_dict):
        return self.base_individual_processor('stp_frm',base_dict)

    def _dt_processor(self,base_dict):
        return self.base_individual_processor('_dt',base_dict)

    def stp_time_processor(self,base_dict):
        return self.base_individual_processor('stp_time',base_dict)

    def red_rsn_processor(self,base_dict):
        return self.base_individual_processor('red_rsn',base_dict)

    def red_frm_processor(self,base_dict):
        return self.base_individual_processor('red_frm',base_dict)

    def red_to_processor(self,base_dict):
        return self.base_individual_processor('red_to',base_dict)
    
    def red_time_processor(self,base_dict):
        return self.base_individual_processor('red_time',base_dict)

    def dev_rsn_processor(self,base_dict):
        return self.base_individual_processor('dev_rsn',base_dict)

    def dev_dst_processor(self,base_dict):
        return self.base_individual_processor('dev_dst',base_dict)

    def red_dst_processor(self,base_dict):
        return self.base_individual_processor('red_dst',base_dict)

    def amb_tmp_processor(self,base_dict):
        return self.base_individual_processor('amb_tmp',base_dict)
    
    def amb_tmp_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported'] = 0  #to be read from API/AIS data
        base_dict['processed'] = base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def rel_hum_processor(self,base_dict):
        return self.base_individual_processor('rel_hum',base_dict)

    def cpress_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported'] = 0  #to be read from API/AIS data
        base_dict['processed'] = base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def weather_processor(self,base_dict):
        return self.base_individual_processor('weather',base_dict)

    def sea_st_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported'] = self.process_sea_st(self.daily_data['data']['sea_st'],None)
        base_dict['processed'] = base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def sea_st_txt_processor(self,base_dict):
        return self.base_individual_processor('sea_st_txt',base_dict)

    def w_force_txt_processor(self,base_dict):
        return self.base_individual_processor('w_force_txt',base_dict)

    def w_speed_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported'] = 0  #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def w_dir_deg_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported'] = 0  #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False  
        return base_dict

    def w_dir_deg_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported'] = self.to_degree(self.daily_data['data']['w_dir_deg'].strip())
        base_dict['processed'] = base_dict['reported'] 
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def swell_i_processor(self,base_dict):
        base_dict=base_dict
        val=0 #to be read from api
        if type(val)==str:
            base_dict['reported']=self.to_degree(val.strip())
        else:
            base_dict['reported']=val
        base_dict['processed']=base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def swelldir_deg_processor(self,base_dict):
        base_dict=base_dict
        val=self.daily_data['data']['swelldir_deg']
        if type(val)==str:
            base_dict['reported']=self.to_degree(val.strip())
        else:
            base_dict['reported']=val
        base_dict['processed']=base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def swelldir_deg_i_processor(self,base_dict):
        base_dict=base_dict
        val="NE" #to be read from api
        if type(val)==str:
            base_dict['reported']=self.to_degree(val.strip())
        else:
            base_dict['reported']=val
        base_dict['processed']=base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def vessel_head_i_processor(self,base_dict):
        base_dict=base_dict       
        base_dict['reported']=0  #to be read from api
        base_dict['processed']=base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def w_dir_rel_i_processor(self,base_dict):
        base_dict=base_dict      
        wind_dir_deg="NE"      #to be read from api
        if type(wind_dir_deg)==str:
            wind_dir_deg_i=self.to_degree(wind_dir_deg.strip())
        else:
            wind_dir_deg_i=wind_dir_deg
        base_dict['processed']=wind_dir_deg_i - self.daily_data['data']['vessel_head']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def swell_dir_rel_i_processor(self,base_dict):
        base_dict=base_dict   
        base_dict['reported']=40 #to be read from api
        base_dict['processed']=base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def curknots_i_processor(self,base_dict):
        base_dict=base_dict 
        base_dict['reported']=6     #to be read from api
        base_dict['processed']=base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def current_dir_rel_i_processor(self,base_dict):
        base_dict=base_dict
        report="a"  #to be read from api
        if report==None:
            base_dict['processed']="Null"
        elif report=="F" or report=="f" or report=="+":
            base_dict['processed']=0
        elif report=="A" or report=="a" or report=="-":
            base_dict['processed']=180
        else:
            base_dict['processed']=report
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def dst_last_i_processor(self,base_dict):
        base_dict=base_dict       
        base_dict['reported'] = 0  #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def stm_tot_processor(self,base_dict):
        return self.base_individual_processor('stm_tot',base_dict)

    def dst_tot_processor(self,base_dict):
        return self.base_individual_processor('dst_tot',base_dict)

    def speed_ship_sog_processor(self,base_dict):
        return self.base_individual_processor('speed_ship_sog',base_dict)

    def speed_sog_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=0 #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def speed_stw_calc_processor(self,base_dict):
        base_dict=base_dict      
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
        base_dict['is_read']=False
        base_dict['is_processed']=True
        return base_dict

    def speed_stw_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=0 #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def real_slip_processor(self,base_dict):
        return self.base_individual_processor('real_slip',base_dict)
    
    def real_slip_calc_processor(self,base_dict):
        return self.base_individual_processor('real_slip_calc',base_dict)

    def real_slip_i_processor(self,base_dict):
        base_dict=base_dict
        base_dict['reported']=0 #to be read from api
        base_dict['processed'] = base_dict['reported']
        base_dict['is_read']=True
        base_dict['is_processed']=False
        return base_dict

    def dst_togo_processor(self,base_dict):
        return self.base_individual_processor('dst_togo',base_dict)

    def stmtogo_processor(self,base_dict):
        return self.base_individual_processor('stmtogo',base_dict)

    def last_port_processor(self,base_dict):
        return self.base_individual_processor('last_port',base_dict)

    def lat_processor(self,base_dict):
        return self.base_individual_processor('lat',base_dict)

    def long_processor(self,base_dict):
        return self.base_individual_processor('long',base_dict)

    def nextport_processor(self,base_dict):
        return self.base_individual_processor('nextport',base_dict)

    def estLat_processor(self,base_dict):
        return self.base_individual_processor('estLat',base_dict)

    def estLong_processor(self,base_dict):
        return self.base_individual_processor('estLong',base_dict)

    def eta_processor(self,base_dict):
        return self.base_individual_processor('eta',base_dict)

    def main_lsfo_processor(self,base_dict):
        return self.base_individual_processor('main_lsfo',base_dict)

    def main_hsdo_processor(self,base_dict):
        return self.base_individual_processor('main_hsdo',base_dict)

    def main_lsdo_processor(self,base_dict):
        return self.base_individual_processor('main_lsdo',base_dict)

    def main_hfo_processor(self,base_dict):
        return self.base_individual_processor('main_hfo',base_dict)

    def main_do_processor(self,base_dict):
        return self.base_individual_processor('main_do',base_dict)

    def main_fuel_per_dst_processor(self,base_dict):
        return self.base_individual_processor('main_fuel_per_dst',base_dict)

    def main_total_processor(self,base_dict):
        return self.base_individual_processor('main_total',base_dict)

    def gen_lsfo_processor(self,base_dict):
        return self.base_individual_processor('gen_lsfo',base_dict)

    def gen_hsdo_processor(self,base_dict):
        return self.base_individual_processor('gen_hsdo',base_dict)

    def gen_lsdo_processor(self,base_dict):
        return self.base_individual_processor('gen_lsdo',base_dict)

    def gen_hfo_processor(self,base_dict):
        return self.base_individual_processor('gen_hfo',base_dict)

    def gen_do_processor(self,base_dict):
        return self.base_individual_processor('gen_do',base_dict)

    def boiler_lsfo_processor(self,base_dict):
        return self.base_individual_processor('boiler_lsfo',base_dict)

    def boiler_hsdo_processor(self,base_dict):
        return self.base_individual_processor('boiler_hsdo',base_dict)

    def boiler_lsdo_processor(self,base_dict):
        return self.base_individual_processor('boiler_lsdo',base_dict)

    def boiler_hfo_processor(self,base_dict):
        return self.base_individual_processor('boiler_hfo',base_dict)

    def boiler_do_processor(self,base_dict):
        return self.base_individual_processor('boiler_do',base_dict)

    def avg_lsfo_processor(self,base_dict):
        return self.base_individual_processor('avg_lsfo',base_dict) 

    def avg_hsdo_processor(self,base_dict):
        return self.base_individual_processor('avg_hsdo',base_dict) 

    def avg_lsdo_processor(self,base_dict):
        return self.base_individual_processor('avg_lsdo',base_dict) 

    def avg_hfo_processor(self,base_dict):
        return self.base_individual_processor('avg_hfo',base_dict) 

    def avg_do_processor(self,base_dict):
        return self.base_individual_processor('avg_do',base_dict) 
    
    def ch_lsfo_processor(self,base_dict):
        return self.base_individual_processor('ch_lsfo',base_dict)

    def ch_hsdo_processor(self,base_dict):
        return self.base_individual_processor('ch_hsdo',base_dict)

    def ch_lsdo_processor(self,base_dict):
        return self.base_individual_processor('ch_lsdo',base_dict)

    def ch_hfo_processor(self,base_dict):
        return self.base_individual_processor('ch_hfo',base_dict)

    def ch_do_processor(self,base_dict):
        return self.base_individual_processor('ch_do',base_dict)

    def ig_lsfo_processor(self,base_dict):
        return self.base_individual_processor('ig_lsfo',base_dict)

    def ig_hsdo_processor(self,base_dict):
        return self.base_individual_processor('ig_hsdo',base_dict)

    def ig_lsdo_processor(self,base_dict):
        return self.base_individual_processor('ig_lsdo',base_dict)

    def ig_hfo_processor(self,base_dict):
        return self.base_individual_processor('ig_hfo',base_dict)

    def ig_do_processor(self,base_dict):
        return self.base_individual_processor('ig_do',base_dict)

    def tc_lsfo_processor(self,base_dict):
        return self.base_individual_processor('tc_lsfo',base_dict)

    def tc_hsdo_processor(self,base_dict):
        return self.base_individual_processor('tc_hsdo',base_dict)

    def tc_lsdo_processor(self,base_dict):
        return self.base_individual_processor('tc_lsdo',base_dict)

    def tc_hfo_processor(self,base_dict):
        return self.base_individual_processor('tc_hfo',base_dict)

    def tc_do_processor(self,base_dict):
        return self.base_individual_processor('tc_do',base_dict)

    def rob_lsfo_processor(self,base_dict):
        return self.base_individual_processor('rob_lsfo',base_dict)

    def rob_hsdo_processor(self,base_dict):
        return self.base_individual_processor('rob_hsdo',base_dict)

    def rob_lsdo_processor(self,base_dict):
        return self.base_individual_processor('rob_lsdo',base_dict) 

    def rob_hfo_processor(self,base_dict):
        return self.base_individual_processor('rob_hfo',base_dict)

    def rob_do_processor(self,base_dict):
        return self.base_individual_processor('rob_do',base_dict)

    def stm_hrd_processor(self,base_dict):
        return self.base_individual_processor('stm_hrd',base_dict)

    def bridge_ctrl_processor(self,base_dict):
        return self.base_individual_processor('bridge_ctrl',base_dict)

    def goi_processor(self,base_dict):
        return self.base_individual_processor('goi',base_dict)

    def foi_processor(self,base_dict):
        return self.base_individual_processor('foi',base_dict)

    def eli_processor(self,base_dict):
        return self.base_individual_processor('eli',base_dict)

    def es_processor(self,base_dict):
        return self.base_individual_processor('es',base_dict)

    def es_calc_processor(self,base_dict):
        return self.base_individual_processor('es_calc',base_dict) 

    def edist_processor(self,base_dict):
        return self.base_individual_processor('edist',base_dict)

    def odist_processor(self,base_dict):
        return self.base_individual_processor('odist',base_dict)

    def pwr_processor(self,base_dict):
        return self.base_individual_processor('pwr',base_dict)

    def slip_o_processor(self,base_dict):
        return self.base_individual_processor('slip_o',base_dict) 

    def pwr_perc_processor(self,base_dict):
        return self.base_individual_processor('pwr_perc',base_dict) 

    def fo_serv_temp_processor(self,base_dict):
        return self.base_individual_processor('fo_serv_temp',base_dict)

    def fo_sucin_pres1_processor(self,base_dict):
        return self.base_individual_processor('fo_sucin_pres1',base_dict)

    def fo_in_pres1_processor(self,base_dict):
        return self.base_individual_processor('fo_in_pres1',base_dict)

    def fo_in_pres2_processor(self,base_dict):
        return self.base_individual_processor('fo_in_pres2',base_dict)

    def fo_circ_pres1_processor(self,base_dict):
        return self.base_individual_processor('fo_circ_pres1',base_dict)

    def fo_circ_pres2_processor(self,base_dict):
        return self.base_individual_processor('fo_circ_pres2',base_dict)

    def fo_in_temp_processor(self,base_dict):
        return self.base_individual_processor('fo_in_temp',base_dict)

    def fo_in_visc_processor(self,base_dict):
        return self.base_individual_processor('fo_in_visc',base_dict)

    def comp_pres1_processor(self,base_dict):
        return self.base_individual_processor('comp_pres1',base_dict)

    def peak_pres1_processor(self,base_dict):
        return self.base_individual_processor('peak_pres1',base_dict)

    def ext_temp1_processor(self,base_dict):
        return self.base_individual_processor('ext_temp1',base_dict)

    def jwme_out_temp1_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp1',base_dict)

    def pwme_out_temp1_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp1',base_dict)

    def comp_pres2_processor(self,base_dict):
        return self.base_individual_processor('comp_pres2',base_dict)

    def peak_pres2_processor(self,base_dict):
        return self.base_individual_processor('peak_pres2',base_dict)

    def ext_temp2_processor(self,base_dict):
        return self.base_individual_processor('ext_temp2',base_dict)

    def jwme_out_temp2_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp2',base_dict)

    def pwme_out_temp2_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp2',base_dict)

    def comp_pres3_processor(self,base_dict):
        return self.base_individual_processor('comp_pres3',base_dict)

    def peak_pres3_processor(self,base_dict):
        return self.base_individual_processor('peak_pres3',base_dict)

    def ext_temp3_processor(self,base_dict):
        return self.base_individual_processor('ext_temp3',base_dict)

    def jwme_out_temp3_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp3',base_dict)

    def pwme_out_temp3_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp3',base_dict)

    def comp_pres4_processor(self,base_dict):
        return self.base_individual_processor('comp_pres4',base_dict)

    def peak_pres4_processor(self,base_dict):
        return self.base_individual_processor('peak_pres4',base_dict)

    def ext_temp4_processor(self,base_dict):
        return self.base_individual_processor('ext_temp4',base_dict)

    def jwme_out_temp4_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp4',base_dict)

    def pwme_out_temp4_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp4',base_dict)
    
    def comp_pres5_processor(self,base_dict):
        return self.base_individual_processor('comp_pres5',base_dict)

    def peak_pres5_processor(self,base_dict):
        return self.base_individual_processor('peak_pres5',base_dict)

    def ext_temp5_processor(self,base_dict):
        return self.base_individual_processor('ext_temp5',base_dict)

    def jwme_out_temp5_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp5',base_dict)

    def pwme_out_temp5_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp5',base_dict)

    def comp_pres6_processor(self,base_dict):
        return self.base_individual_processor('comp_pres6',base_dict)

    def peak_pres6_processor(self,base_dict):
        return self.base_individual_processor('peak_pres6',base_dict)

    def ext_temp6_processor(self,base_dict):
        return self.base_individual_processor('ext_temp6',base_dict)

    def jwme_out_temp6_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp6',base_dict)

    def pwme_out_temp6_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp6',base_dict)

    def comp_pres7_processor(self,base_dict):
        return self.base_individual_processor('comp_pres7',base_dict)

    def peak_pres7_processor(self,base_dict):
        return self.base_individual_processor('peak_pres7',base_dict)

    def ext_temp7_processor(self,base_dict):
        return self.base_individual_processor('ext_temp7',base_dict)

    def jwme_out_temp7_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp7',base_dict)

    def pwme_out_temp7_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp7',base_dict)

    def comp_pres8_processor(self,base_dict):
        return self.base_individual_processor('comp_pres8',base_dict)

    def peak_pres8_processor(self,base_dict):
        return self.base_individual_processor('peak_pres8',base_dict)

    def ext_temp8_processor(self,base_dict):
        return self.base_individual_processor('ext_temp8',base_dict)

    def jwme_out_temp8_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp8',base_dict)

    def pwme_out_temp8_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp8',base_dict)

    def comp_pres9_processor(self,base_dict):
        return self.base_individual_processor('comp_pres9',base_dict)

    def peak_pres9_processor(self,base_dict):
        return self.base_individual_processor('peak_pres9',base_dict)

    def ext_temp9_processor(self,base_dict):
        return self.base_individual_processor('ext_temp9',base_dict)

    def jwme_out_temp9_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp9',base_dict)

    def pwme_out_temp9_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp9',base_dict)

    def comp_pres10_processor(self,base_dict):
        return self.base_individual_processor('comp_pres10',base_dict)

    def peak_pres10_processor(self,base_dict):
        return self.base_individual_processor('peak_pres10',base_dict)

    def ext_temp10_processor(self,base_dict):
        return self.base_individual_processor('ext_temp10',base_dict)

    def jwme_out_temp10_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp10',base_dict)

    def pwme_out_temp10_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp10',base_dict)

    def comp_pres11_processor(self,base_dict):
        return self.base_individual_processor('comp_pres11',base_dict)

    def peak_pres11_processor(self,base_dict):
        return self.base_individual_processor('peak_pres11',base_dict)

    def ext_temp11_processor(self,base_dict):
        return self.base_individual_processor('ext_temp11',base_dict)

    def jwme_out_temp11_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp11',base_dict)

    def pwme_out_temp11_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp11',base_dict)

    def comp_pres12_processor(self,base_dict):
        return self.base_individual_processor('comp_pres12',base_dict)

    def peak_pres12_processor(self,base_dict):
        return self.base_individual_processor('peak_pres12',base_dict)

    def ext_temp12_processor(self,base_dict):
        return self.base_individual_processor('ext_temp12',base_dict)

    def jwme_out_temp12_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_temp12',base_dict)

    def pwme_out_temp12_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_temp12',base_dict)

    def peak_presavg_processor(self,base_dict):
        base_dict=base_dict
        peak_pres=self.base_avg_minmax_evaluator("peak_pres")
        if len(peak_pres)>0:
            base_dict['processed'] = sum(peak_pres.values())/len(peak_pres.values())
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def peak_presmax_no_processor(self,base_dict):
        base_dict=base_dict
        peak_pres=self.base_avg_minmax_evaluator("peak_pres")
        if len(peak_pres)>0:
            base_dict['processed'] = max(peak_pres,key=peak_pres.get)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def peak_presmin_no_processor(self,base_dict):
        base_dict=base_dict
        peak_pres=self.base_avg_minmax_evaluator("peak_pres")
        if len(peak_pres)>0:
            base_dict['processed'] = min(peak_pres,key=peak_pres.get)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict
    
    def peakpres_maxmin_diff_processor(self,base_dict):
        base_dict=base_dict
        peak_pres=self.base_avg_minmax_evaluator("peak_pres")
        if len(peak_pres)>1:
            max_val=max(peak_pres.values())
            min_val=min(peak_pres.values())
            base_dict['processed'] = max_val-min_val
            base_dict['is_read']=False
            base_dict['is_processed']=True
        elif len(peak_pres)>0 and len(peak_pres)==1 :
            base_dict['processed'] = "only 1 value "
        return base_dict

    def comp_presavg_processor(self,base_dict):
        base_dict=base_dict
        comp_pres=self.base_avg_minmax_evaluator("comp_pres")    
        if len(comp_pres)>0:
            base_dict['processed'] = sum(comp_pres.values())/len(comp_pres)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def comp_presmax_no_processor(self,base_dict):
        base_dict=base_dict
        comp_pres=self.base_avg_minmax_evaluator("comp_pres")         
        if len(comp_pres)>0:
            base_dict['processed'] = max(comp_pres,key=comp_pres.get)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def comp_presmin_no_processor(self,base_dict):
        base_dict=base_dict
        comp_pres=self.base_avg_minmax_evaluator("comp_pres")         
        if len(comp_pres)>0:
            base_dict['processed'] = min(comp_pres,key=comp_pres.get)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def comppres_maxmin_diff_processor(self,base_dict):
        base_dict=base_dict
        comp_pres=self.base_avg_minmax_evaluator("comp_pres")        
        if len(comp_pres)>1:
            max_val=max(comp_pres.values())
            min_val=min(comp_pres.values())
            base_dict['processed'] = max_val-min_val
            base_dict['is_read']=False
            base_dict['is_processed']=True
        elif len(comp_pres)>0 and len(comp_pres)==1 :
            base_dict['processed'] = "only 1 value "
        return base_dict

    def ext_tempavg_processor(self,base_dict):
        base_dict=base_dict
        ext_temp=self.base_avg_minmax_evaluator("ext_temp")
        if len(ext_temp)>0:
            base_dict['processed'] = sum(ext_temp.values())/len(ext_temp)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def ext_max_no_processor(self,base_dict):
        base_dict=base_dict
        ext_temp=self.base_avg_minmax_evaluator("ext_temp")
        if len(ext_temp)>0:
            base_dict['processed'] = max(ext_temp,key=ext_temp.get)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def ext_min_no_processor(self,base_dict):
        base_dict=base_dict
        ext_temp=self.base_avg_minmax_evaluator("ext_temp")
        if len(ext_temp)>0:
            base_dict['processed'] = min(ext_temp,key=ext_temp.get)   
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def ext_maxmin_diff_processor(self,base_dict):
        base_dict=base_dict
        ext_temp= self.base_avg_minmax_evaluator('ext_temp') 
        if len(ext_temp)>1:
            max_val=max(ext_temp.values())
            min_val=min(ext_temp.values())
            base_dict['processed'] =max_val-min_val
            base_dict['is_read']=False
            base_dict['is_processed']=True
        elif len(ext_temp)>0 and len(ext_temp)==1 :
            base_dict['processed'] = "only 1 value "
        return base_dict

    def ext_pres_processor(self,base_dict):
        return self.base_individual_processor('ext_pres',base_dict)

    def tc1_rpm_processor(self,base_dict):
        return self.base_individual_processor('tc1_rpm',base_dict)
    
    def tc1_extin_temp_processor(self,base_dict):
        return self.base_individual_processor('tc1_extin_temp',base_dict)

    def tc1_extout_temp_processor(self,base_dict):
        return self.base_individual_processor('tc1_extout_temp',base_dict)

    def tc1_pres_drop_processor(self,base_dict):
        return self.base_individual_processor('tc1_pres_drop',base_dict)

    def tc1_loblower_temp_processor(self,base_dict):
        return self.base_individual_processor('tc1_loblower_temp',base_dict)

    def tc1_loturbine_temp_processor(self,base_dict):
        return self.base_individual_processor('tc1_loturbine_temp',base_dict)
    
    def tc2_rpm_processor(self,base_dict):
        return self.base_individual_processor('tc2_rpm',base_dict)

    def tc2_extin_temp_processor(self,base_dict):
        return self.base_individual_processor('tc2_extin_temp',base_dict)

    def tc2_extout_temp_processor(self,base_dict):
        return self.base_individual_processor('tc2_extout_temp',base_dict)

    def tc2_pres_drop_processor(self,base_dict):
        return self.base_individual_processor('tc2_pres_drop',base_dict)

    def tc2_loblower_temp_processor(self,base_dict):
        return self.base_individual_processor('tc2_loblower_temp',base_dict)

    def tc2_loturbine_temp_processor(self,base_dict):
        return self.base_individual_processor('tc2_loturbine_temp',base_dict)

    def tc3_rpm_processor(self,base_dict):
        return self.base_individual_processor('tc3_rpm',base_dict)

    def tc3_extin_temp_processor(self,base_dict):
        return self.base_individual_processor('tc3_extin_temp',base_dict)

    def tc3_extout_temp_processor(self,base_dict):
        return self.base_individual_processor('tc3_extout_temp',base_dict)

    def tc3_pres_drop_processor(self,base_dict):
        return self.base_individual_processor('tc3_pres_drop',base_dict)

    def tc3_loblower_temp_processor(self,base_dict):
        return self.base_individual_processor('tc3_loblower_temp',base_dict)

    def tc3_loturbine_temp_processor(self,base_dict):
        return self.base_individual_processor('tc3_loturbine_temp',base_dict)

    def tc4_rpm_processor(self,base_dict):
        return self.base_individual_processor('tc4_rpm',base_dict)

    def tc4_extin_temp_processor(self,base_dict):
        return self.base_individual_processor('tc4_extin_temp',base_dict)

    def tc4_extout_temp_processor(self,base_dict):
        return self.base_individual_processor('tc4_extout_temp',base_dict)

    def tc4_pres_drop_processor(self,base_dict):
        return self.base_individual_processor('tc4_pres_drop',base_dict)

    def tc4_loblower_temp_processor(self,base_dict):
        return self.base_individual_processor('tc4_loblower_temp',base_dict)

    def tc4_loturbine_temp_processor(self,base_dict):
        return self.base_individual_processor('tc4_loturbine_temp',base_dict)

    def sa_pres_processor(self,base_dict):
        return self.base_individual_processor('sa_pres',base_dict)

    def sa_temp_processor(self,base_dict):
        return self.base_individual_processor('sa_temp',base_dict)

    def ac1_pres_drop_processor(self,base_dict):
        return self.base_individual_processor('ac1_pres_drop',base_dict)

    def ac1_in_temp_processor(self,base_dict):
        return self.base_individual_processor('ac1_in_temp',base_dict)

    def ac1_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('ac1_swin_temp',base_dict)

    def ac1_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('ac1_swout_temp',base_dict)

    def ac2_pres_drop_processor(self,base_dict):
        return self.base_individual_processor('ac2_pres_drop',base_dict)

    def ac2_in_temp_processor(self,base_dict):
        return self.base_individual_processor('ac2_in_temp',base_dict)

    def ac2_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('ac2_swin_temp',base_dict)

    def ac2_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('ac2_swout_temp',base_dict)

    def ac3_pres_drop_processor(self,base_dict):
        return self.base_individual_processor('ac3_pres_drop',base_dict)

    def ac3_in_temp_processor(self,base_dict):
        return self.base_individual_processor('ac3_in_temp',base_dict)

    def ac3_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('ac3_swin_temp',base_dict)

    def ac3_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('ac3_swout_temp',base_dict)

    def ac4_pres_drop_processor(self,base_dict):
        return self.base_individual_processor('ac4_pres_drop',base_dict)

    def ac4_in_temp_processor(self,base_dict):
        return self.base_individual_processor('ac4_in_temp',base_dict)

    def ac4_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('ac4_swin_temp',base_dict)

    def ac4_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('ac4_swout_temp',base_dict)

    def er_temp_processor(self,base_dict):
        return self.base_individual_processor('er_temp',base_dict)

    def er_pres_processor(self,base_dict):
        return self.base_individual_processor('er_pres',base_dict)

    def er_hum_processor(self,base_dict):
        return self.base_individual_processor('er_hum',base_dict)

    def jwme_in_pres_processor(self,base_dict):
        return self.base_individual_processor('jwme_in_pres',base_dict)

    def jwme_in_temp_processor(self,base_dict):
        return self.base_individual_processor('jwme_in_temp',base_dict)

    def jwme_out_tempavg_processor(self,base_dict):
        base_dict=base_dict
        jwme_temp={}
        jwme_str=self.base_avg_minmax_evaluator("jwme_out_temp")
        if len(jwme_temp)>0:
            base_dict['processed'] = sum(jwme_temp.values())/len(jwme_temp)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def jwme_out_pres_processor(self,base_dict):
        return self.base_individual_processor('jwme_out_pres',base_dict)

    def jwme_out_temp_max_no_processor(self,base_dict):
        base_dict=base_dict
        jwme_temp={}
        jwme_str=self.base_avg_minmax_evaluator("jwme_out_temp")
        if len(jwme_temp)>0:
            base_dict['processed'] = max(jwme_temp,key=jwme_temp.get)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def jwme_out_temp_min_no_processor(self,base_dict):
        base_dict=base_dict
        jwme_temp=self.base_avg_minmax_evaluator("jwme_out_temp")
        if len(jwme_temp)>0:
            base_dict['processed'] = min(jwme_temp,key=jwme_temp.get)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict
    
    def jwc1_used_no_processor(self,base_dict):
        return self.base_individual_processor('jwc1_used_no',base_dict)

    def jwc1_fwin_pres_processor(self,base_dict):
        return self.base_individual_processor('jwc1_fwin_pres',base_dict)

    def jwc1_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('jwc1_swin_temp',base_dict)

    def jwc1_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('jwc1_swout_temp',base_dict)

    def jwc1_fwin_temp_processor(self,base_dict):
        return self.base_individual_processor('jwc1_fwin_temp',base_dict)

    def jwc1_fwout_temp_processor(self,base_dict):
        return self.base_individual_processor('jwc1_fwout_temp',base_dict)

    def jwc1_fwout_pres_processor(self,base_dict):
        return self.base_individual_processor('jwc1_fwout_pres',base_dict)

    def jwc1_swin_pres_processor(self,base_dict):
        return self.base_individual_processor('jwc1_swin_pres',base_dict)

    def jwc1_swout_pres_processor(self,base_dict):
        return self.base_individual_processor('jwc1_swout_pres',base_dict)

    def jwc2_used_no_processor(self,base_dict):
        return self.base_individual_processor('jwc2_used_no',base_dict)

    def jwc2_fwin_pres_processor(self,base_dict):
        return self.base_individual_processor('jwc2_fwin_pres',base_dict)

    def jwc2_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('jwc2_swin_temp',base_dict)

    def jwc2_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('jwc2_swout_temp',base_dict)

    def jwc2_fwin_temp_processor(self,base_dict):
        return self.base_individual_processor('jwc2_fwin_temp',base_dict)

    def jwc2_fwout_temp_processor(self,base_dict):
        return self.base_individual_processor('jwc2_fwout_temp',base_dict)

    def jwc2_fwout_pres_processor(self,base_dict):
        return self.base_individual_processor('jwc2_fwout_pres',base_dict)

    def jwc2_swin_pres_processor(self,base_dict):
        return self.base_individual_processor('jwc2_swin_pres',base_dict)

    def jwc2_swout_pres_processor(self,base_dict):
        return self.base_individual_processor('jwc2_swout_pres',base_dict)

    def pwme_in_pres_processor(self,base_dict):
        return self.base_individual_processor('pwme_in_pres',base_dict)

    def pwme_in_temp_processor(self,base_dict):
        return self.base_individual_processor('pwme_in_temp',base_dict)

    def pwme_out_tempavg_processor(self,base_dict):
        base_dict=base_dict
        pwme_temp=self.base_avg_minmax_evaluator("pwme_out_temp")
        if len(pwme_temp)>0:
            base_dict['processed'] = sum(pwme_temp.values())/len(pwme_temp)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def pwme_out_pres_processor(self,base_dict):
        return self.base_individual_processor('pwme_out_pres',base_dict)

    def pwme_out_temp_max_no_processor(self,base_dict):
        base_dict=base_dict
        pwme_temp=self.base_avg_minmax_evaluator("pwme_out_temp")
        if len(pwme_temp)>0:
            base_dict['processed'] = max(pwme_temp,key=pwme_temp.get)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def pwme_out_temp_min_no_processor(self,base_dict):
        base_dict=base_dict
        pwme_temp=self.base_avg_minmax_evaluator("pwme_out_temp")
        if len(pwme_temp)>0:
            base_dict['processed'] = min(pwme_temp,key=pwme_temp.get)
            base_dict['is_read']=False
            base_dict['is_processed']=True
        return base_dict

    def pwc1_used_no_processor(self,base_dict):
        return self.base_individual_processor('pwc1_used_no',base_dict)

    def pwc1_fwin_pres_processor(self,base_dict):
        return self.base_individual_processor('pwc1_fwin_pres',base_dict)

    def pwc1_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('pwc1_swin_temp',base_dict)

    def pwc1_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('pwc1_swout_temp',base_dict)

    def pwc1_fwin_temp_processor(self,base_dict):
        return self.base_individual_processor('pwc1_fwin_temp',base_dict)

    def pwc1_fwout_temp_processor(self,base_dict):
        return self.base_individual_processor('pwc1_fwout_temp',base_dict)

    def pwc1_fwout_pres_processor(self,base_dict):
        return self.base_individual_processor('pwc1_fwout_pres',base_dict)

    def pwc1_swin_pres_processor(self,base_dict):
        return self.base_individual_processor('pwc1_swin_pres',base_dict)

    def pwc1_swout_pres_processor(self,base_dict):
        return self.base_individual_processor('pwc1_swout_pres',base_dict)

    def pwc2_used_no_processor(self,base_dict):
        return self.base_individual_processor('pwc2_used_no',base_dict)

    def pwc2_fwin_pres_processor(self,base_dict):
        return self.base_individual_processor('pwc2_fwin_pres',base_dict)

    def pwc2_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('pwc2_swin_temp',base_dict)

    def pwc2_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('pwc2_swout_temp',base_dict)

    def pwc2_fwin_temp_processor(self,base_dict):
        return self.base_individual_processor('pwc2_fwin_temp',base_dict)

    def pwc2_fwout_temp_processor(self,base_dict):
        return self.base_individual_processor('pwc2_fwout_temp',base_dict)

    def pwc2_fwout_pres_processor(self,base_dict):
        return self.base_individual_processor('pwc2_fwout_pres',base_dict)

    def pwc2_swin_pres_processor(self,base_dict):
        return self.base_individual_processor('pwc2_swin_pres',base_dict)

    def pwc2_swout_pres_processor(self,base_dict):
        return self.base_individual_processor('pwc2_swout_pres',base_dict)

    def lome_xhd_pres_processor(self,base_dict):
        return self.base_individual_processor('lome_xhd_pres',base_dict)

    def lome_ib_pres_processor(self,base_dict):
        return self.base_individual_processor('lome_ib_pres',base_dict)

    def lome_cc_pres_processor(self,base_dict):
        return self.base_individual_processor('lome_cc_pres',base_dict)

    def lome_in_temp_processor(self,base_dict):
        return self.base_individual_processor('lome_in_temp',base_dict)
        
    def lome_out_temp_processor(self,base_dict):
        return self.base_individual_processor('lome_out_temp',base_dict)

    def lome_cs_pres_processor(self,base_dict):
        return self.base_individual_processor('lome_cs_pres',base_dict)

    def loc_used_no_processor(self,base_dict):
        return self.base_individual_processor('loc_used_no',base_dict)

    def loc1_fwin_pres_processor(self,base_dict):
        return self.base_individual_processor('loc1_fwin_pres',base_dict)

    def loc1_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('loc1_swin_temp',base_dict)

    def loc1_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('loc1_swout_temp',base_dict)

    def loc1_loin_temp_processor(self,base_dict):
        return self.base_individual_processor('loc1_loin_temp',base_dict)

    def loc1_loout_temp_processor(self,base_dict):
        return self.base_individual_processor('loc1_loout_temp',base_dict)

    def loc1_loout_pres_processor(self,base_dict):
        return self.base_individual_processor('loc1_loout_pres',base_dict)

    def loc1_swin_pres_processor(self,base_dict):
        return self.base_individual_processor('loc1_swin_pres',base_dict)

    def loc1_swout_pres_processor(self,base_dict):
        return self.base_individual_processor('loc1_swout_pres',base_dict)

    def loc2_fwin_pres_processor(self,base_dict):
        return self.base_individual_processor('loc2_fwin_pres',base_dict)

    def loc2_swin_temp_processor(self,base_dict):
        return self.base_individual_processor('loc2_swin_temp',base_dict)

    def loc2_swout_temp_processor(self,base_dict):
        return self.base_individual_processor('loc2_swout_temp',base_dict)

    def loc2_loin_temp_processor(self,base_dict):
        return self.base_individual_processor('loc2_loin_temp',base_dict)

    def loc2_loout_temp_processor(self,base_dict):
        return self.base_individual_processor('loc2_loout_temp',base_dict)

    def loc2_loout_pres_processor(self,base_dict):
        return self.base_individual_processor('loc2_loout_pres',base_dict)

    def loc2_swin_pres_processor(self,base_dict):
        return self.base_individual_processor('loc2_swin_pres',base_dict)

    def loc2_swout_pres_processor(self,base_dict):
        return self.base_individual_processor('loc2_swout_pres',base_dict)

    def sw1_used_no_processor(self,base_dict):
        return self.base_individual_processor('sw1_used_no',base_dict)

    def sw1_pres_processor(self,base_dict):
        return self.base_individual_processor('sw1_pres',base_dict)

    def sw1_temp_processor(self,base_dict):
        return self.base_individual_processor('sw1_temp',base_dict)

    def sw2_used_no_processor(self,base_dict):
        return self.base_individual_processor('sw2_used_no',base_dict)

    def sw2_pres_processor(self,base_dict):
        return self.base_individual_processor('sw2_pres',base_dict)

    def sw2_temp_processor(self,base_dict):
        return self.base_individual_processor('sw2_temp',base_dict)

    def gen_sw_pres_processor(self,base_dict):
        return self.base_individual_processor('gen_sw_pres',base_dict)

    def gen_sw_temp_processor(self,base_dict):
        return self.base_individual_processor('gen_sw_temp',base_dict)