from logging import NOTSET
from pickle import TRUE
from re import T
import sys
import os
from dotenv import load_dotenv
from numpy.core.defchararray import endswith, index
from numpy.lib.function_base import median
from numpy.lib.npyio import load
from numpy.lib.shape_base import dsplit

import pandas
from pandas.core.algorithms import factorize
from pandas.core.frame import DataFrame
from pandas.tseries.offsets import Second 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.processors.dd_processor.individual_processors import IndividualProcessors
from src.processors.dd_processor.outlier_two import OutlierTwo
from src.processors.dd_processor.universal_limit import Universal_limits
from src.processors.dd_processor.update_individual_processor import UpdateIndividualProcessors,EWMA
from src.processors.dd_processor.individual_processor_lvl_two import IndividualProcessorsTwo
# from src.processors.dd_processor.update_individual_predictions import UpdateIndividualProcessorspredictions
from src.processors.dd_extractor.extractor_new import DailyInsert
from src.processors.dd_processor.indices_procesor import Indice_Processing,mewma
from src.processors.dd_processor.Universal_indice_limits import Universal_indices_limits
from src.configurations.logging_config import CommonLogger
from datetime import date, datetime
from src.db.schema.main import Main_db
from src.db.schema.ship import Ship 
from src.db.schema.ddschema import DailyData
import numpy as np
from pymongo import MongoClient
import random
from src.processors.dd_processor.outlier import CheckOutlier
from pymongo import ASCENDING
from dateutil.relativedelta import relativedelta
# import matplotlib.pyplot as plt
log = CommonLogger(__name__,debug=True).setup_logger()
from bson import json_util
import scipy.stats as st
import time

load_dotenv()

MONGODB_URI = os.getenv('MONGODB_ATLAS')
client = MongoClient(MONGODB_URI)

# client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
database = db



def check_status(func) -> object:
    """
    Decorator for functions in class.
    Working:
        Decorator check the error flag each time before executing the function. If error present it skips function.
        Error is set using raise_error() function whenever there is error and it is neede to return to request.
        This function is outside class ad it works with self parameters.

        Only few first and last function are not applied with this decorator.

        Example: There's error in set_data which is set using raise_error. Then next functions which have this decorator will \
        first check that flag to find that there was error set, hence it will skip.
    """
    """def wrapper(self, *arg, **kw):
        if self.error == False:
            try:
                res = func(self, *arg, **kw)
                log.info(f"Executed {func.__name__}")
            except Exception as e:
                res =None
                self.error = True
                self.traceback_msg = f"Error in {func.__name__}(): {e}"
                log.info(f"Error in {func.__name__}(): {e}")

        else:
            res = None
            log.info(f"Did not execute {func.__name__}")
        return res
    return wrapper"""


class MainDB():
    """def __init__(self,ship_imo,date,override):
        self.database= None
        self.ship_configs = None
        self.daily_data = None
        self.ship_imo = ship_imo
        self.date = date
        self.override=override
        self.error = False
        self.traceback_msg = None
        pass"""

    def __init__(self,ship_imo,timestamp,report_date):
        self.timestamp=timestamp
        self.ship_imo=ship_imo 
        self.report_date=report_date


    def raise_error(self, message):
        """
        Whenever there needs to be raised something(error message), which needs to be returned to request, this functions is envoked.
        It sets error flag and message. Error flag in turn is used by decorator check_status() to decide if it needs execute function or skip it.
        One of the mose important function in the class.

        """
        self.error = True
        self.error_message = message
        log.info(f"Error \"{self.error_message}\" is set")

    def do_steps(self):
        self.connect_db()
        self.get_daily_data()
        self.get_ship_configs()
        self.process_daily_data()
        self.process_weather_api_data()
        self.process_position_api_data()
        # self.process_indices()
        inserted_id = self.main_db_writer()
        if self.error:
            return False, str(self.traceback_msg)
        else:
            return True, str(inserted_id)

    #@staticmethod
    def base_dict(self,identifier=None,
                  old_identifier=None,
                  name=None,
                  reported=None,
                  processed=None,
                  within_outlier_limits=None,
                  within_operational_limits=None,
                  is_read=None,
                  is_processed=None,
                  z_score=None,
                  unit=None,
                  var_type=None,
                  statement=None,
                  predictions=None,
                  ):
        
        return {"identifier": identifier,
                "old_identifier":old_identifier,
                "name": name,
                "reported": reported,
                "processed": processed,
                "within_outlier_limits": within_outlier_limits,
                "within_operational_limits": within_operational_limits,
                "is_read":is_read,
                "is_processed":is_processed,
                "z_score": z_score,
                "unit": unit,
                "var_type":var_type,
                "statement": statement,
                "predictions": predictions}
        
    def equipment_base_dict(self,identifier=None,
                  equipment_type=None,
                  name=None,
                  code_name=None,
                  input_param=None,
                  output_param=None,
                  T_2=None,
                  SPE=None,
                  MEWMA=None,
                  Anamoly_lvl=None,
                  fault=None,
                  health_state=None,
                  
                  ):
        
        return {"identifier": identifier,
                "equipment_type":equipment_type,
                "name": name,
                "code_name": code_name,
                "input_param": input_param,
                "output_param": output_param,
                "T_2": T_2,
                "SPE":SPE,
                "MEWMA":MEWMA,
                "Anamoly_lvl": Anamoly_lvl,
                "fault": fault,
                "health_state":health_state}

    @check_status
    def connect_db(self):
        self.database = connect_db()

    #@check_status
    def get_ship_configs(self):
        self.ship_configs_collection = database.get_collection("ship")
        self.ship_configs = self.ship_configs_collection.find({"ship_imo": self.ship_imo})[0]
        

    #@check_status
    def get_daily_data(self,index):
        daily_data_collection =database.get_collection("daily_data")
        # self.daily_data = daily_data_collection.find({"ship_imo": self.ship_imo})[index]
        # self.daily_data = daily_data_collection.find({"ship_imo": self.ship_imo})[0]
        self.daily_data = daily_data_collection.find({"ship_imo": self.ship_imo,"data.rep_dt":self.report_date,"timestamp":self.timestamp})[0]

    #@check_status
    def build_base_dict(self, identifier):
        return self.base_dict(identifier=identifier,
                              old_identifier=self.ship_configs['data'][identifier]['identifier_old'],
                              name=self.ship_configs['data'][identifier]['short_names'],
                              var_type=self.ship_configs['data'][identifier]['var_type'],
                              unit=self.ship_configs['data'][identifier]['unit'])
        

    def build_equipment_base_dict(self, identifier):
        return self.equipment_base_dict(identifier=identifier,
                              equipment_type=self.ship_configs['data'][identifier]['var_type'],
                              name=self.ship_configs['data'][identifier]['short_names'],
                              code_name=self.ship_configs['data'][identifier]['identifier_old'])


    def within_good_voyage_limits(self):
        default=True
        steaminghrs=self.daily_data['data']['stm_hrs']
        speedonground=self.daily_data['data']['speed_ship_sog']
        shipmaxspeed=self.ship_configs['static_data']['ship_maxspeed']
        cp_speed=self.daily_data['data']['cp_speed']
        if steaminghrs<18 or speedonground<(shipmaxspeed-4) or speedonground<(cp_speed-3):
            default=False
            return default
        else:
            default=True
            return default

    def vessel_load_check(self):
        try:
            try:
                vsl_load=self.daily_data['data']['vsl_load']
            except:
                vsl_load=self.daily_data['data']['vsl_load_bal']
            if pandas.isnull(vsl_load):
                return "not checked"
            elif vsl_load==1:
                return "Loaded"
            elif vsl_load==0:
                return "Ballast"      
        except:
            return None
        

    def data_available_check(self,data_available_nav):
        
        # data_available_nav=self.daily_data['data_available_nav']
            
        if len(data_available_nav)==0:
            return False
        else:
            return True      
       
    #@check_status
    def process_daily_data(self):
        "processing dailydata with all identifiers available in ship data"
        self.processed_daily_data =  {}
        self.processed_equipment_data={}
        IP = IndividualProcessors(configs=self.ship_configs,dd=self.daily_data)
        for key,val in self.ship_configs['data'].items():
            if (self.ship_configs['data'][key]['var_type']=='E') or (self.ship_configs['data'][key]['var_type']=='E1'): 
                equipment_base_dict=self.build_equipment_base_dict(key)
                input=self.ship_configs['data'][key]['input']
                output=self.ship_configs['data'][key]['output']
                input_list=None
                output_list=None
                if pandas.isnull(input)==False:
                    # for i in input.split():
                    input=input.replace(',',' ')
                    input_list=input.split()
                if pandas.isnull(output)==False:
                    # for i in output.split():
                    output=output.replace(',',' ')
                    output_list=output.split()
                if pandas.isnull(self.ship_configs['data'][key]['source_idetifier'])==False and self.ship_configs['data'][key]['source_idetifier']=="available":
                    equipment_base_dict['processed']=1
                else:
                    equipment_base_dict['processed']=0

                equipment_base_dict['input_param']=input_list
                equipment_base_dict['output_param']=output_list
                self.processed_equipment_data[key]=equipment_base_dict
                # equipment t2 spe codes functin here later
            else:
                if (self.ship_configs['data'][key]['var_type']!='T2&SPE'):
                    base_dict = self.build_base_dict(key)
                    if key in self.ship_configs['data_available_nav']:
                        base_dict[key+"_i"]=None
                    try:
                        self.ship_configs['data'][key]
                        self.processed_daily_data[key] = eval("IP."+key.strip()+"_processor")(base_dict) # IP.rpm_processor(base_dict)
                    except KeyError:
                        self.processed_daily_data[key] = IP.base_individual_processor(key,base_dict)
                    except AttributeError:
                        self.processed_daily_data[key] = IP.base_individual_processor(key,base_dict)
            
    
    def add_calc_i_cp(self):
        maindb = database.get_collection("Main_db")
        # maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
        print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom")
        self.get_main_db()
        processed_daily_data=self.main_data['processed_daily_data']
        for key in self.ship_configs['data']:
            if key.endswith("_cp"):
                key_split=key[:-3]
                if key_split in processed_daily_data:
                    processed_daily_data[key_split][key]=processed_daily_data[key]['processed']
                    maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data."+key_split:processed_daily_data[key_split]}})
                    print(key)
            elif key.endswith("_i") or key.endswith("_e"):
                key_split=key[:-2]
                if key_split in processed_daily_data:
                    processed_daily_data[key_split][key]=processed_daily_data[key]['processed']
                    maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data."+key_split:processed_daily_data[key_split]}})
                    print(key)
            elif key.endswith("_calc"):
                key_split=key[:-5]
                if key_split in processed_daily_data:
                    processed_daily_data[key_split][key]=processed_daily_data[key]['processed']
                    maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data."+key_split:processed_daily_data[key_split]}})
                    print("done")

    def maindb_lvl_two(self):
        maindb = database.get_collection("Main_db")
        print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom")
        self.get_main_db()
        processed_daily_data=self.main_data['processed_daily_data']
        equipment_data=self.main_data['Equipment']
        IP_two=IndividualProcessorsTwo(self.ship_configs,processed_daily_data,equipment_data)
        for key in self.ship_configs['data']:
            if key in processed_daily_data and (pandas.isnull(processed_daily_data[key]['processed']) or processed_daily_data[key]['processed']==0 or processed_daily_data[key]['processed']==1) and self.ship_configs['data'][key]['Derived']==True:
                print(key)
                base_dict=processed_daily_data[key]
                maindict=IP_two.base_individual_processor(key,base_dict)
                maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data."+key:maindict}})
                print("doneee")
            

    def get_main_db(self):
        self.maindb = database.get_collection("Main_db")
        self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0]      

    def process_main_data(self):
        "processing maindb data for updation ,only processed daily data will be updated."
        maindb = database.get_collection("Main_db")
        self.get_main_db()
        self.base_main_data={}
        check_outlier=CheckOutlier(configs=self.ship_configs,main_db=self.main_data,ship_imo=self.ship_imo)
        outlier_two=OutlierTwo(self.ship_configs,self.main_data,self.ship_imo)
        outlier_two.connect()
        main_data_dict=self.main_data['processed_daily_data']

        for key in main_data_dict:
            # if key=='rpm':
            try:
                main_data_dict_key=main_data_dict[key] 
                # print(main_data_dict_key['processed'])
                if pandas.isnull(main_data_dict_key['processed'])==False:
                    try:
                        print(key)
                    
                        oplow=self.ship_configs['data'][key]['limits']['oplow'] 
                        ophigh=self.ship_configs['data'][key]['limits']['ophigh']                                    #rpm with % 
                        olmin=self.ship_configs['data'][key]['limits']['olmin']
                        olmax=self.ship_configs['data'][key]['limits']['olmax']
                        limit_val=self.ship_configs['data'][key]['limits']['type']
                        outlier_limit_msg={}
                        operational_limit_msg={}
                        within_outlier_limit_min={}
                        outliermin_max={}
                        operationalmin_max={}
                        within_operational_limit_min={}
                        within_operational_limit_max={}
                        within_outlier_limit_max={}
                        within_outlier_limit={}
                        within_operational_limit={}
                        z_score,data_available=outlier_two.base_prediction_processor(key,main_data_dict_key,main_data_dict)
                        outlier_z_score=outlier_two.outlier_operational_z_score(z_score,3)
                        operational_z_score=outlier_two.outlier_operational_z_score(z_score,2)
                        
                        within_outlier_limit_min['m3']=check_outlier.outlier_min(key,main_data_dict_key['processed'],oplow,olmin,limit_val,outlier_z_score['m3'],data_available['m3'])
                        within_outlier_limit_min['m6']=check_outlier.outlier_min(key,main_data_dict_key['processed'],oplow,olmin,limit_val,outlier_z_score['m6'],data_available['m6'])
                        within_outlier_limit_min['m12']=check_outlier.outlier_min(key,main_data_dict_key['processed'],oplow,olmin,limit_val,outlier_z_score['m12'],data_available['m12'])
                        within_outlier_limit_min['ly_m3']=check_outlier.outlier_min(key,main_data_dict_key['processed'],oplow,olmin,limit_val,outlier_z_score['ly_m3'],data_available['ly_m3'])
                        within_outlier_limit_min['ly_m6']=check_outlier.outlier_min(key,main_data_dict_key['processed'],oplow,olmin,limit_val,outlier_z_score['ly_m6'],data_available['ly_m6'])
                        within_outlier_limit_min['ly_m12']=check_outlier.outlier_min(key,main_data_dict_key['processed'],oplow,olmin,limit_val,outlier_z_score['ly_m12'],data_available['ly_m12'])
                        
                        within_outlier_limit_max['m3']=check_outlier.outlier_max(key,main_data_dict_key['processed'],ophigh,olmax,limit_val,outlier_z_score['m3'],data_available['m3'])
                        within_outlier_limit_max['m6']=check_outlier.outlier_max(key,main_data_dict_key['processed'],ophigh,olmax,limit_val,outlier_z_score['m6'],data_available['m6'])
                        within_outlier_limit_max['m12']=check_outlier.outlier_max(key,main_data_dict_key['processed'],ophigh,olmax,limit_val,outlier_z_score['m12'],data_available['m12'])
                        within_outlier_limit_max['ly_m3']=check_outlier.outlier_max(key,main_data_dict_key['processed'],ophigh,olmax,limit_val,outlier_z_score['ly_m3'],data_available['ly_m3'])
                        within_outlier_limit_max['ly_m6']=check_outlier.outlier_max(key,main_data_dict_key['processed'],ophigh,olmax,limit_val,outlier_z_score['ly_m6'],data_available['ly_m6'])
                        within_outlier_limit_max['ly_m12']=check_outlier.outlier_max(key,main_data_dict_key['processed'],ophigh,olmax,limit_val,outlier_z_score['ly_m12'],data_available['ly_m12'])
                        
                        
                        within_outlier_limit['m3'],outlier_limit_msg['m3']=check_outlier.final_outlier(within_outlier_limit_min['m3'][0],within_outlier_limit_max['m3'][0],"out")
                        within_outlier_limit['m6'],outlier_limit_msg['m6']=check_outlier.final_outlier(within_outlier_limit_min['m6'][0],within_outlier_limit_max['m6'][0],"out")
                        within_outlier_limit['m12'],outlier_limit_msg['m12']=check_outlier.final_outlier(within_outlier_limit_min['m12'][0],within_outlier_limit_max['m12'][0],"out")
                        within_outlier_limit['ly_m3'],outlier_limit_msg['ly_m3']=check_outlier.final_outlier(within_outlier_limit_min['ly_m3'][0],within_outlier_limit_max['ly_m3'][0],"out")
                        within_outlier_limit['ly_m6'],outlier_limit_msg['ly_m6']=check_outlier.final_outlier(within_outlier_limit_min['ly_m6'][0],within_outlier_limit_max['ly_m6'][0],"out")
                        within_outlier_limit['ly_m12'],outlier_limit_msg['ly_m12']=check_outlier.final_outlier(within_outlier_limit_min['ly_m12'][0],within_outlier_limit_max['ly_m12'][0],"out")
                        
                        
                        within_operational_limit_min['m3']=check_outlier.operational_min(key,main_data_dict_key['processed'],oplow,limit_val,operational_z_score['m3'],data_available['m3'])
                        within_operational_limit_min['m6']=check_outlier.operational_min(key,main_data_dict_key['processed'],oplow,limit_val,operational_z_score['m6'],data_available['m6'])
                        within_operational_limit_min['m12']=check_outlier.operational_min(key,main_data_dict_key['processed'],oplow,limit_val,operational_z_score['m12'],data_available['m12'])
                        within_operational_limit_min['ly_m3']=check_outlier.operational_min(key,main_data_dict_key['processed'],oplow,limit_val,operational_z_score['ly_m3'],data_available['ly_m3'])
                        within_operational_limit_min['ly_m6']=check_outlier.operational_min(key,main_data_dict_key['processed'],oplow,limit_val,operational_z_score['ly_m6'],data_available['ly_m6'])
                        within_operational_limit_min['ly_m12']=check_outlier.operational_min(key,main_data_dict_key['processed'],oplow,limit_val,operational_z_score['ly_m12'],data_available['ly_m12'])
                        
                        within_operational_limit_max['m3']=check_outlier.operational_max(key,main_data_dict_key['processed'],ophigh,limit_val,operational_z_score['m3'],data_available['m3'])
                        within_operational_limit_max['m6']=check_outlier.operational_max(key,main_data_dict_key['processed'],ophigh,limit_val,operational_z_score['m6'],data_available['m6'])
                        within_operational_limit_max['m12']=check_outlier.operational_max(key,main_data_dict_key['processed'],ophigh,limit_val,operational_z_score['m12'],data_available['m12'])
                        within_operational_limit_max['ly_m3']=check_outlier.operational_max(key,main_data_dict_key['processed'],ophigh,limit_val,operational_z_score['ly_m3'],data_available['ly_m3'])
                        within_operational_limit_max['ly_m6']=check_outlier.operational_max(key,main_data_dict_key['processed'],ophigh,limit_val,operational_z_score['ly_m6'],data_available['ly_m6'])
                        within_operational_limit_max['ly_m12']=check_outlier.operational_max(key,main_data_dict_key['processed'],ophigh,limit_val,operational_z_score['ly_m12'],data_available['ly_m12'])
                        
                        within_operational_limit['m3'],operational_limit_msg['m3']=check_outlier.final_outlier(within_operational_limit_min['m3'][0],within_operational_limit_max['m3'][0],"op")
                        within_operational_limit['m6'],operational_limit_msg['m6']=check_outlier.final_outlier(within_operational_limit_min['m6'][0],within_operational_limit_max['m6'][0],"op")
                        within_operational_limit['m12'],operational_limit_msg['m12']=check_outlier.final_outlier(within_operational_limit_min['m12'][0],within_operational_limit_max['m12'][0],"op")
                        within_operational_limit['ly_m3'],operational_limit_msg['ly_m3']=check_outlier.final_outlier(within_operational_limit_min['ly_m3'][0],within_operational_limit_max['ly_m3'][0],"op")
                        within_operational_limit['ly_m6'],operational_limit_msg['ly_m6']=check_outlier.final_outlier(within_operational_limit_min['ly_m6'][0],within_operational_limit_max['ly_m6'][0],"op")
                        within_operational_limit['ly_m12'],operational_limit_msg['ly_m12']=check_outlier.final_outlier(within_operational_limit_min['ly_m12'][0],within_operational_limit_max['ly_m12'][0],"op")
                        
                        try:
                            outliermin_max['min']=within_outlier_limit_min['m6'][1]#has to be changed to m3 once regular structure is deefined 
                        except:
                            outliermin_max['min']=None
                        try:
                            outliermin_max['max']=within_outlier_limit_max['m6'][1]
                        except:
                            outliermin_max['max']=None
                        try:
                            operationalmin_max['min']=within_operational_limit_min['m6'][1]
                        except:
                            operationalmin_max['min']=None
                        try:
                            operationalmin_max['max']=within_operational_limit_max['m6'][1]
                        except:
                            operationalmin_max['max']=None
                        main_data_dict_key['within_outlier_limits']=within_outlier_limit
                        main_data_dict_key['within_operational_limits']=within_operational_limit
                        main_data_dict_key['outlier_limit_msg']=outlier_limit_msg
                        main_data_dict_key['operational_limit_msg']=operational_limit_msg
                        main_data_dict_key['z_score']=z_score
                        main_data_dict_key['outlier_limit_value']=outliermin_max
                        main_data_dict_key['operational_limit_value']=operationalmin_max
                        if key.endswith("_cp"):
                            # print(key)
                            print(within_operational_limit)
                            print(within_outlier_limit)
                            print(outliermin_max)
                            print(operationalmin_max)
                        
                        # exit()
                        maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data."+key:main_data_dict_key}})
                        # print("kiiiiiiiiiiiiiiiiiiiiii")
                        
                        
                    except:
                        
                        continue
                else:
                    outliermin_max={}
                    operationalmin_max={}
                    else_dict={"m3":None,"m6":None,"m12":None,"ly_m3":None,"ly_m6":None,"ly_m12":None}
                    outliermin_max['min']=None
                    outliermin_max['max']=None
                    operationalmin_max['min']=None
                    operationalmin_max['max']=None
                    main_data_dict_key['within_outlier_limits']=else_dict
                    main_data_dict_key['within_operational_limits']=else_dict
                    main_data_dict_key['outlier_limit_msg']=else_dict
                    main_data_dict_key['operational_limit_msg']=else_dict
                    main_data_dict_key['z_score']=else_dict
                    main_data_dict_key['outlier_limit_value']=outliermin_max
                    main_data_dict_key['operational_limit_value']=operationalmin_max
                    if key.endswith("_cp"):
                        print(key)
                        print(main_data_dict_key)
                    maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data."+key:main_data_dict_key}})
                

            except KeyError:
                continue
            except AttributeError:
                continue
            
             


    def update_maindb(self,index):
        
        #self.maindb.update_one({"ship_imo": int(self.ship_imo)},{"$set":{"processed_daily_data":self.base_main_data}})
        self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo)})[index],{"$set":{"processed_daily_data":self.base_main_data}})
            
        # document = {
        #     "ship_imo": self.ship_imo,
        #     "date": datetime.utcnow(),
        #     "historical":True,
        #     "processed_daily_data": self.base_main_data,
        #     "within_good_voyage_limit":True, #new
        #     "vessel_loaded_check":self.main_data['vessel_loaded_check'],
        #     "weather_api": self.main_data['weather_api'],
        #     "position_api": self.main_data['position_api'],
        #     "indices": self.main_data['indices'],
        #     "faults": self.main_data['faults'],
        #     "health_status": self.main_data['health_status'],
        #     "Equipment":self.main_data['Equipment']
        # }

        # print(self.rep_dt)
        # self.maindb.delete_one({"ship_imo": self.ship_imo,'processed_daily_data.rep_dt.processed':self.rep_dt})        
        # self.maindb.insert_one(document).inserted_id

    def update_maindb_alldoc(self):
        maindb = database.get_collection("Main_db")
        self.get_main_db()
        print("neeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeew")
        self.process_main_data()

    def process_main_data_predictions(self):
        "processing maindb data for updation ,only processed daily data for each identifier prediction value will be updated."
        self.base_main_data_prediction={}
        UIP = UpdateIndividualProcessors(configs=self.ship_configs,md=self.main_data,imo=self.ship_imo)
        self.key_replace_dict={}
        for key in self.ship_configs['data']:
            if pandas.isnull(self.ship_configs['data'][key]['identifier_old'])==False:
                self.key_replace_dict[self.ship_configs['data'][key]['identifier_old']]=key
        
        main_data_dict=self.main_data['processed_daily_data']
        ml_control=self.ship_configs['mlcontrol']
        indices=self.ship_configs['indices_data']
        for i in indices:
            if indices[i]['Derived']==True:
                input=indices[i]['input']
                ml_control[i]=input
        # print(ml_control)
        # print(main_data_dict['rep_dt']['processed'])
        # variable_list=['ext_temp1','ext_temp2','ext_temp3','ext_temp4','ext_temp5','ext_temp6','ext_temp7','ext_temp8','ext_temp9','ext_temp10','ext_temp11','ext_temp12','ext_tempavg','tc1_extin_temp','tc1_extout_temp']
        for key in ml_control:
            # if key=="pwr":
            print(key)
            if key in main_data_dict and pandas.isnull(main_data_dict[key]['processed'])==False:
                print(key)
                main_data_dict_key=main_data_dict[key]
                val=ml_control[key]
                for i in val:
                    if i=="" or i==" " or i=="  ":
                        val.remove(i)
                for j in val:
                    if j not in main_data_dict:
                        val.remove(j)
                        try:
                            val.append(self.key_replace_dict[j])
                        except:
                            val.append(j)
                val.append("vsl_load_bal")
                
                main_data_dict[key]['predictions']=None
                main_data_dict[key]['SPEy']=None
                main_data_dict[key]['t2_initial']=None
                main_data_dict[key]['ewma']=None
                main_data_dict[key]['cumsum']=None
                pred,spe,t2_initial,length_dataframe,ewma,cumsum=UIP.base_prediction_processor(key,main_data_dict_key,val,main_data_dict)
                main_data_dict[key]['predictions']=pred
                main_data_dict[key]['SPEy']=spe
                main_data_dict[key]['t2_initial']=t2_initial
                main_data_dict[key]['length_dataframe']=length_dataframe
                main_data_dict[key]['ewma']=ewma
                main_data_dict[key]['cumsum']=cumsum
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data."+key:main_data_dict[key]}})
                print("kiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")

                

    def update_maindb_predictions_alldoc(self):
        print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom")
        self.get_main_db()
        self.process_main_data_predictions()
        


    def processing_indices(self):
        ship_indices_data=self.ship_configs['indices_data']
        main_processed_daily_data=self.main_data['processed_daily_data']
        indice=Indice_Processing(self.ship_configs,self.main_data,self.ship_imo)
        self.key_replace_dict={}
        for key in self.ship_configs['data']:
            if pandas.isnull(self.ship_configs['data'][key]['identifier_old'])==False:
                self.key_replace_dict[self.ship_configs['data'][key]['identifier_old']]=key
        for i in ship_indices_data:
            # if i == "main_fuel_index":
            if ship_indices_data[i]['Derived']!=True:
                print(i)
                main_data=self.main_data
                indice_dict={}
                main_data_dict=main_data['processed_daily_data']
                # main_data_dict_key=main_data_dict[i]
                input=ship_indices_data[i]['input']
                output=ship_indices_data[i]['output']
                input_outcast=[]
                for j in input:
                    if j not in main_data_dict.keys():
                        input_outcast.append(j)
                        try:
                            input.append(self.key_replace_dict[j])
                        except:
                            input_outcast.remove(j)
                if len(input_outcast)>0:
                    for outcast in input_outcast:
                        input.remove(outcast)
                output_outcast=[]
                for j in output:
                    if j not in main_data_dict.keys():
                        output_outcast.append(j)
                        try:
                            output.append(self.key_replace_dict[j])  
                        except:
                            output_outcast.remove(j)
                if len(output_outcast)>0:
                    for outcast in output_outcast:
                        output.remove(outcast)
                                    
                input.append("vsl_load_bal")
                indice_dict['index_id']=ship_indices_data[i]['name']
                indice_dict['IndexName']=ship_indices_data[i]['short_names']
                indice_dict['ParamGroup']=None
                indice_dict['t2_initial']=None
                indice_dict['length_dataframe']=None
                val,t2_initial,length_dataframe,mewma_val,mewma_ucl=indice.base_prediction_processor(output,input,main_data_dict)
                indice_dict['SPEy']=val
                indice_dict['t2_initial']=t2_initial
                indice_dict['length_dataframe']=length_dataframe
                indice_dict['mewma_val']=mewma_val
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"independent_indices."+i:indice_dict}})
                print("kiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                
    def update_indices(self):
        print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom")
        self.get_main_db()
        self.processing_indices()

    #@check_status
    def process_weather_api_data(self):
        weather_api_data = 1 #= {}
        return weather_api_data

    #@check_status
    def process_position_api_data(self):
        position_api_data = 1
        return position_api_data
    
    #@check_status
    def process_positions(self):
        faults_data = 1 # Actual data here
        return faults_data

    #@check_status
    def process_faults(self):
        faults_data = 1 # Actual data here
        return faults_data

    #@check_status
    def process_health_status(self):
        health_status = 1 # Actual data here
        return health_status

    def update_good_voyage(self):
        print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom")
        self.get_main_db()
        self.good_voyage()

    def good_voyage(self,):
        maindb = database.get_collection("Main_db")
        speed=self.main_data['processed_daily_data']['speed_stw_calc']['processed']
        rpm=self.main_data['processed_daily_data']['rpm']['processed']
        w_force=self.main_data['processed_daily_data']['w_force']['processed']
        sea_state=self.main_data['processed_daily_data']['sea_st']['processed']
        stm_hrs=self.main_data['processed_daily_data']['stm_hrs']['processed']
        print(self.main_data['processed_daily_data']['draft_mean'])
        draft=self.main_data['processed_daily_data']['draft_mean']['within_operational_limits']['m6']
        rpm_op=self.main_data['processed_daily_data']['rpm']['within_operational_limits']['m6']
        print(self.main_data['processed_daily_data']['rep_dt']['processed'])
        if pandas.isnull(speed)==False and pandas.isnull(rpm_op)==False and pandas.isnull(w_force)==False and pandas.isnull(sea_state)==False and pandas.isnull(stm_hrs)==False and pandas.isnull(draft)==False:
            if speed>6 and rpm_op==True and w_force<5 and sea_state<5 and stm_hrs>10 and draft==True:
                maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"within_good_voyage_limit":True}})
                # self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,5,1,12)}})[index],{"$set":{"within_good_voyage_limit":True}})
            else:
                maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"within_good_voyage_limit":False}})
        else:
            maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"within_good_voyage_limit":False}})
        
    #@check_status
    def main_db_writer(self):
        
        """self.main_db = {}
        self.main_db["ship_imo"] = self.ship_imo
        self.main_db['date'] = datetime.utcnow()
        self.main_db['historical'] = False
        self.main_db['daily_data'] = self.data
        self.main_db['weather_api'] = self.process_weather_api_data()
        self.main_db['position_api'] = self.process_positions()
        self.main_db['indices'] = self.process_indices()
        self.main_db['faults']=self.process_faults()
        self.main_db['health_status']=self.process_health_status()
        db.Main_db.insert_one(self.main_db)"""
        eng=self.daily_data['data_available_engine']
        nav=self.daily_data['data_available_nav']
        document = {
            "ship_imo": self.ship_imo,
            "date": datetime.utcnow(),
            "historical":True,
            "timestamp":self.timestamp,
            "processed_daily_data": self.processed_daily_data,
            "within_good_voyage_limit":True, #new
            "vessel_loaded_check":self.vessel_load_check(),
            "data_available_engine":self.data_available_check(eng),
            "data_available_nav":self.data_available_check(nav),
            "weather_api": self.process_weather_api_data(),
            "position_api": self.process_positions(),
            # "indices": self.process_indices(),
            "faults": self.process_faults(),
            "health_status": self.process_health_status(),
            "Equipment":self.processed_equipment_data
        }

        

        return db.Main_db.insert_one(document).inserted_id
    
    def ad_all(self):
        print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom")
        self.get_daily_data(0)
        self.base_dict()
        self.process_daily_data()
        self.main_db_writer()
    
        
    def update_main_fuel(self):
        maindb = database.get_collection("Main_db")
        dst_main =maindb.find({"ship_imo":self.ship_imo,"within_good_voyage_limit":True},{"processed_daily_data.dst_last.processed":1,"_id":0})
        mainobject=json_util.dumps(dst_main)
        load=json_util.loads(mainobject)
        temp_list_2=[]
        self.get_main_db()
        for k in range(0,len(load)):
            try:
                temp_list_2.append(load[k]['processed_daily_data']['dst_last']['processed'])
            except KeyError:
                temp_list_2.append(None)
        temp_list_2=[x for x in temp_list_2 if x is not None]
        dst_mean=round(np.mean(temp_list_2),2)

        Q_y_m3=self.ship_configs['spe_limits']['main_fuel_per_dst']['m3']
        Q_y_m6=self.ship_configs['spe_limits']['main_fuel_per_dst']['m6']
        Q_y_m12=self.ship_configs['spe_limits']['main_fuel_per_dst']['m12']
        Q_y_ly_m3=self.ship_configs['spe_limits']['main_fuel_per_dst']['ly_m3']
        Q_y_ly_m6=self.ship_configs['spe_limits']['main_fuel_per_dst']['ly_m6']
        Q_y_ly_m12=self.ship_configs['spe_limits']['main_fuel_per_dst']['ly_m12']

        T_2_limit=self.ship_configs['t2_limits']['main_fuel_per_dst']

        ewma_limit_m3=self.ship_configs['ewma_limits']['main_fuel_per_dst']['m3']
        ewma_limit_m6=self.ship_configs['ewma_limits']['main_fuel_per_dst']['m6']
        ewma_limit_m12=self.ship_configs['ewma_limits']['main_fuel_per_dst']['m12']
        ewma_limit_ly_m3=self.ship_configs['ewma_limits']['main_fuel_per_dst']['ly_m3']
        ewma_limit_ly_m6=self.ship_configs['ewma_limits']['main_fuel_per_dst']['ly_m6']
        ewma_limit_ly_m12=self.ship_configs['ewma_limits']['main_fuel_per_dst']['ly_m12']

        Q_y_list={"m3":Q_y_m3,"m6":Q_y_m6,"m12":Q_y_m12,"ly_m3":Q_y_ly_m3,"ly_m6":Q_y_ly_m6,"ly_m12":Q_y_ly_m12}
        ewma_limit={"m3":ewma_limit_m3,"m6":ewma_limit_m6,"m12":ewma_limit_m12,"ly_m3":ewma_limit_ly_m3,"ly_m6":ewma_limit_ly_m6,"ly_m12":ewma_limit_ly_m12}
        Q_y={}
        t_2_ucl={}
        ewma_ucl={}
        for month in Q_y_list:
            if Q_y_list[month] and pandas.isnull(Q_y_list[month])==False and pandas.isnull(dst_mean)==False:
                spe_alpha={}
                for i in Q_y_list[month]:
                    spe_alpha[i]=Q_y_list[month][i]*dst_mean
                Q_y[month]=spe_alpha
            else:
                Q_y[month]=None
            
            try:
                if len(ewma_limit[month])>0 and pandas.isnull(dst_mean)==False:
                    ewma_alpha=[]
                    for i in ewma_limit[month]:
                        ewma_alpha.append(i*dst_mean)
                    ewma_ucl[month]=ewma_alpha
            except:
                ewma_ucl[month]=None
        
        if pandas.isnull(T_2_limit)==False and pandas.isnull(dst_mean)==False:
            for i in T_2_limit:
                t_2_ucl[i]=T_2_limit[i]*dst_mean
        else:
            t_2_ucl=None
        
        self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"spe_limits.main_fuel":Q_y,"t2_limits.main_fuel":t_2_ucl,"ewma_limits.main_fuel":ewma_ucl}})   
        print("ship_doneeeeeeeeee")
        try:
            self.get_main_db()
            # main_fuel_per_dst_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['m3']
            main_fuel_per_dst_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['m6']
            main_fuel_per_dst_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['m12']
            # main_fuel_per_dst_ly_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['ly_m3']
            # main_fuel_per_dst_ly_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['ly_m6']
            # main_fuel_per_dst_ly_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['ly_m12']

            # spe_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['m3']
            spe_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['m6']
            spe_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['m12']
            # spe_ly_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['ly_m3']
            # spe_ly_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['ly_m6']
            # spe_ly_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['ly_m12']

            # ewma_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['ewma']['m3']
            ewma_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['ewma']['m6']
            ewma_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['ewma']['m12']
            # ewma_ly_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['ewma']['y_m3']
            # ewma_ly_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['ewma']['y_m6']
            # ewma_ly_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['ewma']['y_m12']

            # T_2_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['m3']
            T_2_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['m6']
            T_2_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['m12']
            # T_2_ly_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['ly_m3']
            # T_2_ly_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['ly_m6']
            # T_2_ly_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['ly_m12']

            # stm_hrs=self.main_data['processed_daily_data']['stm_hrs']['processed']
            dst_last=self.main_data['processed_daily_data']['dst_last']['processed']
            pred={}
            spe_y={}
            t_2={}
            ewma={}
            # months_list={"m3":main_fuel_per_dst_m3,"m6":main_fuel_per_dst_m6,"m12":main_fuel_per_dst_m12,"ly_m3":main_fuel_per_dst_ly_m3,"ly_m6":main_fuel_per_dst_ly_m6,"ly_m12":main_fuel_per_dst_ly_m12}
            # spe_list={"m3":spe_m3,"m6":spe_m6,"m12":spe_m12,"ly_m3":spe_ly_m3,"ly_m6":spe_ly_m6,"ly_m12":spe_ly_m12}
            # T_2_list={"m3":T_2_m3,"m6":T_2_m6,"m12":T_2_m12,"ly_m3":T_2_ly_m3,"ly_m6":T_2_ly_m6,"ly_m12":T_2_ly_m12}
            # ewma_list={"m3":ewma_m3,"m6":ewma_m6,"m12":ewma_m12,"ly_m3":ewma_ly_m3,"ly_m6":ewma_ly_m6,"ly_m12":ewma_ly_m12}
            months_list={"m3":None,"m6":main_fuel_per_dst_m6,"m12":main_fuel_per_dst_m12,"ly_m3":None,"ly_m6":None,"ly_m12":None}
            spe_list={"m3":None,"m6":spe_m6,"m12":spe_m12,"ly_m3":None,"ly_m6":None,"ly_m12":None}
            T_2_list={"m3":None,"m6":T_2_m6,"m12":T_2_m12,"ly_m3":None,"ly_m6":None,"ly_m12":None}
            ewma_list={"m3":None,"m6":ewma_m6,"m12":ewma_m12,"ly_m3":None,"ly_m6":None,"ly_m12":None}
            
            main_fuel=self.main_data['processed_daily_data']['main_fuel']
            
            for month in months_list:
                if months_list[month] and pandas.isnull(months_list[month][1])==False and pandas.isnull(dst_last)==False:
                    temp_list=[]
                    if pandas.isnull(months_list[month][0])==False or months_list[month][0]!=None:
                        temp_list.append(months_list[month][0] * dst_last)
                    else:
                        temp_list.append(None)
                    temp_list.append(months_list[month][1] * dst_last)
                    if pandas.isnull(months_list[month][2])==False or months_list[month][2]!=None:
                        temp_list.append(months_list[month][2] * dst_last)
                    else:
                        temp_list.append(None)
                    pred[month]=temp_list
                else:
                    pred[month]=[]
                main_fuel['predictions']=pred

                if spe_list[month] and pandas.isnull(spe_list[month])==False and pandas.isnull(dst_last)==False:
                    spe_y[month]=spe_list[month] * dst_last
                else:
                    spe_y[month]=None
                main_fuel['SPEy']=spe_y

                if T_2_list[month] and pandas.isnull(T_2_list[month])==False and pandas.isnull(dst_last)==False:
                    t_2[month]=T_2_list[month] * dst_last
                else:
                    t_2[month]=None
                main_fuel['t2_initial']=t_2

                try:
                    if len(ewma_list[month])>0 and pandas.isnull(dst_last)==False:
                        ewma_alpha=[]
                        for j in ewma_list[month]:
                            ewma_alpha.append(j*dst_last)
                        ewma[month]=ewma_alpha
                except:
                    ewma[month]=None
                main_fuel['ewma']=ewma

                """if T_2_limit[month] and pandas.isnull(T_2_limit[month])==False:
                    t_2_ucl[month]=T_2_limit[month] * dst_mean
                    if pandas.isnull(t_2[month])==False or t_2[month]!=None:
                        if t_2[month]>t_2_ucl[month]:
                            t_2_anamoly[month]=False
                        else:
                            t_2_anamoly[month]=True
                    else:
                        t_2_anamoly[month]=None
                else: 
                    t_2_ucl[month]=None
                    t_2_anamoly[month]=None
                main_fuel['ucl_crit_beta']=t_2_ucl
                main_fuel['t2_anamoly']=t_2_anamoly

                if Q_y_list[month] and pandas.isnull(Q_y_list[month][1])==False:
                    temp_list1=[]
                    temp_list2=[]
                    if pandas.isnull(Q_y_list[month][0])==False or Q_y_list[month][0]!=None:
                        temp_list1.append(Q_y_list[month][0] * dst_mean)
                        if pandas.isnull(Q_y_list[month][0] * dst_mean)==False and pandas.isnull(main_fuel['SPEy'][month])==False:
                            if (Q_y_list[month][0] * dst_mean)<main_fuel['SPEy'][month]:
                                temp_list2.append(False)
                            else:
                                temp_list2.append(True)
                        else:
                            temp_list2.append(True)
                    else:
                        temp_list1.append(None)
                        temp_list2.append(None)

                    temp_list1.append(Q_y_list[month][1] *  dst_mean)
                    if pandas.isnull(Q_y_list[month][1] *  dst_mean)==False and pandas.isnull(main_fuel['SPEy'][month])==False:
                        if (Q_y_list[month][1] *  dst_mean)<main_fuel['SPEy'][month]:
                            temp_list2.append(False)
                        else:
                            temp_list2.append(True)
                    else:
                        temp_list2.append(True)

                    if pandas.isnull(Q_y_list[month][2])==False or Q_y_list[month][2]!=None:
                        temp_list1.append(Q_y_list[month][2] * dst_mean)
                        if pandas.isnull(Q_y_list[month][2] * dst_mean)==False and pandas.isnull(main_fuel['SPEy'][month])==False:
                            if (Q_y_list[month][2] * dst_mean)<main_fuel['SPEy'][month]:
                                temp_list2.append(False)
                            else:
                                temp_list2.append(True)
                        else:
                            temp_list2.append(True)
                    else:
                        temp_list1.append(None)
                        temp_list2.append(None)
                    Q_y[month]=temp_list1
                    spe_anamoly[month]=temp_list2
                else:
                    Q_y[month]=[]
                    spe_anamoly[month]=[]
                main_fuel['Q_y']=Q_y
                main_fuel['spe_anamoly']=spe_anamoly"""
            # self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[i],{"$set":{"processed_daily_data.main_fuel":main_fuel}})
            self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data.main_fuel":main_fuel}})
            print("done")
        except:
            print("nopeeeee")
            


    def update_sfoc(self):
        maindb = database.get_collection("Main_db")
        # maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
        stm_main =maindb.find({"ship_imo":self.ship_imo,"within_good_voyage_limit":True},{"processed_daily_data.stm_hrs.processed":1,"_id":0})
        mainobject=json_util.dumps(stm_main)
        load=json_util.loads(mainobject)
        temp_list_2=[]
        self.get_main_db()
        for k in range(0,len(load)):
            try:
                temp_list_2.append(load[k]['processed_daily_data']['stm_hrs']['processed'])
            except KeyError:
                temp_list_2.append(None)
        temp_list_2=[x for x in temp_list_2 if x is not None]
        stm_mean=round(np.mean(temp_list_2),2)
        # months_list=["m3","m6","m12","ly_m3","ly_m6","ly_m12"]
        # months_list2={"m3":"m3","m6":"m6","m12":"m12","ly_m3":"y_m3","ly_m6":"y_m6","ly_m12":"y_m12"}
        months_list=["m6","m12"]
        months_list2={"m6":"m6","m12":"m12"}
        Q_y={}
        t_2_ucl={}
        ewma_limit={}
        for month in months_list:
            if self.ship_configs['spe_limits']['main_fuel'][month] and pandas.isnull(self.ship_configs['spe_limits']['main_fuel'][month])==False and self.ship_configs['spe_limits']['pwr'][month] and pandas.isnull(self.ship_configs['spe_limits']['pwr'][month])==False and pandas.isnull(stm_mean)==False:
                spe_alpha={}
                for i in self.ship_configs['spe_limits']['main_fuel'][month]:
                    spe_alpha[i]=(self.ship_configs['spe_limits']['main_fuel'][month][i]/self.ship_configs['spe_limits']['pwr'][month][i]/stm_mean)*1000
                Q_y[month]=spe_alpha
            else:
                Q_y[month]=None

            try:
                if len(self.ship_configs['ewma_limits']['main_fuel'][month])>0 and len(self.ship_configs['ewma_limits']['pwr'][month])>0 and pandas.isnull(stm_mean)==False:
                    ewma_alpha=[]
                    for i in range(0,3):
                        ewma_alpha.append((self.ship_configs['ewma_limits']['main_fuel'][month][i]/self.ship_configs['ewma_limits']['pwr'][month][i]/stm_mean)*1000)
                    ewma_limit[month]=ewma_alpha
            except:
                ewma_limit[month]=None
        
        if pandas.isnull(self.ship_configs['t2_limits']['main_fuel'])==False and pandas.isnull(self.ship_configs['t2_limits']['pwr'])==False and pandas.isnull(stm_mean)==False:
            for i in self.ship_configs['t2_limits']['main_fuel']:
                t_2_ucl[i]=(self.ship_configs['t2_limits']['main_fuel'][i]/self.ship_configs['t2_limits']['pwr'][i]/stm_mean)*1000
        else:
            t_2_ucl=None
        
        self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"spe_limits.sfoc":Q_y,"t2_limits.sfoc":t_2_ucl,"ewma_limits.sfoc":ewma_limit}})   
        print("ship_doneeeeeeeeee")

        try:
            self.get_main_db()
            sfoc=self.main_data['processed_daily_data']['sfoc']
            main_fuel=self.main_data['processed_daily_data']['main_fuel']
            pwr=self.main_data['processed_daily_data']['pwr']
            stm_hrs=self.main_data['processed_daily_data']['stm_hrs']['processed']
            pred={}
            spe_y={}
            t_2={}
            ewma={}
            
            
            for month in months_list:
                if main_fuel['predictions'][month] and pandas.isnull(main_fuel['predictions'][month][1])==False and pwr['predictions'][month] and pandas.isnull(pwr['predictions'][month][1])==False and pandas.isnull(stm_hrs)==False:
                    temp_list=[]
                    if pandas.isnull(main_fuel['predictions'][month][0])==False or main_fuel['predictions'][month][0]!=None:
                        first_div=main_fuel['predictions'][month][0]/pwr['predictions'][month][1]
                        second_div=first_div/stm_hrs
                        val=second_div*1000
                        temp_list.append(val)
                    else:
                        temp_list.append(None)
                    first_div=main_fuel['predictions'][month][1]/pwr['predictions'][month][1]
                    second_div=first_div/stm_hrs
                    val=second_div*1000
                    temp_list.append(val)
                    if pandas.isnull(main_fuel['predictions'][month][2])==False or main_fuel['predictions'][month][2]!=None :
                        first_div=main_fuel['predictions'][month][2]/pwr['predictions'][month][1]
                        second_div=first_div/stm_hrs
                        val=second_div*1000
                        temp_list.append(val)
                    else:
                        temp_list.append(None)
                    pred[month]=temp_list
                else:
                    pred[month]=[]
                sfoc['predictions']=pred

                if main_fuel['SPEy'][month] and pandas.isnull(main_fuel['SPEy'][month])==False and pwr['SPEy'][month] and pandas.isnull(pwr['SPEy'][month])==False and pandas.isnull(stm_hrs)==False:
                    spe_y[month]=((main_fuel['SPEy'][month]/pwr['SPEy'][month])/stm_hrs)*1000
                else:
                    spe_y[month]=None
                sfoc['SPEy']=spe_y

                if main_fuel['t2_initial'][month] and pandas.isnull(main_fuel['t2_initial'][month])==False and pwr['t2_initial'][month] and pandas.isnull(pwr['t2_initial'][month])==False and pandas.isnull(stm_hrs)==False:
                    t_2[month]=((main_fuel['t2_initial'][month]/pwr['t2_initial'][month])/stm_hrs)*1000
                else:
                    t_2[month]=None
                sfoc['t2_initial']=t_2

                try:
                    if len(main_fuel['ewma'][month])>0 and len(pwr['ewma'][months_list2[month]])>0 and pandas.isnull(stm_hrs)==False:
                        ewma_alpha2=[]
                        for j in range(0,3):
                            ewma_alpha2.append((main_fuel['ewma'][month][i]/pwr['ewma'][months_list2[month]][j]/stm_hrs)*1000)
                        ewma[month]=ewma_alpha2
                    else:
                        ewma[month]=None
                except:
                    ewma[month]=None
                sfoc['ewma']=ewma
                
                """if main_fuel['ucl_crit_beta'][month] and pandas.isnull(main_fuel['ucl_crit_beta'][month])==False and pwr['ucl_crit_beta'][month] and pandas.isnull(pwr['ucl_crit_beta'][month])==False and pandas.isnull(stm_hrs)==False:
                    t_2_ucl[month]=((main_fuel['ucl_crit_beta'][month]/pwr['ucl_crit_beta'][month])/stm_mean)*1000
                    if pandas.isnull(t_2[month])==False or t_2[month]!=None:
                        if t_2[month]>t_2_ucl[month]:
                            t_2_anamoly[month]=False
                        else:                   
                            t_2_anamoly[month]=True                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
                    else:
                        t_2_anamoly[month]=None
                else: 
                    t_2_ucl[month]=None
                    t_2_anamoly[month]=None
                sfoc['ucl_crit_beta']=t_2_ucl
                sfoc['t2_anamoly']=t_2_anamoly

                if main_fuel['Q_y'][month] and pandas.isnull(main_fuel['Q_y'][month][1])==False and pwr['Q_y'][month] and pandas.isnull(pwr['Q_y'][month][1])==False and pandas.isnull(stm_hrs)==False:
                    temp_list1=[]
                    temp_list2=[]
                    if pandas.isnull(main_fuel['Q_y'][month][0])==False or main_fuel['Q_y'][month][0]!=None and pandas.isnull(pwr['Q_y'][month][0])==False or pwr['Q_y'][month][0]!=None:
                        temp_list1.append(((main_fuel['Q_y'][month][0]/pwr['Q_y'][month][0])/stm_mean)*1000)
                        if pandas.isnull(((main_fuel['Q_y'][month][0]/pwr['Q_y'][month][0])/stm_mean)*1000)==False and pandas.isnull(sfoc['SPEy'][month])==False:
                            if (((main_fuel['Q_y'][month][0]/pwr['Q_y'][month][0])/stm_mean)*1000)<sfoc['SPEy'][month]:
                                temp_list2.append(False)
                            else:
                                temp_list2.append(True)
                        else:
                            temp_list2.append(True)
                    else:
                        temp_list1.append(None)
                        temp_list2.append(None)

                    temp_list1.append(((main_fuel['Q_y'][month][1]/pwr['Q_y'][month][1])/stm_mean)*1000)
                    if pandas.isnull(((main_fuel['Q_y'][month][1]/pwr['Q_y'][month][1])/stm_mean)*1000)==False and pandas.isnull(sfoc['SPEy'][month])==False:
                        if (((main_fuel['Q_y'][month][1]/pwr['Q_y'][month][1])/stm_mean)*1000)<sfoc['SPEy'][month]:
                            temp_list2.append(False)
                        else:
                            temp_list2.append(True)
                    else:
                        temp_list2.append(True)

                    if pandas.isnull(main_fuel['Q_y'][month][2])==False or main_fuel['Q_y'][month][2]!=None and pandas.isnull(pwr['Q_y'][month][2])==False or pwr['Q_y'][month][2]!=None:
                        temp_list1.append(((main_fuel['Q_y'][month][2]/pwr['Q_y'][month][2])/stm_mean)*1000)
                        if pandas.isnull(((main_fuel['Q_y'][month][2]/pwr['Q_y'][month][2])/stm_mean)*1000)==False and pandas.isnull(sfoc['SPEy'][month])==False:
                            if (((main_fuel['Q_y'][month][2]/pwr['Q_y'][month][2])/stm_mean)*1000)<sfoc['SPEy'][month]:
                                temp_list2.append(False)
                            else:
                                temp_list2.append(True)
                        else:
                            temp_list2.append(True)
                    else:
                        temp_list1.append(None)
                        temp_list2.append(None)
                    Q_y[month]=temp_list1
                    spe_anamoly[month]=temp_list2
                else:
                    Q_y[month]=[]
                    spe_anamoly[month]=[]
                sfoc['Q_y']=Q_y
                sfoc['spe_anamoly']=spe_anamoly"""
            # self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[i],{"$set":{"processed_daily_data.sfoc":sfoc}})
            self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data.sfoc":sfoc}})
            print("done")
        except:
            print("nooooooooooooo")


    def update_avg_hfo(self):
        maindb = database.get_collection("Main_db")
        stm_main =maindb.find({"ship_imo":self.ship_imo,"within_good_voyage_limit":True},{"processed_daily_data.stm_hrs.processed":1,"_id":0})
        mainobject=json_util.dumps(stm_main)
        load=json_util.loads(mainobject)
        temp_list_2=[]
        self.get_main_db()
        for k in range(0,len(load)):
            try:
                temp_list_2.append(load[k]['processed_daily_data']['stm_hrs']['processed'])
            except KeyError:
                temp_list_2.append(None)
        temp_list_2=[x for x in temp_list_2 if x is not None]
        stm_mean=round(np.mean(temp_list_2),2)
        # months_list1=["m3","m6","m12","ly_m3","ly_m6","ly_m12"]
        months_list1=["m6","m12"]
        Q_y={}
        t_2_ucl={}
        ewma_limit={}
        for month in months_list1:
            if self.ship_configs['spe_limits']['main_fuel'][month] and pandas.isnull(self.ship_configs['spe_limits']['main_fuel'][month])==False and pandas.isnull(stm_mean)==False:
                spe_alpha={}
                for i in self.ship_configs['spe_limits']['main_fuel'][month]:
                    spe_alpha[i]=self.ship_configs['spe_limits']['main_fuel'][month][i]*(24/stm_mean)
                Q_y[month]=spe_alpha
            else:
                Q_y[month]=None

            try:
                if len(self.ship_configs['ewma_limits']['main_fuel'][month])>0 and  pandas.isnull(stm_mean)==False:
                    ewma_alpha=[]
                    for i in range(0,3):
                        ewma_alpha.append(self.ship_configs['ewma_limits']['main_fuel'][month][i]*(24/stm_mean))
                    ewma_limit[month]=ewma_alpha
            except:
                ewma_limit[month]=None
        
        if pandas.isnull(self.ship_configs['t2_limits']['main_fuel'])==False and pandas.isnull(stm_mean)==False:
            for i in self.ship_configs['t2_limits']['main_fuel']:
                t_2_ucl[i]=self.ship_configs['t2_limits']['main_fuel'][i]*(24/stm_mean)
        else:
            t_2_ucl=None
        
        self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"spe_limits.avg_hfo":Q_y,"t2_limits.avg_hfo":t_2_ucl,"ewma_limits.avg_hfo":ewma_limit}})   
        print("ship_doneeeeeeeeee")
        
        try:
            self.get_main_db()
            # main_fuel_per_dst_m3=self.main_data['processed_daily_data']['main_fuel']['predictions']['m3']
            main_fuel_per_dst_m6=self.main_data['processed_daily_data']['main_fuel']['predictions']['m6']
            main_fuel_per_dst_m12=self.main_data['processed_daily_data']['main_fuel']['predictions']['m12']
            # main_fuel_per_dst_ly_m3=self.main_data['processed_daily_data']['main_fuel']['predictions']['ly_m3']
            # main_fuel_per_dst_ly_m6=self.main_data['processed_daily_data']['main_fuel']['predictions']['ly_m6']
            # main_fuel_per_dst_ly_m12=self.main_data['processed_daily_data']['main_fuel']['predictions']['ly_m12']

            # spe_m3=self.main_data['processed_daily_data']['main_fuel']['SPEy']['m3']
            spe_m6=self.main_data['processed_daily_data']['main_fuel']['SPEy']['m6']
            spe_m12=self.main_data['processed_daily_data']['main_fuel']['SPEy']['m12']
            # spe_ly_m3=self.main_data['processed_daily_data']['main_fuel']['SPEy']['ly_m3']
            # spe_ly_m6=self.main_data['processed_daily_data']['main_fuel']['SPEy']['ly_m6']
            # spe_ly_m12=self.main_data['processed_daily_data']['main_fuel']['SPEy']['ly_m12']

            # T_2_m3=self.main_data['processed_daily_data']['main_fuel']['t2_initial']['m3']
            T_2_m6=self.main_data['processed_daily_data']['main_fuel']['t2_initial']['m6']
            T_2_m12=self.main_data['processed_daily_data']['main_fuel']['t2_initial']['m12']
            # T_2_ly_m3=self.main_data['processed_daily_data']['main_fuel']['t2_initial']['ly_m3']
            # T_2_ly_m6=self.main_data['processed_daily_data']['main_fuel']['t2_initial']['ly_m6']
            # T_2_ly_m12=self.main_data['processed_daily_data']['main_fuel']['t2_initial']['ly_m12']

            # ewma_m3=self.main_data['processed_daily_data']['main_fuel']['ewma']['m3']
            ewma_m6=self.main_data['processed_daily_data']['main_fuel']['ewma']['m6']
            ewma_m12=self.main_data['processed_daily_data']['main_fuel']['ewma']['m12']
            # ewma_ly_m3=self.main_data['processed_daily_data']['main_fuel']['ewma']['ly_m3']
            # ewma_ly_m6=self.main_data['processed_daily_data']['main_fuel']['ewma']['ly_m6']
            # ewma_ly_m12=self.main_data['processed_daily_data']['main_fuel']['ewma']['ly_m12']

            stm_hrs=self.main_data['processed_daily_data']['stm_hrs']['processed']
            pred={}
            spe_y={}
            Q_y={}
            t_2={}
            ewma={}

            # months_list={"m3":main_fuel_per_dst_m3,"m6":main_fuel_per_dst_m6,"m12":main_fuel_per_dst_m12,"ly_m3":main_fuel_per_dst_ly_m3,"ly_m6":main_fuel_per_dst_ly_m6,"ly_m12":main_fuel_per_dst_ly_m12}
            # spe_list={"m3":spe_m3,"m6":spe_m6,"m12":spe_m12,"ly_m3":spe_ly_m3,"ly_m6":spe_ly_m6,"ly_m12":spe_ly_m12}
            # T_2_list={"m3":T_2_m3,"m6":T_2_m6,"m12":T_2_m12,"ly_m3":T_2_ly_m3,"ly_m6":T_2_ly_m6,"ly_m12":T_2_ly_m12}
            # ewma_list={"m3":ewma_m3,"m6":ewma_m6,"m12":ewma_m12,"ly_m3":ewma_ly_m3,"ly_m6":ewma_ly_m6,"ly_m12":ewma_ly_m12}
            months_list={"m3":None,"m6":main_fuel_per_dst_m6,"m12":main_fuel_per_dst_m12,"ly_m3":None,"ly_m6":None,"ly_m12":None}
            spe_list={"m3":None,"m6":spe_m6,"m12":spe_m12,"ly_m3":None,"ly_m6":None,"ly_m12":None}
            T_2_list={"m3":None,"m6":T_2_m6,"m12":T_2_m12,"ly_m3":None,"ly_m6":None,"ly_m12":None}
            ewma_list={"m3":None,"m6":ewma_m6,"m12":ewma_m12,"ly_m3":None,"ly_m6":None,"ly_m12":None}
            
            avg_hfo=self.main_data['processed_daily_data']['avg_hfo']

            for month in months_list:
                if months_list[month] and pandas.isnull(months_list[month][1])==False and pandas.isnull(stm_hrs)==False:
                    
                    temp_list=[]
                    if pandas.isnull(months_list[month][0])==False or months_list[month][0]!=None:
                        temp_list.append(months_list[month][0] * (24/stm_hrs))
                    else:
                        temp_list.append(None)
                    temp_list.append(months_list[month][1] * (24/stm_hrs))
                    if pandas.isnull(months_list[month][2])==False or months_list[month][2]!=None:
                        temp_list.append(months_list[month][2] * (24/stm_hrs))
                    else:
                        temp_list.append(None)

                    pred[month]=temp_list
                else:
                    pred[month]=[]
                
                avg_hfo['predictions']=pred

                if spe_list[month] and pandas.isnull(spe_list[month])==False and pandas.isnull(stm_hrs)==False:
                    spe_y[month]=spe_list[month] * (24/stm_hrs)
                else:
                    spe_y[month]=None
                
                avg_hfo['SPEy']=spe_y

                if T_2_list[month] and pandas.isnull(T_2_list[month])==False and pandas.isnull(stm_hrs)==False:
                    t_2[month]=T_2_list[month] * (24/stm_hrs)
                else:
                    t_2[month]=None
                avg_hfo['t2_initial']=t_2

                try:
                    if len(ewma_list[month])>0 and pandas.isnull(stm_hrs)==False:
                        ewma_alpha=[]
                        for j in ewma_list[month]:
                            ewma_alpha.append(j*(24/stm_hrs))
                        ewma[month]=ewma_alpha
                except:
                    ewma[month]=None
                avg_hfo['ewma']=ewma



                """if Q_y_list[month] and pandas.isnull(Q_y_list[month][1])==False and pandas.isnull(stm_hrs)==False:
                    temp_list1=[]
                    if pandas.isnull(Q_y_list[month][0])==False or Q_y_list[month][0]!=None:
                        temp_list1.append(Q_y_list[month][0] *  (24/stm_hrs))
                    else:
                        temp_list1.append(None)
                    temp_list1.append(Q_y_list[month][1] *  (24/stm_hrs))
                    if pandas.isnull(Q_y_list[month][2])==False or Q_y_list[month][2]!=None:
                        temp_list1.append(Q_y_list[month][2] *  (24/stm_hrs))
                    else:
                        temp_list1.append(None)

                    Q_y[month]=temp_list1
                else:
                    Q_y[month]=[]

                avg_hfo['Q_y']=Q_y"""
                
            # self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[i],{"$set":{"processed_daily_data.avg_hfo":avg_hfo}})
            self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data.avg_hfo":avg_hfo}})
            print("done")    
            
            
        except:
            print("nopeee")
            
    
    
    def ewma_limits(self):
        maindb = database.get_collection("Main_db")
        ml_control=self.ship_configs['mlcontrol']
        indices=self.ship_configs['indices_data']
        self.get_main_db()
        for i in indices:
            if indices[i]['Derived']==True:
                input=indices[i]['input']
                ml_control[i]=input
        months=['m3','m6','m12','ly_m3','ly_m6','ly_m12']
        alpha=[0.2,0.1,0.05]
        t2_alpha_str={"0.2":"zero_two","0.1":"zero_one","0.05":"zero_zero_five"}
        for j in ml_control:
            # if j=="pwr":
            try:
                print(j)
                ewma_limits={}
                spe_limits={}
                for month in months:
                    try:
                        print(month)
                        spe_main =maindb.find({"ship_imo":self.ship_imo,"within_good_voyage_limit":True,"processed_daily_data."+j+".SPEy."+month:{"$lte":8}},{"processed_daily_data."+j+".SPEy."+month:1,"_id":0})
                        mainobject=json_util.dumps(spe_main)
                        load=json_util.loads(mainobject)
                        temp_list_2=[]
                        for k in range(0,len(load)):
                            try:
                                temp_list_2.append(load[k]['processed_daily_data'][j]['SPEy'][month])
                            except KeyError:
                                temp_list_2.append(None)
                        spe_data=pandas.DataFrame({"spe":temp_list_2})
                        spe_data=spe_data.dropna()
                        spe_data=spe_data.reset_index(drop=True)
                        std_val=np.std(spe_data['spe'])
                        mean_val=np.mean(spe_data['spe'])
                        var_sped=np.var(spe_data['spe'])
                        ewma_limit_list=[]
                        h_val=2*(mean_val**2)/(var_sped)
                        g_val=(var_sped)/(2*(mean_val))
                        spe_y_limit_array={}
                        for alphas in alpha:
                            ewma_obj=EWMA()
                            ewma_obj.fit(spe_data['spe'],0.2,mean_val)
                            L=st.norm.ppf(1-alphas)
                            ewma_val_cal,ewma_ucl_cal,ewma_lcl_cal=ewma_obj.ControlChart(L=L,sigma=std_val)
                            # ewma_val_cal_2=spe_data['spe'].ewm(alpha=0.05,adjust=False).mean()
                            # ewma_val=round(ewma_val_cal[-1],2)
                            # ewma_ucl_val=round(ewma_ucl_cal[-1],2)
                            ewma_limit_list.append(round(ewma_ucl_cal[-1],2))
                            chi_val=st.chi2.ppf(1-alphas, h_val)
                            spe_limit_g_val=g_val*chi_val
                            spe_y_limit_array[t2_alpha_str[str(alphas)]]=spe_limit_g_val
                            
                        ewma_limits[month]=ewma_limit_list
                        spe_limits[month]=spe_y_limit_array
                        
                    except:
                        ewma_limits[month]=None
                        spe_limits[month]=None
                
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"spe_limits."+j:spe_limits,"ewma_limits."+j:ewma_limits}})
                print("kiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
            except:
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"spe_limits."+j:None,"ewma_limits."+j:None}})
                print("nuuuuuuuuuuuuuuuuuuuuuuuuuuuuullllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll")
    

    def indice_ewma_limit(self):
        maindb = database.get_collection("Main_db")
        ship_indices_data=self.ship_configs['indices_data']
        months=['m3','m6','m12','ly_m3','ly_m6','ly_m12']
        alpha=[0.2,0.1,0.05]
        t2_alpha_str={"0.2":"zero_two","0.1":"zero_one","0.05":"zero_zero_five"}
        self.get_main_db()
        for j in ship_indices_data:
            if ship_indices_data[j]['Derived']!=True:
                try:
                    print(j)
                    mewma_limits={}
                    spe_limits={}
                    for month in months:
                        try:
                            print(month)
                            spe_main =maindb.find({"ship_imo":self.ship_imo,"within_good_voyage_limit":True,"independent_indices."+j+".SPEy."+month:{"$lte":8}},{"independent_indices."+j+".SPEy."+month:1,"_id":0})
                            mainobject=json_util.dumps(spe_main)
                            load=json_util.loads(mainobject)
                            temp_list_2=[]
                            for k in range(0,len(load)):
                                try:
                                    temp_list_2.append(load[k]['independent_indices'][j]['SPEy'][month])
                                except KeyError:
                                    temp_list_2.append(None)
                            spe_data=pandas.DataFrame({"spe":temp_list_2})
                            spe_data=spe_data.dropna()
                            spe_data=spe_data.reset_index(drop=True)
                            std_val=np.std(spe_data['spe'])
                            mean_val=np.mean(spe_data['spe'])
                            var_sped=np.var(spe_data['spe'])
                            mewma_limit_list=[]
                            h_val=2*(mean_val**2)/(var_sped)
                            g_val=(var_sped)/(2*(mean_val))
                            spe_y_limit_array={}
                            for alphas in alpha:
                                mewma_obj=EWMA()
                                mewma_obj.fit(spe_data['spe'],0.2,mean_val)
                                L=st.norm.ppf(1-alphas)
                                mewma_val_cal,mewma_ucl_cal,mewma_lcl_cal=mewma_obj.ControlChart(L=L,sigma=std_val)
                                # ewma_val_cal_2=spe_data['spe'].ewm(alpha=0.05,adjust=False).mean()
                                # ewma_val=round(ewma_val_cal[-1],2)
                                # ewma_ucl_val=round(ewma_ucl_cal[-1],2)
                                mewma_limit_list.append(round(mewma_ucl_cal[-1],2))
                                chi_val=st.chi2.ppf(1-alphas, h_val)
                                spe_limit_g_val=g_val*chi_val
                                spe_y_limit_array[t2_alpha_str[str(alphas)]]=spe_limit_g_val
                            mewma_limits[month]=mewma_limit_list 
                            spe_limits[month]=spe_y_limit_array
                        except:
                            mewma_limits[month]=None
                            spe_limits[month]=None
                    self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"spe_limits_indices."+j:spe_limits,"mewma_limits."+j:mewma_limits}})
                    print("kiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                except:
                    self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"spe_limits_indices."+j:None,"mewma_limits."+j:None}})
                    print("nuuuuuuuuuuuuuuuuuuuuuuuuuuuuullllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll")

                
    def universal_limit(self):
        ml_control=self.ship_configs['mlcontrol']
        indices=self.ship_configs['indices_data']
        self.key_replace_dict={}
        self.get_main_db()
        for key in self.ship_configs['data']:
            if pandas.isnull(self.ship_configs['data'][key]['identifier_old'])==False:
                self.key_replace_dict[self.ship_configs['data'][key]['identifier_old']]=key
        for i in indices:
            if indices[i]['Derived']==True:
                input=indices[i]['input']
                ml_control[i]=input
        for key in ml_control:
            # if key=="i_2":
            val=ml_control[key]
            for i in val:
                if i=="" or i==" " or i=="  ":
                    val.remove(i)
                if i not in self.ship_configs['data']:
                    val.remove(i)
                    if i in self.key_replace_dict:
                        val.append(self.key_replace_dict[i])
            val.append("vsl_load_bal")
            try:
                print(key)
                full_data=Universal_limits(key,val,self.ship_imo,self.ship_configs)
                # spe_limits,t2_limits=full_data.spe_limit()
                t2_limits=full_data.spe_limit()
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"t2_limits."+key:t2_limits}})
                print("done")
            except:
                print(key)
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"t2_limits."+key:None}})
                print("no")


    def anamolies_by_config(self):
        ml_control=self.ship_configs['mlcontrol']
        indices=self.ship_configs['indices_data']
        for i in indices:
            if indices[i]['Derived']==True:
                input=indices[i]['input']
                ml_control[i]=input
        ml_list=list(ml_control.keys())
        if "avg_hfo" not in ml_list:
            ml_list.append("avg_hfo")
        if "main_fuel" not in ml_list:
            ml_list.append("main_fuel")
        if "sfoc" not in ml_list:
            ml_list.append("sfoc")
        self.get_main_db()
        spe_limits=self.main_data['spe_limits']
        t2_limits=self.main_data['t2_limits']
        ewma_limits=self.main_data['ewma_limits']
        # months=['m3','m6','m12','ly_m3','ly_m6','ly_m12']
        months=['m6','m12']
        alpha=['zero_two','zero_one','zero_zero_five']
        spe_anamoly={}
        t2_anamoly={}
        ewma_anamoly={}
        spe_messages={}
        t2_messages={}
        ewma_messages={}


        indice_list=[]
        for i in indices:
            if indices[i]['Derived']!=True:
                indice_list.append(i)

        spe_indice_limits=self.ship_configs['spe_limits_indices']
        t2_indice_limits=self.ship_configs['t2_limits_indices']
        mewma_limits=self.ship_configs['mewma_limits']
        t2_indice_anamoly={}
        spe_indice_anamoly={}
        mewma_anamoly={}
        t2_indice_messages={}
        spe_indice_messages={}
        mewma_messages={}


        try:
            for key in ml_list:
                try:
                    if key in self.main_data['processed_daily_data']:
                        print(key)
                        for month in months:
                            spe_list=[]
                            t2_list=[]
                            ewma_list=[]
                            if pandas.isnull(self.main_data['processed_daily_data'][key]['SPEy'][month])==False:
                                for alphas in alpha:
                                    if spe_limits[key][month][alphas]>self.main_data['processed_daily_data'][key]['SPEy'][month]:
                                        spe_list.append(True)
                                    else:
                                        spe_list.append(False)
                            else:
                                spe_list=None
                            spe_anamoly[month]=spe_list
                            if pandas.isnull(self.main_data['processed_daily_data'][key]['t2_initial'][month])==False:
                                for alphas in alpha:
                                    if t2_limits[key][alphas]>self.main_data['processed_daily_data'][key]['t2_initial'][month]:
                                        t2_list.append(True)
                                    else:
                                        t2_list.append(False)
                            else:
                                t2_list=None
                            t2_anamoly[month]=t2_list
                            try:
                                if len(self.main_data['processed_daily_data'][key]['ewma'][month])>0:
                                    for j in range(0,3):
                                        if ewma_limits[key][month][j]>self.main_data['processed_daily_data'][key]['ewma'][month][j]:
                                            ewma_list.append(True)
                                        else:
                                            ewma_list.append(False)
                                else:
                                    ewma_list.append(False)
                            except:
                                ewma_list=None
                            ewma_anamoly[month]=ewma_list
                        for month in months:
                            spe_msg_list=[]
                            t2_msg_list=[]
                            ewma_msg_list=[]
                            if len(spe_anamoly[month])>0:
                                for ind in range(0,3):
                                    if spe_anamoly[month][ind]==False:
                                        print("false_ speeeeeeeee here")
                                        if ind==0:
                                            spe_msg_list.append(self.ship_configs['parameter_anamoly']['SPE_alpha1']['message'])
                                        elif ind==1:
                                            spe_msg_list.append(self.ship_configs['parameter_anamoly']['SPE_alpha2']['message'])
                                        elif ind==2:
                                            spe_msg_list.append(self.ship_configs['parameter_anamoly']['SPE_alpha3']['message'])
                                    else:
                                        spe_msg_list.append(None)
                                spe_messages[month]=spe_msg_list

                            if len(t2_anamoly[month])>0:
                                for ind in range(0,3):
                                    if t2_anamoly[month][ind]==False:
                                        print("false t2 hereeeeeeeeeeeee")
                                        if ind==0:
                                            t2_msg_list.append(self.ship_configs['parameter_anamoly']['T2_alpha1']['message'])
                                        elif ind==1:
                                            t2_msg_list.append(self.ship_configs['parameter_anamoly']['T2_alpha2']['message'])
                                        elif ind==2:
                                            t2_msg_list.append(self.ship_configs['parameter_anamoly']['T2_alpha3']['message'])
                                    else:
                                        t2_msg_list.append(None)
                                t2_messages[month]=t2_msg_list

                            if len(ewma_anamoly[month])>0:
                                for ind in range(0,3):
                                    if ewma_anamoly[month][ind]==False:
                                        print("false t2 hereeeeeeeeeeeee")
                                        if ind==0:
                                            ewma_msg_list.append(self.ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha1']['message'])
                                        elif ind==1:
                                            ewma_msg_list.append(self.ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha2']['message'])
                                        elif ind==2:
                                            ewma_msg_list.append(self.ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha3']['message'])
                                    else:
                                        ewma_msg_list.append(None)
                                ewma_messages[month]=ewma_msg_list
                        self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"processed_daily_data."+key+".is_not_spe_anamolous":spe_anamoly,"processed_daily_data."+key+".is_not_t2_anamolous":t2_anamoly,"processed_daily_data."+key+".is_not_ewma_anamolous":ewma_anamoly,"processed_daily_data."+key+".spe_messages":spe_messages,"processed_daily_data."+key+"._t2_messages":t2_messages,"processed_daily_data."+key+".ewma_messages":ewma_messages}})
                except:
                    continue
            
            for key in indice_list:
                print("indiiiiiiiiiiiiiiiiiiicesssssssssssssssssssssssss")
                try:
                    if key in self.main_data[i]['independent_indices']:
                        for month in months:
                            spe_indice_list=[]
                            t2_indice_list=[]
                            mewma_list=[]
                            if pandas.isnull(self.main_data[i]['independent_indices'][key]['SPEy'][month])==False:
                                for alphas in alpha:
                                    if spe_indice_limits[key][month][alphas]>self.main_data[i]['independent_indices'][key]['SPEy'][month]:
                                        spe_indice_list.append(True)
                                    else:
                                        spe_indice_list.append(False)
                            else:
                                spe_indice_list=None
                            spe_indice_anamoly[month]=spe_indice_list
                            if pandas.isnull(self.main_data[i]['independent_indices'][key]['t2_initial'][month])==False:
                                for alphas in alpha:
                                    if t2_indice_limits[key][alphas]>self.main_data[i]['independent_indices'][key]['t2_initial'][month]:
                                        t2_indice_list.append(True)
                                    else:
                                        t2_indice_list.append(False)
                            else:
                                t2_indice_list=None
                            t2_indice_anamoly[month]=t2_indice_list
                            if pandas.isnull(self.main_data[i]['independent_indices'][key]['mewma_val'][month])==False:
                                for j in range(0,3):
                                    if mewma_limits[key][month][j]>self.main_data[i]['independent_indices'][key]['mewma_val'][month]:
                                        mewma_list.append(True)
                                    else:
                                        mewma_list.append(False)
                            else:
                                mewma_list.append(None)
                            
                            mewma_anamoly[month]=mewma_list
                        for month in months:
                            spe_indice_msg_list=[]
                            t2_indice_msg_list=[]
                            mewma_msg_list=[]
                            if len(spe_indice_anamoly[month])>0:
                                for ind in range(0,3):
                                    if spe_indice_anamoly[month][ind]==False:
                                        print("false_ speeeeeeeee here")
                                        if ind==0:
                                            spe_indice_msg_list.append(self.ship_configs['parameter_anamoly']['SPE_alpha1']['message']+str(self.ship_configs['data'][key]['spe_rule_based_message']))
                                        elif ind==1:
                                            spe_indice_msg_list.append(self.ship_configs['parameter_anamoly']['SPE_alpha2']['message']+str(self.ship_configs['data'][key]['spe_rule_based_message']))
                                        elif ind==2:
                                            spe_indice_msg_list.append(self.ship_configs['parameter_anamoly']['SPE_alpha3']['message']+str(self.ship_configs['data'][key]['spe_rule_based_message']))
                                    else:
                                        spe_indice_msg_list.append(None)
                                spe_indice_messages[month]=spe_indice_msg_list

                            if len(t2_indice_anamoly[month])>0:
                                for ind in range(0,3):
                                    if t2_indice_anamoly[month][ind]==False:
                                        print("false t2 hereeeeeeeeeeeee")
                                        if ind==0:
                                            t2_indice_msg_list.append(self.ship_configs['parameter_anamoly']['T2_alpha1']['message']+str(self.ship_configs['data'][key]['t2_rule_based_message']))
                                        elif ind==1:
                                            t2_indice_msg_list.append(self.ship_configs['parameter_anamoly']['T2_alpha2']['message']+str(self.ship_configs['data'][key]['t2_rule_based_message']))
                                        elif ind==2:
                                            t2_indice_msg_list.append(self.ship_configs['parameter_anamoly']['T2_alpha3']['message']+str(self.ship_configs['data'][key]['t2_rule_based_message']))
                                    else:
                                        t2_indice_msg_list.append(None)
                                t2_indice_messages[month]=t2_indice_msg_list

                            if len(mewma_anamoly[month])>0:
                                for ind in range(0,3):
                                    if mewma_anamoly[month][ind]==False:
                                        print("false mewma hereeeeeeeeeeeee")
                                        if ind==0:
                                            mewma_msg_list.append(self.ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha1']['message']+str(self.ship_configs['data'][key]['ewma_rule_based_message']))
                                        elif ind==1:
                                            mewma_msg_list.append(self.ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha2']['message']+str(self.ship_configs['data'][key]['ewma_rule_based_message']))
                                        elif ind==2:
                                            mewma_msg_list.append(self.ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha3']['message']+str(self.ship_configs['data'][key]['ewma_rule_based_message']))
                                    else:
                                        mewma_msg_list.append(None)
                                mewma_messages[month]=mewma_msg_list
                            
                            


                    # print(spe_anamoly)
                    # print(t2_anamoly)
                    # print(ewma_anamoly)

                    # maindb.update_one(maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[i],{"$set":{"processed_daily_data."+key+".spe_anamoly":spe_anamoly,"processed_daily_data."+key+".t2_anamoly":t2_anamoly,"processed_daily_data."+key+".ewma_anamoly":ewma_anamoly}})
                    self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"independent_indices."+key+".is_not_spe_anamolous":spe_indice_anamoly,"independent_indices."+key+".is_not_t2_anamolous":t2_indice_anamoly,"independent_indices."+key+".is_not_mewma_anamolous":mewma_anamoly,"independent_indices."+key+".spe_messages":spe_indice_messages,"independent_indices."+key+"._t2_messages":t2_indice_messages,"independent_indices."+key+".mewma_messages":mewma_messages}})
                    print("doneeeeeeee")
                except:
                    continue
        except:
            pass
    
    def universal_indices_limits(self):
        ship_indices_data=self.ship_configs['indices_data']
        self.key_replace_dict={}
        self.get_main_db()
        for key in self.ship_configs['data']:
            if pandas.isnull(self.ship_configs['data'][key]['identifier_old'])==False:
                self.key_replace_dict[self.ship_configs['data'][key]['identifier_old']]=key
        for key in ship_indices_data:
            if ship_indices_data[key]['Derived']!=True:
                input=ship_indices_data[key]['input']
                output=ship_indices_data[key]['output']
                input.append("vsl_load_bal")
                for i in input:
                    if i=="" or i==" " or i=="  ":
                        input.remove(i)
                for i in output:
                    if i=="" or i==" " or i=="  ":
                        output.remove(i)
                input_outcast=[]
                for j in input:
                    if j not in self.ship_configs['data']:
                        input_outcast.append(j)
                        if j in self.key_replace_dict:
                            input.append(self.key_replace_dict[j])
                if len(input_outcast)>0:
                    for outcast in input_outcast:
                        input.remove(outcast)
                output_outcast=[]
                for j in output:
                    if j not in self.ship_configs['data']:
                        output_outcast.append(j)
                        if j in self.key_replace_dict:
                            output.append(self.key_replace_dict[j])  
                if len(output_outcast)>0:
                    for outcast in output_outcast:
                        output.remove(outcast)
                try:
                    print(key)
                    full_data=Universal_indices_limits(output,input,self.ship_imo,self.ship_configs)
                    # spe_limits,t2_limits=full_data.indices_limit()
                    t2_limits=full_data.indices_limit()
                    self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"t2_limits_indices."+key:t2_limits}})
                    print("done")
                except:
                    print(key)
                    self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"t2_limits_indices."+key:None}})
                    print("no")

    def update_cp_msg(self):
        self.get_main_db()
        try:
            main_fuel_cp_olmax=self.main_data['processed_daily_data']['main_fuel_cp']['operational_limit_value']['max']
            w_force_olmin=self.main_data['processed_daily_data']['w_force_cp']['operational_limit_value']['min']
            if pandas.isnull(self.main_data['processed_daily_data']['main_fuel']['processed'])==False and pandas.isnull(self.main_data['processed_daily_data']['w_force']['processed'])==False:
                if pandas.isnull(main_fuel_cp_olmax)==False and pandas.isnull(w_force_olmin)==False:
                    if self.main_data['processed_daily_data']['main_fuel']['processed']>main_fuel_cp_olmax and self.main_data['processed_daily_data']['w_force']['processed']<=w_force_olmin:
                        self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"cp_message":self.ship_configs['data']['main_fuel_cp']['rule_based_message']}})
                        print("doneeeeee")
                    else:
                        self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"cp_message":None}})
        except:
            self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"timestamp":self.timestamp,"processed_daily_data.rep_dt.processed":self.report_date})[0],{"$set":{"cp_message":None}})

    def write_ship_stats(self):
        "writing into shipstats"
            
        # print(max(m3.values()))
        # print(max(m3,key=m3.get))

    # def update_sfoc_processed(self):
    #     maindb = database.get_collection("Main_db")
    #     maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
    #     for i in range(0,maindata.count()):
    #         print(i)
    #         self.get_main_db(i)
    #         processed_val=self.main_data['processed_daily_data']['sfoc']['processed']
    #         if pandas.isnull(processed_val)==False and processed_val!=None:
    #             final_val=processed_val*1000
    #             self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[i],{"$set":{"processed_daily_data.sfoc.processed":final_val}})
    #             print("done")
    #         else:
    #             print("nullllllllllll")






# obj=MainDB(9591301)

start_time = time.time()

# daily_obj=DailyInsert('F:\Afzal_cs\Internship\Arvind data files\RTM FUEL.xlsx','F:\Afzal_cs\Internship\Arvind data files\RTM ENGINE.xlsx',9591301,True)
# daily_obj.do_steps()
obj=MainDB(9591301,400,datetime.strptime('22/3/18','%d/%m/%y'))
obj.get_ship_configs()
obj.get_main_db(0)
obj.ad_all()
# obj.add_calc_i_cp()
# obj.maindb_lvl_two()
#initial population done (remove date condition on find  before uploading in aws)

# obj.update_maindb_alldoc()

#outlier (both outlier 1 and 2 inside this) and (remove date condition on find  before uploading in aws)
# obj.update_good_voyage()
#good voyage tag created here essential for predictions process 
# obj.update_maindb_predictions_alldoc()
#predictions, spe, t2, ewma, cumsum all done here (remove date condition on find  before uploading in aws)
# obj.update_indices()
#creating indices as well as prediction, spe, t2, mewma, mcumsum, all done here

# obj.universal_limit()
# obj.universal_indices_limits()
# obj.ewma_limits()
# obj.indice_ewma_limit()

# obj.update_main_fuel()
#backcalculationg main fuel by given furlmula (all values which are created in predictions processe will be backcalculated with same formula)
# obj.update_sfoc()
#backcalculating 
# obj.update_avg_hfo()
#Backcalculating
# obj.update_cp_msg()
obj.anamolies_by_config()

#temporarily added for checking spe and ewma using a diferent dataframe and formula
# done till here  

end_time=time.time()
print(end_time-start_time)