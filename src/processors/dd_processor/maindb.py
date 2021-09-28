import sys

import pandas 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.processors.dd_processor.individual_processors import IndividualProcessors
from src.processors.dd_processor.outlier_two import OutlierTwo
from src.processors.dd_processor.update_individual_processor import UpdateIndividualProcessors
from src.configurations.logging_config import CommonLogger
from datetime import datetime
from src.db.schema.main import Main_db
from src.db.schema.ship import Ship 
from src.db.schema.ddschema import DailyData
import numpy as np
from pymongo import MongoClient
import random
from src.processors.config_extractor.outlier import CheckOutlier


log = CommonLogger(__name__,debug=True).setup_logger()


client = MongoClient("mongodb://localhost:27017/aranti")
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

    def __init__(self,ship_imo):
        self.ship_imo=ship_imo 
          #change later
        pass

      

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
        self.get_ship_stats()
        self.process_daily_data()
        self.process_weather_api_data()
        self.process_position_api_data()
        self.process_indices()
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
        ship_configs_collection = database.get_collection("ship")
        self.ship_configs = ship_configs_collection.find({"ship_imo": self.ship_imo})[0]
        

    #@check_status
    def get_daily_data(self,index):
        daily_data_collection =database.get_collection("daily_data")
        self.daily_data = daily_data_collection.find({"ship_imo": self.ship_imo})[index]
        # self.daily_data = daily_data_collection.find({"ship_imo": self.ship_imo})[0]
            

    #@check_status
    def get_ship_stats(self):
        ship_stats_collection = database.get_collection("Ship_Stats")
        self.ship_stats = ship_stats_collection.find({"ship_imo": self.ship_imo})[0]

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
        vsl_load=self.daily_data['data']['vsl_load']
        if pandas.isnull(vsl_load):
            return "not checked"
        elif vsl_load==1:
            return "Loaded"
        elif vsl_load==0:
            return "Ballast"      

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
                input_list=[]
                output_list=[]
                if pandas.isnull(input)==False:
                    for i in input.split():
                        i=i.replace(',','')
                        input_list.append(i)
                if pandas.isnull(output)==False:
                    for i in output.split():
                        i=i.replace(',','')
                        output_list.append(i)
                equipment_base_dict['input_param']=input_list
                equipment_base_dict['output_param']=output_list
                self.processed_equipment_data[key]=equipment_base_dict
                # equipment t2 spe codes functin here later
            else:
                base_dict = self.build_base_dict(key)
                try:
                    self.ship_configs['data'][key]
                    self.processed_daily_data[key] = eval("IP."+key.strip()+"_processor")(base_dict) # IP.rpm_processor(base_dict)
                except KeyError:
                    continue
                except AttributeError:
                    continue
            

    def get_main_db(self,index):
        self.maindb = database.get_collection("Main_db")
        # self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo)})[0]
        self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo)})[index]

    def get_outlier(self,identifier,identifier_value): 
        identifier=identifier
        identifier_value=identifier_value
        check_outlier=CheckOutlier(configs=self.ship_configs,main_db=self.main_data)
        within_outlier_limit=check_outlier.Outlierlimitcheck(identifier,identifier_value)
        return within_outlier_limit

    def get_operational_outlier(self,identifier,identifier_value):
        identifier=identifier
        identifier_value=identifier_value
        check_outlier=CheckOutlier(configs=self.ship_configs,main_db=self.main_data)
        within_operational_limit=check_outlier.operational_limit(identifier,identifier_value)
        return within_operational_limit

    def process_main_data(self):
        "processing maindb data for updation ,only processed daily data will be updated."
        self.base_main_data={}
        #UIP = UpdateIndividualProcessors(configs=self.ship_configs,md=self.main_data,ss=self.ship_stats)
        main_data_dict=self.main_data['processed_daily_data']
        for key in main_data_dict:
            try:
                main_data_dict_key=main_data_dict[key] 
                if pandas.isnull(main_data_dict_key['processed'])==False:
                    try:
                        within_outlier_limit=self.get_outlier(key,main_data_dict_key['processed'])
                        within_operational_limits=self.get_operational_outlier(key,main_data_dict_key['processed'])    
                        main_data_dict_key['within_outlier_limits']=within_outlier_limit    
                        main_data_dict_key['within_operational_limits']=within_operational_limits                  
                    except:
                        continue
                    # if type(main_data_dict_key['processed'])==int or type(main_data_dict_key['processed'])==float:
                    #     if key in self.ship_stats['daily_data']:
                    #         if self.ship_stats['daily_data'][key]['standard_deviation']!=0:
                    #             main_data_dict_key['z_score']=(main_data_dict_key['processed']-self.ship_stats['daily_data'][key]['mean'])/self.ship_stats['daily_data'][key]['standard_deviation']
                                
                self.base_main_data[key]=main_data_dict_key
            except KeyError:
                continue
            except AttributeError:
                continue
        for key in main_data_dict:
            if key not in self.base_main_data:
                self.base_main_data[key]=main_data_dict[key]  

        self.base_main_data_new={}
        for key in self.base_main_data: 
            # if key=="er_temp":
            processed_daily_data=self.main_data['processed_daily_data']
            
            main_data_dict_key=main_data_dict[key] 
            if pandas.isnull(main_data_dict_key['processed'])==False:
                try:
                    outlier_two=OutlierTwo(self.ship_configs,self.main_data,self.ship_imo)
                    outlier,operational,z_score=outlier_two.base_prediction_processor(key,main_data_dict_key,processed_daily_data)
                    main_data_dict_key['within_outlier_limits']=outlier  
                    main_data_dict_key['within_operational_limits']=operational
                    main_data_dict_key['z_score']=z_score
                    
                except:
                    continue
            self.base_main_data_new[key]=main_data_dict_key
        for key in self.base_main_data:
            if key not in self.base_main_data_new:
                self.base_main_data_new[key]=self.base_main_data[key]
                    
        #print(len(self.base_main_data),len(main_data_dict))        


    def update_maindb(self,index):
        for key in self.main_data['processed_daily_data']:
            try:
                #self.maindb.update_one({"ship_imo": int(self.ship_imo)},{"$set":{"processed_daily_data":self.base_main_data}})
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo)})[index],{"$set":{"processed_daily_data":self.base_main_data}})
            except TypeError:
                continue
            except KeyError:
                continue

    def update_maindb_alldoc(self):
        maindb = database.get_collection("Main_db")
        maindata = maindb.find({"ship_imo": int(self.ship_imo)})
        for i in range(0,maindata.count()):
            print(i)
            self.get_main_db(i)
            self.process_main_data()
            self.update_maindb(i)

    def process_main_data_predictions(self):
        "processing maindb data for updation ,only processed daily data for each identifier prediction value will be updated."
        self.base_main_data_prediction={}
        UIP = UpdateIndividualProcessors(configs=self.ship_configs,md=self.main_data,ss=self.ship_stats,imo=self.ship_imo)
        
        main_data_dict=self.main_data['processed_daily_data']
        ml_control=self.ship_configs['mlcontrol']
        for key in ml_control:
            # if key=="sa_pres":
            if key in main_data_dict:
                main_data_dict_key=main_data_dict[key]
                val=ml_control[key]
                for i in val:
                    if i=="" or i==" " or i=="  ":
                        val.remove(i)
                pred,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array=UIP.base_prediction_processor(key,main_data_dict_key,val,main_data_dict)
                main_data_dict[key]['predictions']=pred
                main_data_dict[key]['SPEy']=spe
                main_data_dict[key]['spe_limit_array']=spe_limit_array
                main_data_dict[key]['crit_data']=crit_data
                main_data_dict[key]['crit_val_dynamic']=crit_val_dynamic
                main_data_dict[key]['t2_initial']=t2_initial
                main_data_dict[key]['t2_final']=t2_final
                
                self.base_main_data_prediction[key]=main_data_dict[key]
           
            # if key=="main_fuel_per_dst":
            #     main_data_dict_key=main_data_dict[key]
            #     ml_control=self.ship_configs['mlcontrol']
            #     if key in ml_control.keys():
            #         val=ml_control[key]
            #         main_data_dict_key['predictions']=UIP.base_prediction_processor(key,main_data_dict_key,val,main_data_dict)
            #         print(main_data_dict_key)
        # for key in main_data_dict:  
        #     try:
        #         main_data_dict_key=main_data_dict[key]
        #         self.base_main_data_prediction[key]=eval("UIP."+key+"_processor")(main_data_dict_key,main_data_dict) # UIP.rpm_processor(base_dict)
        #     except KeyError:
        #         continue
        #     except AttributeError:
        #         continue

        
        for key in main_data_dict:
            if key not in self.base_main_data_prediction:
                self.base_main_data_prediction[key]=main_data_dict[key]

    def update_maindb_predictions_alldoc(self):
        maindb = database.get_collection("Main_db")
        maindata = maindb.find({"ship_imo": int(self.ship_imo)})
        for i in range(0,maindata.count()):
            print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom",i)
            self.get_main_db(i)
            self.process_main_data_predictions()
            self.update_maindb_preds(i)

    def update_maindb_preds(self,index):
        for key in self.main_data['processed_daily_data']:
            try:
                #self.maindb.update_one({"ship_imo": int(self.ship_imo)},{"$set":{"processed_daily_data":self.base_main_data}})
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo)})[index],{"$set":{"processed_daily_data":self.base_main_data_prediction}})
            except TypeError:
                continue
            except KeyError:
                continue
        

    #@check_status
    def process_weather_api_data(self):
        weather_api_data = 1 #= {}
        return weather_api_data

    #@check_status
    def process_position_api_data(self):
        position_api_data = 1
        return position_api_data

    #@check_status
    def process_indices(self):
        indices = 1 # Actual data here
        return indices
    
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

        document = {
            "ship_imo": self.ship_imo,
            "date": datetime.utcnow(),
            "historical":True,
            "processed_daily_data": self.processed_daily_data,
            "within_good_voyage_limit":True, #new
            "vessel_loaded_check":self.vessel_load_check(),
            "weather_api": self.process_weather_api_data(),
            "position_api": self.process_positions(),
            "indices": self.process_indices(),
            "faults": self.process_faults(),
            "health_status": self.process_health_status(),
            "Equipment":self.processed_equipment_data
        }

        

        return db.Main_db.insert_one(document).inserted_id
    
    def ad_all(self):
        daily_data_collection =database.get_collection("daily_data")
        self.all_data = daily_data_collection.find({"ship_imo": self.ship_imo})
        for i in range(self.all_data.count()):
            print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom",i)
            self.get_daily_data(i)
            self.base_dict()
            self.process_daily_data()
            self.main_db_writer()
    
    def write_ship_stats(self):
        "writing into shipstats"
            
        # print(max(m3.values()))
        # print(max(m3,key=m3.get))
    


obj=MainDB(9591301)
# import time
# start_time = time.time()
obj.get_ship_configs()
# obj.get_daily_data()
# obj.process_daily_data()
obj.get_ship_stats()
# obj.get_main_db()
# obj.process_main_data()
# obj.update_maindb()
obj.update_maindb_alldoc()
# obj.process_main_data_predictions()
# obj.example_equipment()
#obj.main_db_writer()
# obj.ad_all()

# obj.update_maindb_predictions_alldoc()
# end_time=time.time()
# print(end_time-start_time)

# obj=MainDB(9591302)

# obj.get_ship_configs()
# # obj.get_daily_data()
# # obj.process_daily_data()
# # obj.get_ship_stats()
# # obj.get_main_db()
# # obj.process_main_data()
# # obj.update_maindb()
# # obj.update_maindb_alldoc()
# # obj.process_main_data_predictions()
# # obj.main_db_writer()
# obj.ad_all()



