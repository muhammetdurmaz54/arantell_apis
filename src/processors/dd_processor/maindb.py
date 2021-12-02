from logging import NOTSET
import sys
from numpy.core.defchararray import index
from numpy.lib.function_base import median
from numpy.lib.npyio import load
from numpy.lib.shape_base import dsplit

import pandas
from pandas.core.algorithms import factorize
from pandas.tseries.offsets import Second 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.processors.dd_processor.individual_processors import IndividualProcessors
from src.processors.dd_processor.outlier_two import OutlierTwo
from src.processors.dd_processor.update_individual_processor import UpdateIndividualProcessors
from src.processors.dd_processor.update_individual_predictions import UpdateIndividualProcessorspredictions
from src.processors.dd_processor.indices_procesor import Indice_Processing
from src.configurations.logging_config import CommonLogger
from datetime import date, datetime
from src.db.schema.main import Main_db
from src.db.schema.ship import Ship 
from src.db.schema.ddschema import DailyData
import numpy as np
from pymongo import MongoClient
import random
from src.processors.config_extractor.outlier import CheckOutlier
from pymongo import ASCENDING
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
log = CommonLogger(__name__,debug=True).setup_logger()
client = MongoClient("mongodb+srv://iamuser:iamuser@democluster.lw5i0.mongodb.net/test?ssl=true&ssl_cert_reqs=CERT_NONE")

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
        # self.daily_data = daily_data_collection.find({"ship_imo": self.ship_imo})[index]
        # self.daily_data = daily_data_collection.find({"ship_imo": self.ship_imo})[0]
        self.daily_data = daily_data_collection.find({"ship_imo": self.ship_imo,"data.rep_dt":{"$lte":datetime(2015,12,1,12)}})[index]
       

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
                    self.processed_daily_data[key] = IP.base_individual_processor(key,base_dict)
                except AttributeError:
                    self.processed_daily_data[key] = IP.base_individual_processor(key,base_dict)
            

    def get_main_db(self,identifier):
        self.maindb = database.get_collection("Main_db")
        # self.maindb.delete_many(self.maindb.find({"ship_imo":self.ship_imo}),{"processed_daily_data.main_fuel_index"})
        # self.maindb.delete_many({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2016,2,1,12)}})
        # self.maindb.update_many({}, {'$unset': {"processed_daily_data.main_fuel_index":1}})
        # print("done")
        # exit()
        self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo)})
        # self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True,"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,7,1,12),"$gte":datetime(2015,5,1,12)}})[index]
        # self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
        # print(self.main_data['processed_daily_data']['main_fuel'])
        # # print(self.main_data['within_good_voyage_limit'])
        # exit()
    
        # self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":datetime(2015,6,21,12)})[0]
        # print(self.main_data['processed_daily_data']['comp_pres2'])
        # print(self.main_data['processed_daily_data']['peak_pres1'])
        # exit()
        pred=[]
        observed=[]
        dat=[]
        for doc in self.maindb.find({"ship_imo": int(self.ship_imo)}):
            for key in doc["processed_daily_data"].keys():
                try:
                    if key==identifier:
                        if pandas.isnull(doc["processed_daily_data"][key]['predictions']["m3"][1])==False:
                            pred.append(doc["processed_daily_data"][key]['predictions']["m3"][1])
                            observed.append(doc["processed_daily_data"][key]['processed'])
                            dat.append(doc["processed_daily_data"]["rep_dt"]['processed'])
                except:
                    continue
        data=pandas.DataFrame({"rep_dt":dat,identifier:observed,"expected":pred})
        print(data)
        exit()




        # self.main_data = self.maindb.find({"ship_imo": self.ship_imo}, {'processed_daily_data.rep_dt.processed': 1}).sort('processed_daily_data.rep_dt.processed',ASCENDING)[0]
        # first_date=self.main_data['processed_daily_data']['rep_dt']['processed']
        # pred_start_date=first_date+relativedelta(months=2)
        # main_datanew=self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":pred_start_date}})
        # print(pred_start_date)
        # date_list=[]
        # for i in range(0,main_datanew.count()):
        #     dum=main_datanew[i]['processed_daily_data']['rep_dt']['processed'].date()
        #     date_list.append(str(dum))
        # print(date_list)

        # exit()
        
        # self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo),'processed_daily_data.avg_hfo.predictions.m3':{"$lte":2000,"$gte":100}})
        # self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo)})
        # self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2016,10,15,12),"$gte":datetime(2016,8,15,12)}})[index]
        
        


        # print(self.main_data['data_available_engine'])
        # exit()
        
        # self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True,"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
        # dft_aft=[]
        # dft_fwd=[]
        # val=[]
        # dat=[]
        # pred=[]
        # spe_y=[]
        # lypred=[]
        # dict_temp={}
        # dataframe_size=[]
        # print(self.main_data.count())
        # for i in range(0,self.main_data.count()):
        #     # print(self.main_data[i]['processed_daily_data'][identifier]['predictions'])
        #     try:
        #         print("tun")

        #         if pandas.isnull(self.main_data[i]['processed_daily_data'][identifier]['Q_y']["m3"][1])==False and self.main_data[i]['processed_daily_data'][identifier]['Q_y']["m3"][1]!=None:
        #         # if pandas.isnull(self.main_data[i]['processed_daily_data'][identifier]['predictions']['m3'][1])==False and self.main_data[i]['processed_daily_data'][identifier]['predictions']['m3'][1]!=None:
        #         # if pandas.isnull(self.main_data[i]['independent_indices'][identifier]['ucl_crit_beta']['m3'])==False and self.main_data[i]['independent_indices'][identifier]['ucl_crit_beta']['m3']!=None:
        #             # if self.main_data[i]['processed_daily_data'][identifier]['spe_anamoly'] and self.main_data[i]['processed_daily_data'][identifier]['spe_anamoly']['m3'] and self.main_data[i]['processed_daily_data'][identifier]['spe_anamoly']['m3'][0]==False:
        #                 # dft_fwd.append(self.main_data[i]['processed_daily_data'][identifier]['predictions']['m3'][0])
        #             print("ijj")
        #             dft_fwd.append(self.main_data[i]['processed_daily_data'][identifier]['Q_y']['m3'][1])
        #             dft_aft.append(self.main_data[i]['processed_daily_data'][identifier]['SPEy']['m3'])
        #             val.append(self.main_data[i]['processed_daily_data'][identifier]['length_dataframe']['m3'])
        #             # pred.append(self.main_data[i]['processed_daily_data'][identifier]['predictions']['m3'][2])
        #             # spe_y.append(self.main_data[i]['processed_daily_data'][identifier]['Q_y']['m3'][2])
        #             # dataframe_size.append(self.main_data[i]['independent_indices'][identifier]['length_dataframe']['m3'])
        #             # val.append(self.main_data[i]['processed_daily_data'][identifier]['processed'])
        #             # pred.append(self.main_data[i]['processed_daily_data'][identifier]['predictions']['m3'][1])
        #             # lypred.append(self.main_data[i]['processed_daily_data'][identifier]['predictions']['ly_m3'][1])
        #             # dft_aft.append(self.main_data[i]['processed_daily_data'][identifier]['predictions']['m3'][2])
        #             # val.append(self.main_data[i]['processed_daily_data'][identifier]['processed'])
                    
        #             dat.append(self.main_data[i]['processed_daily_data']['rep_dt']['processed'].date())
        #     except:
        #         continue
        # dataframe=pandas.DataFrame({'date': dat,
        # 'length': val,
        # # 'upper': pred,
        # 'spe':dft_aft,
        # 'limit':dft_fwd,
        # # 'dataframe_size':dataframe_size
        # # 'spe_y':spe_y
        # # 'ly_pred':lypred
        # })
        
        
        # dataframe=dataframe.sort_values('date')
        # dataframe=dataframe.reset_index(drop=True)
        # # dataframe.to_csv("pwr_check_upperlower.csv")
        # print(dataframe)
        # # dataframe.to_csv("spe_check4.csv")
        # # print(dataframe)
        # # dataframe.to_csv("wrong_data.csv")
        # # plt.plot(dataframe['date'],dataframe['pred'],color='orange')
        # plt.plot(dataframe['date'],dataframe['limit'],color='black')
        # plt.plot(dataframe['date'],dataframe['spe'],color='blue')
        # plt.plot(dataframe['date'],dataframe['length'],color='green')
        # # plt.plot(dataframe['date'],dataframe['spe_y'],color='green')
        # # plt.plot(dataframe['date'],dataframe['val'],color='orange')
        # # plt.plot(dataframe['date'],dataframe['ly_pred'],color='green')
        
        # plt.xlabel('date')
        # plt.ylabel(identifier)
        # plt.show()
        # exit()
        

        # self.main_data = self.maindb.find({"ship_imo": int(self.ship_imo)})[0]
        
     
        
        
        

    def process_main_data(self,index):
        "processing maindb data for updation ,only processed daily data will be updated."
        self.base_main_data={}
        check_outlier=CheckOutlier(configs=self.ship_configs,main_db=self.main_data,ship_imo=self.ship_imo)
        outlier_two=OutlierTwo(self.ship_configs,self.main_data,self.ship_imo)
        #UIP = UpdateIndividualProcessors(configs=self.ship_configs,md=self.main_data,ss=self.ship_stats)
        main_data_dict=self.main_data['processed_daily_data']
        self.rep_dt=main_data_dict['rep_dt']['processed']

        for key in main_data_dict:
            # if key=='ext_tempavg':
            try:
                main_data_dict_key=main_data_dict[key] 
                # print(main_data_dict_key['processed'])
                if pandas.isnull(main_data_dict_key['processed'])==False:
                    try:
                        oplow=self.ship_configs['data'][key]['limits']['oplow'] 
                        ophigh=self.ship_configs['data'][key]['limits']['ophigh']                                    #rpm with % 
                        olmin=self.ship_configs['data'][key]['limits']['olmin']
                        olmax=self.ship_configs['data'][key]['limits']['olmax']
                        limit_val=self.ship_configs['data'][key]['limits']['type']
                        outlier_limit_value={}
                        operational_limit_value={}
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
                        
                        
                        within_outlier_limit['m3']=check_outlier.final_outlier(within_outlier_limit_min['m3'][0],within_outlier_limit_max['m3'][0])
                        within_outlier_limit['m6']=check_outlier.final_outlier(within_outlier_limit_min['m6'][0],within_outlier_limit_max['m6'][0])
                        within_outlier_limit['m12']=check_outlier.final_outlier(within_outlier_limit_min['m12'][0],within_outlier_limit_max['m12'][0])
                        within_outlier_limit['ly_m3']=check_outlier.final_outlier(within_outlier_limit_min['ly_m3'][0],within_outlier_limit_max['ly_m3'][0])
                        within_outlier_limit['ly_m6']=check_outlier.final_outlier(within_outlier_limit_min['ly_m6'][0],within_outlier_limit_max['ly_m6'][0])
                        within_outlier_limit['ly_m12']=check_outlier.final_outlier(within_outlier_limit_min['ly_m12'][0],within_outlier_limit_max['ly_m12'][0])
                        
                        
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
                        
                        within_operational_limit['m3']=check_outlier.final_outlier(within_operational_limit_min['m3'][0],within_operational_limit_max['m3'][0])
                        within_operational_limit['m6']=check_outlier.final_outlier(within_operational_limit_min['m6'][0],within_operational_limit_max['m6'][0])
                        within_operational_limit['m12']=check_outlier.final_outlier(within_operational_limit_min['m12'][0],within_operational_limit_max['m12'][0])
                        within_operational_limit['ly_m3']=check_outlier.final_outlier(within_operational_limit_min['ly_m3'][0],within_operational_limit_max['ly_m3'][0])
                        within_operational_limit['ly_m6']=check_outlier.final_outlier(within_operational_limit_min['ly_m6'][0],within_operational_limit_max['ly_m6'][0])
                        within_operational_limit['ly_m12']=check_outlier.final_outlier(within_operational_limit_min['ly_m12'][0],within_operational_limit_max['ly_m12'][0])
                        

                        outliermin_max['min']=within_outlier_limit_min['m3'][1]
                        outliermin_max['max']=within_outlier_limit_max['m3'][1]
                        operationalmin_max['min']=within_operational_limit_min['m3'][1]
                        operationalmin_max['max']=within_operational_limit_max['m3'][1]
                        main_data_dict_key['within_outlier_limits']=within_outlier_limit
                        main_data_dict_key['within_operational_limits']=within_operational_limit
                        main_data_dict_key['z_score']=z_score
                        main_data_dict_key['outlier_limit_value']=outliermin_max
                        main_data_dict_key['operational_limit_value']=operationalmin_max

                        self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,5,1,12)}})[index],{"$set":{"processed_daily_data."+key:main_data_dict_key}})
                        print("kiiiiiiiiiiiiiiiiiiiiii")
                       
                        
                    except:
                        continue
                        
                

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
        # maindata = maindb.find({"ship_imo": int(self.ship_imo)})
        # maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,7,1,12),"$gte":datetime(2015,5,1,12)}})
        maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,5,1,12)}})
        print(maindata.count())
        for i in range(0,maindata.count()):
        # for i in range(0,34):
            print(i)
            self.get_main_db(i)
            self.process_main_data(i)
            # self.update_maindb(i)
            # print("done")

    def process_main_data_predictions(self,index):
        "processing maindb data for updation ,only processed daily data for each identifier prediction value will be updated."
        self.base_main_data_prediction={}
        UIP = UpdateIndividualProcessors(configs=self.ship_configs,md=self.main_data,ss=self.ship_stats,imo=self.ship_imo)
        
        main_data_dict=self.main_data['processed_daily_data']
        ml_control=self.ship_configs['mlcontrol']
        # print(main_data_dict['rep_dt']['processed'])
        # variable_list=['ext_temp1','ext_temp2','ext_temp3','ext_temp4','ext_temp5','ext_temp6','ext_temp7','ext_temp8','ext_temp9','ext_temp10','ext_temp11','ext_temp12','ext_tempavg','tc1_extin_temp','tc1_extout_temp']
        for key in ml_control:
            # if key=="pwr":
                
            if key in main_data_dict and pandas.isnull(main_data_dict[key]['processed'])==False:
                print(key)
                main_data_dict_key=main_data_dict[key]
                val=ml_control[key]
                for i in val:
                    if i=="" or i==" " or i=="  ":
                        val.remove(i)
                val.append("vsl_load_bal")
                
                main_data_dict[key]['predictions']=None
                main_data_dict[key]['SPEy']=None
                main_data_dict[key]['Q_x']=None
                main_data_dict[key]['ucl_crit_beta']=None
                main_data_dict[key]['ucl_crit_fcrit']=None
                main_data_dict[key]['t2_initial']=None
                main_data_dict[key]['t_2_daily']=None
                main_data_dict[key]['spe_anamoly']=None
                main_data_dict[key]['Q_y']=None
                main_data_dict[key]['t2_anamoly']=None
                # print(main_data_dict[key]['processed'])
                pred,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array,spe_anamoly,spe_y_limit_array,t2_anamoly,length_dataframe=UIP.base_prediction_processor(key,main_data_dict_key,val,main_data_dict)
                main_data_dict[key]['predictions']=pred
                main_data_dict[key]['SPEy']=spe
                main_data_dict[key]['Q_x']=spe_limit_array
                main_data_dict[key]['ucl_crit_beta']=crit_data
                main_data_dict[key]['ucl_crit_fcrit']=crit_val_dynamic
                main_data_dict[key]['t2_initial']=t2_initial
                main_data_dict[key]['t_2_daily']=t2_final
                main_data_dict[key]['spe_anamoly']=spe_anamoly
                main_data_dict[key]['Q_y']=spe_y_limit_array
                main_data_dict[key]['t2_anamoly']=t2_anamoly
                main_data_dict[key]['length_dataframe']=length_dataframe
                print(length_dataframe)
                # print(main_data_dict[key]['predictions'])
                # self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True})[index],{"$set":{"processed_daily_data."+key:main_data_dict_key}})
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[index],{"$set":{"processed_daily_data."+key:main_data_dict[key]}})
                print("kiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")

                

    def update_maindb_predictions_alldoc(self):
        maindb = database.get_collection("Main_db")
        # maindata = maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True})
        maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
        print(maindata.count())
        for i in range(205,maindata.count()): 
            print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom",i)
            self.get_main_db(i)
            self.process_main_data_predictions(i)
            

    def process_main_data_predictions_second(self,index):
        "processing maindb data for updation ,only processed daily data for each identifier prediction value will be updated."
        self.base_main_data_prediction={}
        UIP = UpdateIndividualProcessorspredictions(configs=self.ship_configs,md=self.main_data,ss=self.ship_stats,imo=self.ship_imo)
        
        main_data_dict=self.main_data['processed_daily_data']
        ml_control=self.ship_configs['mlcontrol']
        
        for key in ml_control:
            # if key=="ext_temp1":
            if key in main_data_dict and pandas.isnull(main_data_dict[key]['processed'])==False:
                print(key)
                main_data_dict_key=main_data_dict[key]
                val=ml_control[key]
                for i in val:
                    if i=="" or i==" " or i=="  ":
                        val.remove(i)
                val.append("vsl_load_bal")
            
                pred,spe,crit_data,crit_val_dynamic,t2_initial,t2_final,spe_limit_array,spe_anamoly=UIP.base_prediction_processor(key,main_data_dict_key,val,main_data_dict)
                main_data_dict[key]['predictions']=pred
                main_data_dict[key]['SPEy']=spe
                main_data_dict[key]['spe_limit_array']=spe_limit_array
                main_data_dict[key]['ucl_crit_beta']=crit_data
                main_data_dict[key]['ucl_crit_fcrit']=crit_val_dynamic
                main_data_dict[key]['t2_initial']=t2_initial
                main_data_dict[key]['t_2_daily']=t2_final
                main_data_dict[key]['spe_anamoly']=spe_anamoly

                # self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True,"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,7,1,12),"$gte":datetime(2015,5,1,12)}})[index],{"$set":{"processed_daily_data."+key:main_data_dict_key}})
                print("kiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
            
    def update_maindb_predictions_alldoc_second(self):
        maindb = database.get_collection("Main_db")
        # maindata = maindb.find({"ship_imo": int(self.ship_imo)})
        maindata = maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True,"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,7,1,12),"$gte":datetime(2015,5,1,12)}})
        print(maindata.count())
        for i in range(23,maindata.count()):
            print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom",i)
            self.get_main_db(i)
            self.process_main_data_predictions_second(i)
            # self.update_maindb_preds(i)

    def update_maindb_preds(self,index):
        # for key in self.main_data['processed_daily_data']:
        
            #self.maindb.update_one({"ship_imo": int(self.ship_imo)},{"$set":{"processed_daily_data":self.base_main_data}})
        self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo)})[index],{"$set":{"processed_daily_data":self.base_main_data_prediction}})
        print("kiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")



    def processing_indices(self,index):
        ship_indices_data=self.ship_configs['indices_data']
        main_processed_daily_data=self.main_data['processed_daily_data']
        indice=Indice_Processing(self.ship_configs,self.main_data,self.ship_stats,self.ship_imo)
        for i in ship_indices_data:
            if i == "main_fuel_index":
                indice_dict={}
                main_data=self.main_data
                main_data_dict=main_data['processed_daily_data']
                # main_data_dict_key=main_data_dict[i]
                input=ship_indices_data[i]['input']
                output=ship_indices_data[i]['output']
                input.append("vsl_load_bal")
                indice_dict['index_id']=ship_indices_data[i]['name']
                indice_dict['IndexName']=ship_indices_data[i]['short_names']
                indice_dict['ParamGroup']=None
                indice_dict['SPE']=None
                indice_dict['Q_y']=None
                indice_dict['ucl_crit_beta']=None
                indice_dict['ucl_crit_fcrit']=None
                indice_dict['t2_initial']=None
                indice_dict['t_2_daily']=None
                indice_dict['length_dataframe']=None
                indice_dict['spe_anamoly']=None
                val,spe_chi_square,crit_data,crit_val_dynamic,t2_initial,t2_final,length_dataframe,spe_anamoly,t2_anamoly=indice.base_prediction_processor(output,input,main_data_dict)
                indice_dict['SPEy']=val
                indice_dict['Q_y']=spe_chi_square
                # print(indice_dict['SPE'],indice_dict['Q_y'])
                indice_dict['ucl_crit_beta']=crit_data
                indice_dict['ucl_crit_fcrit']=crit_val_dynamic
                indice_dict['t2_initial']=t2_initial
                indice_dict['t_2_daily']=t2_final
                indice_dict['length_dataframe']=length_dataframe
                indice_dict['spe_anamoly']=spe_anamoly
                indice_dict['t2_anamoly']=t2_anamoly
                # self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True,"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,7,1,12),"$gte":datetime(2015,5,1,12)}})[index],{"$set":{"independent_indices."+i:indice_dict}})
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[index],{"$set":{"independent_indices."+i:indice_dict}})
                print("kiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii")
                
    def update_indices(self):
        maindb = database.get_collection("Main_db")
        # maindata = maindb.find({"ship_imo": int(self.ship_imo)})
        # maindata = maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True,"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,7,1,12),"$gte":datetime(2015,5,1,12)}})
        maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
        print(maindata.count())
        for i in range(111,maindata.count()):
            print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom",i)
            self.get_main_db(i)
            self.processing_indices(i)

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
        maindb = database.get_collection("Main_db")
        maindata = maindb.find({"ship_imo": int(self.ship_imo)})
        # maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,7,1,12),"$gte":datetime(2015,5,1,12)}})
        # maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,5,1,12)}})
        print(maindata.count())
        for i in range(0,maindata.count()): 
            print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom",i)
            self.get_main_db(i)
            self.good_voyage(i)

    def good_voyage(self,index):
        try:
            speed=self.main_data['processed_daily_data']['speed_sog']['processed']
            rpm=self.main_data['processed_daily_data']['rpm']['processed']
            w_force=self.main_data['processed_daily_data']['w_force']['processed']
            sea_state=self.main_data['processed_daily_data']['sea_st']['processed']
            stm_hrs=self.main_data['processed_daily_data']['stm_hrs']['processed']
            draft=self.main_data['processed_daily_data']['draft_mean']['within_operational_limits']['m3']
            rpm_op=self.main_data['processed_daily_data']['rpm']['within_operational_limits']['m3']
            if speed>6 and rpm_op==True and w_force<5 and sea_state<5 and stm_hrs>10 and draft==True:
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo)})[index],{"$set":{"within_good_voyage_limit":True}})
                # self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,5,1,12)}})[index],{"$set":{"within_good_voyage_limit":True}})
        except:
            self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo)})[index],{"$set":{"within_good_voyage_limit":True}})
            # self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$lte":datetime(2015,5,1,12)}})[index],{"$set":{"within_good_voyage_limit":False}})

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
            "processed_daily_data": self.processed_daily_data,
            "within_good_voyage_limit":True, #new
            "vessel_loaded_check":self.vessel_load_check(),
            "data_available_engine":self.data_available_check(eng),
            "data_available_nav":self.data_available_check(nav),
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
        
        self.all_data = daily_data_collection.find({"ship_imo": self.ship_imo,"data.rep_dt":{"$lte":datetime(2015,12,1,12)}})
        
        print(self.all_data.count())
        for i in range(0,self.all_data.count()):
            print("boooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooom",i)
            self.get_daily_data(i)
            self.base_dict()
            self.process_daily_data()
            self.main_db_writer()
    
        
    def update_main_fuel(self):
        maindb = database.get_collection("Main_db")
        maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
        for i in range(0,maindata.count()):
            try:
                print(i)
                self.get_main_db(i)
                main_fuel_per_dst_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['m3']
                main_fuel_per_dst_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['m6']
                main_fuel_per_dst_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['m12']
                main_fuel_per_dst_ly_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['ly_m3']
                main_fuel_per_dst_ly_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['ly_m6']
                main_fuel_per_dst_ly_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['predictions']['ly_m12']

                spe_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['m3']
                spe_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['m6']
                spe_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['m12']
                spe_ly_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['ly_m3']
                spe_ly_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['ly_m6']
                spe_ly_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['SPEy']['ly_m12']

                Q_y_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['Q_y']['m3']
                Q_y_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['Q_y']['m6']
                Q_y_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['Q_y']['m12']
                Q_y_ly_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['Q_y']['ly_m3']
                Q_y_ly_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['Q_y']['ly_m6']
                Q_y_ly_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['Q_y']['ly_m12']


                T_2_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['m3']
                T_2_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['m6']
                T_2_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['m12']
                T_2_ly_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['ly_m3']
                T_2_ly_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['ly_m6']
                T_2_ly_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['t2_initial']['ly_m12']

                T_2_limit_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['ucl_crit_beta']['m3']
                T_2_limit_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['ucl_crit_beta']['m6']
                T_2_limit_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['ucl_crit_beta']['m12']
                T_2_limit_ly_m3=self.main_data['processed_daily_data']['main_fuel_per_dst']['ucl_crit_beta']['ly_m3']
                T_2_limit_ly_m6=self.main_data['processed_daily_data']['main_fuel_per_dst']['ucl_crit_beta']['ly_m6']
                T_2_limit_ly_m12=self.main_data['processed_daily_data']['main_fuel_per_dst']['ucl_crit_beta']['ly_m12']


                # stm_hrs=self.main_data['processed_daily_data']['stm_hrs']['processed']
                dst_last=self.main_data['processed_daily_data']['dst_last']['processed']
                pred={}
                spe_y={}
                Q_y={}
                spe_anamoly={}
                t_2={}
                t_2_ucl={}
                t_2_anamoly={}
                months_list={"m3":main_fuel_per_dst_m3,"m6":main_fuel_per_dst_m6,"m12":main_fuel_per_dst_m12,"ly_m3":main_fuel_per_dst_ly_m3,"ly_m6":main_fuel_per_dst_ly_m6,"ly_m12":main_fuel_per_dst_ly_m12}
                spe_list={"m3":spe_m3,"m6":spe_m6,"m12":spe_m12,"ly_m3":spe_ly_m3,"ly_m6":spe_ly_m6,"ly_m12":spe_ly_m12}
                Q_y_list={"m3":Q_y_m3,"m6":Q_y_m6,"m12":Q_y_m12,"ly_m3":Q_y_ly_m3,"ly_m6":Q_y_ly_m6,"ly_m12":Q_y_ly_m12}
                T_2_list={"m3":T_2_m3,"m6":T_2_m6,"m12":T_2_m12,"ly_m3":T_2_ly_m3,"ly_m6":T_2_ly_m6,"ly_m12":T_2_ly_m12}
                T_2_limit={"m3":T_2_limit_m3,"m6":T_2_limit_m6,"m12":T_2_limit_m12,"ly_m3":T_2_limit_ly_m3,"ly_m6":T_2_limit_ly_m6,"ly_m12":T_2_limit_ly_m12}

                
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

                    if T_2_limit[month] and pandas.isnull(T_2_limit[month])==False and pandas.isnull(dst_last)==False:
                        t_2_ucl[month]=T_2_limit[month] * dst_last
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

                    if Q_y_list[month] and pandas.isnull(Q_y_list[month][1])==False and pandas.isnull(dst_last)==False:
                        temp_list1=[]
                        temp_list2=[]
                        if pandas.isnull(Q_y_list[month][0])==False or Q_y_list[month][0]!=None:
                            temp_list1.append(Q_y_list[month][0] * dst_last)
                            if pandas.isnull(Q_y_list[month][0] * dst_last)==False and pandas.isnull(main_fuel['SPEy'][month])==False:
                                if (Q_y_list[month][0] * dst_last)<main_fuel['SPEy'][month]:
                                    temp_list2.append(False)
                                else:
                                    temp_list2.append(True)
                            else:
                                temp_list2.append(True)
                        else:
                            temp_list1.append(None)
                            temp_list2.append(None)

                        temp_list1.append(Q_y_list[month][1] *  dst_last)
                        if pandas.isnull(Q_y_list[month][1] *  dst_last)==False and pandas.isnull(main_fuel['SPEy'][month])==False:
                            if (Q_y_list[month][1] *  dst_last)<main_fuel['SPEy'][month]:
                                temp_list2.append(False)
                            else:
                                temp_list2.append(True)
                        else:
                            temp_list2.append(True)

                        if pandas.isnull(Q_y_list[month][2])==False or Q_y_list[month][2]!=None:
                            temp_list1.append(Q_y_list[month][2] * dst_last)
                            if pandas.isnull(Q_y_list[month][2] * dst_last)==False and pandas.isnull(main_fuel['SPEy'][month])==False:
                                if (Q_y_list[month][2] * dst_last)<main_fuel['SPEy'][month]:
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
                    main_fuel['spe_anamoly']=spe_anamoly
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[i],{"$set":{"processed_daily_data.main_fuel":main_fuel}})
                print("done")
            except:
                continue


    def update_sfoc(self):
        maindb = database.get_collection("Main_db")
        maindata = maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
        for i in range(0,maindata.count()):
            try:
                print(i)
                self.get_main_db(i)
                sfoc=self.main_data['processed_daily_data']['sfoc']
                main_fuel=self.main_data['processed_daily_data']['main_fuel']
                pwr=self.main_data['processed_daily_data']['pwr']
                stm_hrs=self.main_data['processed_daily_data']['stm_hrs']['processed']
                pred={}
                spe_y={}
                Q_y={}
                spe_anamoly={}
                t_2={}
                t_2_ucl={}
                t_2_anamoly={}
                months_list=["m3","m6","m12","ly_m3","ly_m6","ly_m12"]
                
                
                for month in months_list:
                    if main_fuel['predictions'][month] and pandas.isnull(main_fuel['predictions'][month][1])==False and pwr['predictions'][month] and pandas.isnull(pwr['predictions'][month][1])==False and pandas.isnull(stm_hrs)==False:
                        temp_list=[]
                        if pandas.isnull(main_fuel['predictions'][month][0])==False or main_fuel['predictions'][month][0]!=None :
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

                    if main_fuel['ucl_crit_beta'][month] and pandas.isnull(main_fuel['ucl_crit_beta'][month])==False and pwr['ucl_crit_beta'][month] and pandas.isnull(pwr['ucl_crit_beta'][month])==False and pandas.isnull(stm_hrs)==False:
                        t_2_ucl[month]=((main_fuel['ucl_crit_beta'][month]/pwr['ucl_crit_beta'][month])/stm_hrs)*1000
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
                            temp_list1.append(((main_fuel['Q_y'][month][0]/pwr['Q_y'][month][0])/stm_hrs)*1000)
                            if pandas.isnull(((main_fuel['Q_y'][month][0]/pwr['Q_y'][month][0])/stm_hrs)*1000)==False and pandas.isnull(sfoc['SPEy'][month])==False:
                                if (((main_fuel['Q_y'][month][0]/pwr['Q_y'][month][0])/stm_hrs)*1000)<sfoc['SPEy'][month]:
                                    temp_list2.append(False)
                                else:
                                    temp_list2.append(True)
                            else:
                                temp_list2.append(True)
                        else:
                            temp_list1.append(None)
                            temp_list2.append(None)

                        temp_list1.append(((main_fuel['Q_y'][month][1]/pwr['Q_y'][month][1])/stm_hrs)*1000)
                        if pandas.isnull(((main_fuel['Q_y'][month][1]/pwr['Q_y'][month][1])/stm_hrs)*1000)==False and pandas.isnull(sfoc['SPEy'][month])==False:
                            if (((main_fuel['Q_y'][month][1]/pwr['Q_y'][month][1])/stm_hrs)*1000)<sfoc['SPEy'][month]:
                                temp_list2.append(False)
                            else:
                                temp_list2.append(True)
                        else:
                            temp_list2.append(True)

                        if pandas.isnull(main_fuel['Q_y'][month][2])==False or main_fuel['Q_y'][month][2]!=None and pandas.isnull(pwr['Q_y'][month][2])==False or pwr['Q_y'][month][2]!=None:
                            temp_list1.append(((main_fuel['Q_y'][month][2]/pwr['Q_y'][month][2])/stm_hrs)*1000)
                            if pandas.isnull(((main_fuel['Q_y'][month][2]/pwr['Q_y'][month][2])/stm_hrs)*1000)==False and pandas.isnull(sfoc['SPEy'][month])==False:
                                if (((main_fuel['Q_y'][month][2]/pwr['Q_y'][month][2])/stm_hrs)*1000)<sfoc['SPEy'][month]:
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
                    sfoc['spe_anamoly']=spe_anamoly
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[i],{"$set":{"processed_daily_data.sfoc":sfoc}})
                print("done")
            except:
                continue


    def update_avg_hfo(self):
        maindb = database.get_collection("Main_db")
        maindata = maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True,"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})
        for i in range(0,maindata.count()):
            try:
                print(i)
                self.get_main_db(i)
                main_fuel_per_dst_m3=self.main_data['processed_daily_data']['main_fuel']['predictions']['m3']
                main_fuel_per_dst_m6=self.main_data['processed_daily_data']['main_fuel']['predictions']['m6']
                main_fuel_per_dst_m12=self.main_data['processed_daily_data']['main_fuel']['predictions']['m12']
                main_fuel_per_dst_ly_m3=self.main_data['processed_daily_data']['main_fuel']['predictions']['ly_m3']
                main_fuel_per_dst_ly_m6=self.main_data['processed_daily_data']['main_fuel']['predictions']['ly_m6']
                main_fuel_per_dst_ly_m12=self.main_data['processed_daily_data']['main_fuel']['predictions']['ly_m12']

                spe_m3=self.main_data['processed_daily_data']['main_fuel']['SPEy']['m3']
                spe_m6=self.main_data['processed_daily_data']['main_fuel']['SPEy']['m6']
                spe_m12=self.main_data['processed_daily_data']['main_fuel']['SPEy']['m12']
                spe_ly_m3=self.main_data['processed_daily_data']['main_fuel']['SPEy']['ly_m3']
                spe_ly_m6=self.main_data['processed_daily_data']['main_fuel']['SPEy']['ly_m6']
                spe_ly_m12=self.main_data['processed_daily_data']['main_fuel']['SPEy']['ly_m12']

                Q_y_m3=self.main_data['processed_daily_data']['main_fuel']['Q_y']['m3']
                Q_y_m6=self.main_data['processed_daily_data']['main_fuel']['Q_y']['m6']
                Q_y_m12=self.main_data['processed_daily_data']['main_fuel']['Q_y']['m12']
                Q_y_ly_m3=self.main_data['processed_daily_data']['main_fuel']['Q_y']['ly_m3']
                Q_y_ly_m6=self.main_data['processed_daily_data']['main_fuel']['Q_y']['ly_m6']
                Q_y_ly_m12=self.main_data['processed_daily_data']['main_fuel']['Q_y']['ly_m12']

                stm_hrs=self.main_data['processed_daily_data']['stm_hrs']['processed']
                pred={}
                spe_y={}
                Q_y={}

                months_list={"m3":main_fuel_per_dst_m3,"m6":main_fuel_per_dst_m6,"m12":main_fuel_per_dst_m12,"ly_m3":main_fuel_per_dst_ly_m3,"ly_m6":main_fuel_per_dst_ly_m6,"ly_m12":main_fuel_per_dst_ly_m12}
                spe_list={"m3":spe_m3,"m6":spe_m6,"m12":spe_m12,"ly_m3":spe_ly_m3,"ly_m6":spe_ly_m6,"ly_m12":spe_ly_m12}
                Q_y_list={"m3":Q_y_m3,"m6":Q_y_m6,"m12":Q_y_m12,"ly_m3":Q_y_ly_m3,"ly_m6":Q_y_ly_m6,"ly_m12":Q_y_ly_m12}
                
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

                    if Q_y_list[month] and pandas.isnull(Q_y_list[month][1])==False and pandas.isnull(stm_hrs)==False:
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

                    avg_hfo['Q_y']=Q_y
                    
                self.maindb.update_one(self.maindb.find({"ship_imo": int(self.ship_imo),"within_good_voyage_limit":True,"processed_daily_data.rep_dt.processed":{"$gte":datetime(2016,2,1,12)}})[i],{"$set":{"processed_daily_data.avg_hfo":avg_hfo}})
                print("done")    
              
                
            except:
                continue
    
            
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


obj=MainDB(9591301)
import time
start_time = time.time()
obj.get_ship_configs()
# obj.get_daily_data()
# obj.process_daily_data()
obj.get_ship_stats()
obj.get_main_db("pwr")
# obj.process_main_data()
# obj.update_maindb()
# obj.update_maindb_alldoc()
# obj.process_main_data_predictions(1)
# obj.example_equipment()
#obj.main_db_writer()
# obj.ad_all()
# obj.processing_indices()
obj.update_indices()
# obj.update_main_fuel()
# obj.update_sfoc()
# obj.update_sfoc_processed()
# obj.update_avg_hfo()
# obj.update_maindb_predictions_alldoc()
# obj.update_maindb_predictions_alldoc_second()
# obj.good_voyage()
# obj.update_good_voyage()
end_time=time.time()
print(end_time-start_time)

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



