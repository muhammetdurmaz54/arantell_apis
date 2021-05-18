import sys 
sys.path.insert(1,"arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.processors.dd_processor.individual_processors import IndividualProcessors
from src.configurations.logging_config import CommonLogger
from datetime import datetime
from src.db.schema.main import Main_db
from src.db.schema.ship import Ship 
from src.db.schema.ddschema import DailyData
import numpy as np
from pymongo import MongoClient




log = CommonLogger(__name__,debug=True).setup_logger()


client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
    


database = db
ship_configs_collection = database.get_collection("ship")

ship_configs = ship_configs_collection.find({"ship_imo": 9591301})




daily_data_collection = database.get_collection("daily_data")
daily_data = daily_data_collection.find({"ship_imo": 9591301,"ship_name":"RTM COOK"})



maindb = database.get_collection("Main_db")

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


class Processor():
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
                  name=None,
                  reported=None,
                  processed=None,
                  isoutlier=None,
                  results=None,
                  z_score=None,
                  unit=None,
                  statement=None,
                  predictions=None,
                  ):
        
        return {"identifier": identifier,
                "name": name,
                "reported": reported,
                "processed": processed,
                "isoutlier": isoutlier,
                "results": results,
                "z_score": z_score,
                "unit": unit,
                "statement": statement,
                "predictions": predictions}

    @check_status
    def connect_db(self):
        self.database = connect_db()

    #@check_status
    def get_ship_configs(self):
        """ship_configs_collection = self.database.ship_configs
        self.ship_configs = ship_configs_collection.find({"ship_imo": int(self.ship_imo)})[0]"""
        ship_configs_collection = database.get_collection("ship")
        
        self.ship_configs = ship_configs_collection.find({"ship_imo": self.ship_imo})[0]
        

    #@check_status
    def get_daily_data(self,index):
        """daily_data_collection = self.database.daily_data
        self.daily_data = daily_data_collection.find({"ship_imo": int(self.ship_imo)})[0]"""
        daily_data_collection =database.get_collection("daily_data")
        self.daily_data = daily_data_collection.find({"ship_imo": self.ship_imo})[index]
        
            

        

    

    @check_status
    def get_ship_stats(self):
        ship_stats_collection = self.database.ship_stats
        self.ship_stats = ship_stats_collection.find({"ship_imo": int(self.ship_imo)})[0]

    #@check_status
    def build_base_dict(self, identifier):
        return self.base_dict(identifier=identifier)
        """return self.base_dict(identifier=identifier,
                              name=self.ship_configs['data'][identifier]['name'],
                              unit=self.ship_configs['data'][identifier]['unit'],
                              reported=self.daily_data['data'][identifier])"""


    #@check_status
    def process_daily_data(self):
    
        self.data =  {}
        IP = IndividualProcessors(configs=self.ship_configs,dd=self.daily_data)
        for key,val in self.ship_configs['data'].items():
            
            base_dict = self.build_base_dict(key)
            
            try:
                self.ship_configs['data'][key]
                self.data[key] = eval("IP."+key+"_processor")(base_dict) # IP.rpm_processor(base_dict)
            except KeyError:
                continue
            except AttributeError:
                continue
        
    @check_status
    def process_weather_api_data(self):
        self.weather_data = {}

    @check_status
    def process_position_api_data(self):
        self.position_data = {}

    @check_status
    def process_indices(self):
        self.indices_data = {}

    #@check_status
    def main_db_writer(self):
        
        self.main_db = {}
        self.main_db["ship_imo"] = self.ship_imo
        self.main_db['date'] = datetime.utcnow()
        self.main_db['historical'] = False
        self.main_db['daily_data'] = self.data
        self.main_db['weather_api'] = {}
        self.main_db['position_api'] = {}
        self.main_db['indices'] = {}
        db.Main_db.insert_one(self.main_db)
    
    def ad_all(self):
        imo=self.ship_imo
        daily_data_collection =database.get_collection("daily_data")
        self.all_data = daily_data_collection.find({"ship_imo": self.ship_imo})
        for i in range(self.all_data.count()):
            self.get_daily_data(i)
            self.base_dict()
            self.process_daily_data()
            self.main_db_writer()


obj=Processor(9591301)

obj.get_ship_configs()
#obj.get_daily_data()
#obj.ad_all()
