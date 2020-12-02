from src.db.setup_mongo import connect_db
from src.dd_processor.individual_processors import IndividualProcessors
from src.configurations.logging_config import CommonLogger

log = CommonLogger(__name__,debug=True).setup_logger()

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
    def wrapper(self, *arg, **kw):
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
    return wrapper


class Processor(IndividualProcessors):

    def __init__(self,ship_imo,date):
        IndividualProcessors.__init__(self)
        self.database= None
        self.ship_configs = None
        self.daily_data = None
        self.ship_imo = ship_imo
        self.date = date
        self.error = False
        self.traceback_msg = None
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

    @staticmethod
    def base_dict(identifier,
                  name=None,
                  reported=None,
                  processed=None,
                  is_outlier=None,
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
                "is_outlier": is_outlier,
                "results": results,
                "z_score": z_score,
                "unit": unit,
                "statement": statement,
                "predictions": predictions}

    @check_status
    def connect_db(self):
        self.database = connect_db()

    @check_status
    def get_ship_configs(self):
        ship_configs_collection = self.database.ship_configs
        self.ship_configs = ship_configs_collection.find({"ship_imo": int(self.ship_imo)})[0]


    @check_status
    def get_daily_data(self):
        daily_data_collection = self.database.daily_data
        self.daily_data = daily_data_collection.find({"ship_imo": int(self.ship_imo)})[0]


    @check_status
    def get_ship_stats(self):
        ship_stats_collection = self.database.ship_stats
        self.ship_stats = ship_stats_collection.find({"ship_imo": int(self.ship_imo)})[0]

    @check_status
    def build_base_dict(self, identifier):
        return self.base_dict(identifier=identifier,
                              name=self.ship_configs['data'][identifier]['name'],
                              unit=self.ship_configs['data'][identifier]['unit'],
                              reported=self.daily_data['data'][identifier])

    @check_status
    def process_daily_data(self):
        self.data =  {}
        for key,val in self.daily_data['data'].items():
            base_dict = self.build_base_dict(key)
            self.data[key] = eval("self."+key+"_processor")(base_dict)

    @check_status
    def process_weather_api_data(self):
        self.weather_data = {}

    @check_status
    def process_position_api_data(self):
        self.position_data = {}

    @check_status
    def process_indices(self):
        self.indices_data = {}

    @check_status
    def main_db_writer(self):
        self.main_db = {}
        self.main_db["ship_imo"] = self.ship_imo
        self.main_db['date'] = str(self.date)
        self.main_db['historical'] = False
        self.main_db['daily_data'] = self.data
        self.main_db['weather_api'] = self.weather_data
        self.main_db['position_api'] = self.position_data
        self.main_db['indices'] = self.indices_data
        return self.database.main_db.insert_one(self.main_db).inserted_id

