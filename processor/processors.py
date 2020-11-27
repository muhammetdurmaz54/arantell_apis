from pymongo import MongoClient
from db.setup_mongo import database
from processor.individual_processors import IndividualProcessors

class Processor(IndividualProcessors):

    def __init__(self,imo,date):
        IndividualProcessors.__init__(self)
        self.database= None
        self.ship_configs = None
        self.daily_data = None
        self.ship_imo = imo
        self.date = date #TODO Convert date into specified format.
        pass

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

    def connect_db(self):
        client = MongoClient()
        self.database = client.aranti

    def get_ship_configs(self):
        ship_configs_collection = self.database.ship_configs
        self.ship_configs = ship_configs_collection.find({"ship_imo": int(self.ship_imo)})[0]
        print(self.ship_configs)

    def get_daily_data(self):
        daily_data_collection = self.database.daily_data
        self.daily_data = daily_data_collection.find({"ship_imo": int(self.ship_imo)})[0]
        print(self.daily_data)

    def get_ship_stats(self):
        ship_stats_collection = self.database.ship_stats
        self.ship_stats = ship_stats_collection.find({"ship_imo": int(self.ship_imo)})[0]

    def build_base_dict(self, identifier):
        return self.base_dict(identifier=identifier,
                              name=self.ship_configs['data'][identifier]['name'],
                              unit=self.ship_configs['data'][identifier]['unit'],
                              reported=self.daily_data['data'][identifier])

    def process_daily_data(self):
        self.data =  {}
        for key,val in self.daily_data['data'].items():
            base_dict = self.build_base_dict(key)
            self.data[key] = eval("self."+key+"_processor")(base_dict)

    def process_weather_api_data(self):
        self.weather_data = {}

    def process_position_api_data(self):
        self.position_data = {}

    def process_indices(self):
        self.indices_data = {}

    def main_db_writer(self):
        self.main_db = {}
        self.main_db["ship_imo"] = self.ship_imo
        self.main_db['date'] = "date"
        self.main_db['historical'] = False
        self.main_db['daily_data'] = self.data
        self.main_db['weather_api'] = self.weather_data
        self.main_db['position_api'] = self.position_data
        self.main_db['indices'] = self.indices_data

        self.database.main_db.insert_one(self.main_db)


proc = Processor(9591301,'asd')
proc.connect_db()
proc.get_daily_data()
proc.get_ship_configs()
proc.get_ship_stats()
proc.process_daily_data()
proc.process_weather_api_data()
proc.process_position_api_data()
proc.process_indices()
proc.main_db_writer()