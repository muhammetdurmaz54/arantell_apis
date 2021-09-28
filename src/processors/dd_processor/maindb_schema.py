class MainDB():

    def __init__(self,ship_imo,date,override):
        pass

    def do_steps(self):
        "defining steps to call each funtion one by one"

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
        "returning a noraml dictionary"

    def connect_db(self):
        pass

    def get_ship_configs(self):
        "read shipconfigs from db"

    def get_daily_data(self,index):
        "read dailydata from db"
    
    def build_base_dict(self, identifier):
        "returning some values of base_dict with respect to identifier"

    def within_good_voyage_limits(self):
        "returning true false with specified condtition"
    
    def process_daily_data(self):
        "reading and processing dailydata"

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

    def main_db_writer(self):
        "writing into main from processed daily data"

    def get_main_db(self):
        "read maindb"
    
    def write_ship_stats(self):
        "writing into shipstats"

    def get_ship_stats(self):
        "read shipstats"

    def update_maindb(self):
        "calculate zscore and perform calculations on the main db values and update maindb"

    