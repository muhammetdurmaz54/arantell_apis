
import sys 
sys.path.insert(1,"arantell_apis-main")
from src.db.setup_mongo import connect_db
from src.processors.dd_processor.processor import Processor
from src.configurations.logging_config import CommonLogger
from datetime import datetime
from src.db.schema.main import Main_db
from src.db.schema.ship import Ship 
from src.db.schema.ddschema import DailyData
from src.db.schema.ship_stats import Ship_Stats 
import numpy as np
from pymongo import MongoClient
from scipy import stats
import statistics
log = CommonLogger(__name__,debug=True).setup_logger()


client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
database = db


class StatsGenerator():

    """def __init__(self,
                 ship_imo,
                 from_date,
                 to_date,
                 override,
                 all):
        pass"""

    def __init__(self,ship_imo):
        self.ship_imo = ship_imo
        pass

    def connect_db(self):
        self.database = connect_db()

        
    def get_main_db(self):
        maindb =database.get_collection("Main_db")
        self.main_db =maindb.find({"ship_imo": self.ship_imo})
        self.main_db_o=maindb.find({"ship_imo": self.ship_imo})[0]    #from date ==will be taken from init
        self.main_db_l=maindb.find({"ship_imo": self.ship_imo})[self.main_db.count()-1]   #to date == will be taken from init
        

    def process_main_db(self):
        self.stats={}
        for main_key in range(self.main_db.count()):
            for i,j in self.main_db[main_key]['daily_data'].items():
                if i in self.stats:
                    self.stats[i].append(j['processed'])
                else:
                    self.stats[i]=[]
                    self.stats[i].append(j['processed'])

        self.daily_data={}
        for key,val in self.stats.items():
            self.daily_data[key]={
                "mean":float(round(self.process_mean(val),3)),
                "median":float(round(self.process_median(),3)),
                "mode":float(round(self.process_mode(),3)),
                "variance":float(round(self.process_variance(),3)),
                "standard_deviation":float(round(self.process_deiviation(),3)),
                "min":float(round(self.process_min(),3)),
                "max":float(round(self.process_max(),3))
            }
        
        return self.daily_data    
        
    def process_mean(self,value):
        self.new_val=[]
        for i in value:
            if i==None or (type(i)!= int and type(i)!=float):
                self.new_val.append(0)
            else:
                self.new_val.append(i)
        
        return (sum(self.new_val))/(len(self.new_val)+1)
    
    def process_median(self):
        return (np.median(self.new_val))

    def process_mode(self):
        if len(self.new_val)==0:
            return 0
        else:
            return (statistics.mode(self.new_val))

    def process_variance(self):
        if len(self.new_val)==0:
            return 0
        else:
            return (np.var(self.new_val))

    def process_deiviation(self):
        if len(self.new_val)==0:
            return 0
        else:
            return (np.std(self.new_val))

    def process_min(self):
        if len(self.new_val)==0:
            return 0
        else:
            return (np.min(self.new_val))

    def process_max(self):
        if len(self.new_val)==0:
            return 0
        else:
            return (np.max(self.new_val))

    def write_ship_stats(self):

        self.ship_stats={}
        self.ship_stats["ship_imo"]=self.ship_imo
        self.ship_stats["added_on"]= datetime.utcnow()
        self.ship_stats["from_date"] = self.main_db_o["date"]   #to be taken from init later
        self.ship_stats["to_date"]= self.main_db_l["date"]   # to be taken from init later
        self.ship_stats["samples"]=self.main_db.count()
        self.ship_stats["daily_data"]=self.process_main_db()
        self.ship_stats["weather_api"]={}
        self.ship_stats["position_api"]={}
        self.ship_stats["indices"]={}
        db.Ship_Stats.insert_one(self.ship_stats)


obj=StatsGenerator(9591301)
obj.get_main_db()
obj.process_main_db()
obj.write_ship_stats()