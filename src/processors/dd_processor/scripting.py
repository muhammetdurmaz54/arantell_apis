import sys

import pandas 
sys.path.insert(1,"F:\\Afzal_cs\\Internship\\arantell_apis-main")
from src.processors.config_extractor.extract_config_new import ConfigExtractor
from src.processors.dd_extractor.extractor_new import DailyInsert
from src.processors.stats_generator.stats_generator import StatsGenerator
from src.processors.dd_processor.maindb import MainDB




class AddData():
    def __init__(self,ship_imo,config_file,daily_data_file):
        self.ship_imo=ship_imo
        self.config_file=config_file
        self.daily_data_file=daily_data_file

    def runall(self):
        config_obj=ConfigExtractor(self.ship_imo,"filepath",True)  #done
        config_obj.do_steps()
        daily_data_obj=DailyInsert("filepath",self.ship_imo)   #(remaining)
        daily_data_obj.do_steps()
        maindb_obj=MainDB(self.ship_imo)    #what all to be taken?  (remaining)
        maindb_obj.do_steps()
        ship_stats_obj=StatsGenerator(self.ship_imo,True)   #done 
        ship_stats_obj.do_steps()



obj=AddData()
obj.runall()