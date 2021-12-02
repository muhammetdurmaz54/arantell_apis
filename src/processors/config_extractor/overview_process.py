from src.processors.config_extractor.configurator import Configurator
# import sys
# sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis\\src")
import pandas as pd
import numpy as np

class OverviewExtractor():
    def __init__(self):
        pass

    def process_data(self):
        self.configuration = Configurator(9591301)
        self.ship_configs = self.configuration.get_ship_configs()
        self.main_db = self.configuration.get_main_data()

        imo_list = self.configuration.create_imo_and_name_strings()
        vessel_load_list = self.configuration.create_vessel_load_list()
        eta_list = self.configuration.create_eta_list()
        cp_compliance_list = self.configuration.create_cp_compliance_list()

        result = {}

        for i in range(len(imo_list)):
            try:
                vessel_load = vessel_load_list[i]
                eta = eta_list[i]
                cp_compliance = cp_compliance_list[i]
            except IndexError:
                vessel_load = None
                eta = None
            result[i] = {'imo_string': imo_list[i], 'vessel_load': vessel_load, 'eta': eta, 'cp': cp_compliance}
        
        return result

# res = OverviewExtractor()
# res.process_data()