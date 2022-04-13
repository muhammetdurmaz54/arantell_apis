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
        # cp_compliance_list = self.configuration.create_cp_compliance_list()
        active_ships = self.configuration.create_list_of_active_ships()
        total_ships = len(active_ships)

        actual_active_ships = 0
        for i in active_ships:
            if i != None:
                actual_active_ships = actual_active_ships + 1

        daily_data_received = str(int((actual_active_ships * 100) / total_ships)) + "%"

        total_issues, issuesCount = self.configuration.create_dict_of_issues('')
        cp_compliance_dict = self.configuration.create_cp_compliance_dict()
        print("COMPLIANCE", cp_compliance_dict)
        print("ISSUES COUNT", issuesCount)

        total_issues_number = 0
        result = {}

        for i in range(len(imo_list)):
            try:
                vessel_load = vessel_load_list[i]
                eta = eta_list[i]
                # len_of_cp_dict = len(list(set(list(cp_compliance_dict[].values())))) == 1
                tempImo = imo_list[i].split('-')[0]
                try:
                    for cp in cp_compliance_dict[int(tempImo)]:
                        if cp['compliant'] == "Yes":
                            cp_compliance = "Yes"
                        else:
                            cp_compliance = "No"
                except KeyError:
                    cp_compliance = None
                # cp_compliance = cp_compliance_list[i]
            except IndexError:
                vessel_load = None
                eta = None
            result[i] = {'imo_string': imo_list[i], 'vessel_load': vessel_load, 'eta': eta, 'cp': cp_compliance}
        
        for key in total_issues.keys():
            for i in result.keys():
                tempImo = result[i]['imo_string'].split('-')[0]
                if key == tempImo:
                    result[i]['issues'] = total_issues[key]
                    total_issues_number = total_issues_number + issuesCount[int(key)]['outlier'] + issuesCount[int(key)]['operational']
                    issueText = str(issuesCount[int(key)]['outlier']) + ' of Outliers, ' + str(issuesCount[int(key)]['operational']) + ' of Operational.'
                    result[i]['issuesCount'] = issueText
        
        return result, actual_active_ships, total_ships, daily_data_received, total_issues_number

# res = OverviewExtractor()
# res.process_data()