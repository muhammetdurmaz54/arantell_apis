# react application backend for baseline data process

from src.processors.config_extractor.configurator import Configurator
import numpy as np

class ReportsVoyagePerformance():
    def __init__(self, ship_imo, voyage):
        '''
            Initialize the class.

            :param ship_imo: <str> The IMO number of the ship.
            :param voyage: <voyage> The string representation of
                            the voyage number.
            
            :return: <dict> Return the data required to create the PDF
                    report for the specified voyage.
        '''

        self.ship_imo = int(ship_imo)
        self.voyage = voyage
    
    def instantiate_configurator(self):
        '''
            Instantiate the Configurator class with the ship_imo.

            :return: <Configurator> Return the initialized Configurator object.
        '''
        configuration = Configurator(self.ship_imo)
        return configuration
    
    def process_data(self):
        '''
            Process the data as named. Calls the required functions from the
            Configurator and combines the returned data in a dictionary.

            :return: <dict> Return the data for the specified voyage in a dictionary.
        '''

        configuration = self.instantiate_configurator()
        graph_values_dict_loaded = {}
        graph_values_dict_ballast = {}
        result = {}

        # Graph Data
        baseline_values_ballast, baseline_values_loaded, fuel_cons_values_ballast, fuel_cons_values_loaded, x_axis_list_ballast, x_axis_list_loaded = self.get_actual_baseline_foc_data_lists()
        baseline_values_speed_ballast, baseline_values_speed_loaded, fuel_cons_values_speed_ballast, fuel_cons_values_speed_loaded, speed_values_ballast, speed_values_loaded = self.get_actual_baseline_foc_speed_data_lists()
        percentage_values_ballast, percentage_values_loaded, date_list_ballast, date_list_loaded, loaded_shape, ballast_shape = self.get_deviation()

        # Table Data
        # Loaded Data
        performance_analysis_list_loaded = configuration.get_performance_data(voyage=self.voyage, loaded=True)
        weather_data_loaded = configuration.get_weather_data(voyage=self.voyage, loaded=True)
        fuel_cons_data_loaded = configuration.get_fuel_cons_data(voyage=self.voyage, loaded=True)
        # Ballast Data
        performance_analysis_list_ballast = configuration.get_performance_data(voyage=self.voyage, loaded=False)
        weather_data_ballast = configuration.get_weather_data(voyage=self.voyage, loaded=False)
        fuel_cons_data_ballast = configuration.get_fuel_cons_data(voyage=self.voyage, loaded=False)

        # Vessel Details
        # Loaded Data
        vessel_details_loaded = configuration.get_vessel_details(voyage=self.voyage, loaded=True)
        # Ballast Data
        vessel_details_ballast = configuration.get_vessel_details(voyage=self.voyage, loaded=False)


        graph_values_dict_loaded['Actual Baseline FOC'] = {
            'Baseline': baseline_values_loaded,
            'Fuel Cons': fuel_cons_values_loaded,
            'X Axis': x_axis_list_loaded
        }
        graph_values_dict_loaded['Actual Baseline FOC Speed'] = {
            'Baseline': baseline_values_speed_loaded,
            'Fuel Cons': fuel_cons_values_speed_loaded,
            'Speed': speed_values_loaded
        }
        graph_values_dict_loaded['Percentage Difference'] = {
            'Percentage': percentage_values_loaded,
            'Date': date_list_loaded,
            'Shape': loaded_shape
        }

        graph_values_dict_ballast['Actual Baseline FOC'] = {
            'Baseline': baseline_values_ballast,
            'Fuel Cons': fuel_cons_values_ballast,
            'X Axis': x_axis_list_ballast
        }
        graph_values_dict_ballast['Actual Baseline FOC Speed'] = {
            'Baseline': baseline_values_speed_ballast,
            'Fuel Cons': fuel_cons_values_speed_ballast,
            'Speed': speed_values_ballast
        }
        graph_values_dict_ballast['Percentage Difference'] = {
            'Percentage': percentage_values_ballast,
            'Date': date_list_ballast,
            'Shape': ballast_shape
        }


        result['Loaded'] = {
            'Vessel Details': vessel_details_loaded,
            'Data Points For Performance Analysis': performance_analysis_list_loaded,
            'Weather Data Along the Sailing Route From OSCAR, NOAA, OSTIA': weather_data_loaded,
            'Fuel Oil Consumption': fuel_cons_data_loaded
        }
        result['Ballast'] = {
            'Vessel Details': vessel_details_ballast,
            'Data Points For Performance Analysis': performance_analysis_list_ballast,
            'Weather Data Along the Sailing Route From OSCAR, NOAA, OSTIA': weather_data_ballast,
            'Fuel Oil Consumption': fuel_cons_data_ballast
        }
        result['Loaded']['Graph Data'] = graph_values_dict_loaded
        result['Ballast']['Graph Data'] = graph_values_dict_ballast
        result['Title'] = 'Voyage Performance Report'

        return result
    
    def get_actual_baseline_foc_data_lists(self):
        '''
            Gets the lists required for the Actual v/s Baseline FOC graph.

            :return: <list> Return the data lists required for the graph.
        '''

        configuration = self.instantiate_configurator()
        baseline_values_ballast = configuration.get_baseline_pred(self.voyage, False)
        baseline_values_loaded = configuration.get_baseline_pred(self.voyage, True)
        fuel_cons_values_ballast = configuration.get_all_fuel_cons_data(self.voyage, False)
        fuel_cons_values_loaded = configuration.get_all_fuel_cons_data(self.voyage, True)

        x_axis_loaded_list = np.arange(1, len(baseline_values_loaded)+1)
        x_axis_ballast_list = np.arange(1, len(baseline_values_ballast)+1)

        return baseline_values_ballast, baseline_values_loaded, fuel_cons_values_ballast, fuel_cons_values_loaded, x_axis_ballast_list.tolist(), x_axis_loaded_list.tolist()
    
    def get_actual_baseline_foc_speed_data_lists(self):
        '''
            Gets the lists required for the Actual v/s Baseline FOC
            w.r.t Speed graph.

            :return: <list> Return the data lists required for the graph.
        '''

        configuration = self.instantiate_configurator()
        baseline_values_ballast = configuration.get_baseline_pred(self.voyage, False)
        baseline_values_loaded = configuration.get_baseline_pred(self.voyage, True)
        fuel_cons_values_ballast = configuration.get_all_fuel_cons_data(self.voyage, False)
        fuel_cons_values_loaded = configuration.get_all_fuel_cons_data(self.voyage, True)
        speed_values_ballast = configuration.get_speed_data(self.voyage, False)
        speed_values_loaded = configuration.get_speed_data(self.voyage, True)

        return baseline_values_ballast, baseline_values_loaded, fuel_cons_values_ballast, fuel_cons_values_loaded, speed_values_ballast, speed_values_loaded
    
    def get_deviation(self):
        '''
            Gets the lists required for the Percentage Difference w.r.t
            date graph.

            :return: <list> Return the data lists required for the graph.
        '''

        configuration = self.instantiate_configurator()
        percentage_values_ballast = configuration.get_percentage_difference_for_all_values(self.voyage, False)
        percentage_values_loaded = configuration.get_percentage_difference_for_all_values(self.voyage, True)
        
        loaded_10, loaded_90, ballast_10, ballast_90 = self.divide_graph_10_90_percentage(percentage_values_ballast, percentage_values_loaded)
        try:
            max_percentile_loaded=max(percentage_values_loaded)
        except:
            max_percentile_loaded=None
        try:
            max_percentile_ballast=max(percentage_values_ballast)
        except:
            max_percentile_ballast=None
        # To account for the discrepancy that arises due to taking percentage of the maximum values
        try:
            if 0 < min(percentage_values_loaded) and 0 < loaded_10:
                loaded_y0 = 0
            elif min(percentage_values_loaded) < loaded_10:
                loaded_y0 = min(percentage_values_loaded)
            else:
                loaded_y0 = loaded_10
        except:
            loaded_y0 = loaded_10

        try:
            if 0 < min(percentage_values_ballast) and 0 < ballast_10:
                ballast_y0 = 0
            elif min(percentage_values_ballast) < ballast_10:
                ballast_y0 = min(percentage_values_ballast)
            else:
                ballast_y0 = ballast_10
        except:
            ballast_y0 = ballast_10


        # (`x0`,`y0`), (`x1`,`y0`), (`x1`,`y1`), (`x0`,`y1`), (`x0`,`y0`) 
        loaded_shape = [
            {
                'type': 'rect',
                'xref': 'paper',
                'x0': 0,
                'x1': 1,
                'y0': loaded_y0,
                # 'y1': min(percentage_values_loaded),
                'y1': loaded_10,
                'fillcolor': 'rgb(0, 255, 0)',
                'opacity': 0.2,
                'line': {'width': 0}
            },
            {
                'type': 'rect',
                'xref': 'paper',
                'x0': 0,
                'x1': 1,
                # 'y0': min(percentage_values_loaded),
                # 'y1': max(percentage_values_loaded),
                'y0': loaded_10,
                'y1': max_percentile_loaded,
                'fillcolor': 'rgb(255, 0, 0)',
                'opacity': 0.2,
                'line': {'width': 0}
            }
        ]
        # (`x0`,`y0`), (`x1`,`y0`), (`x1`,`y1`), (`x0`,`y1`), (`x0`,`y0`)
        ballast_shape = [
            {
                'type': 'rect',
                'xref': 'paper',
                'x0': 0,
                'x1': 1,
                'y0': ballast_y0,
                # 'y1': min(percentage_values_ballast),
                'y1': ballast_10,
                'fillcolor': 'rgb(0, 255, 0)',
                'opacity': 0.2,
                'line': {'width': 0}
            },
            {
                'type': 'rect',
                'xref': 'paper',
                'x0': 0,
                'x1': 1,
                # 'y0': min(percentage_values_ballast),
                # 'y1': max(percentage_values_ballast),
                'y0': ballast_10,
                'y1': max_percentile_ballast,
                'fillcolor': 'rgb(255, 0, 0)',
                'opacity': 0.2,
                'line': {'width': 0}
            }
        ]

        date_list_ballast = configuration.get_date_list(self.voyage, False)
        date_list_loaded = configuration.get_date_list(self.voyage, True)

        return percentage_values_ballast, percentage_values_loaded, date_list_ballast, date_list_loaded, loaded_shape, ballast_shape

    def divide_graph_10_90_percentage(self, ballast_values, loaded_values):
        '''
            Returns the values for the 10% and the 90% mark for the ballast
            and loaded values. These values will then be used to create the
            shapes for the graph to will divide it.

            :param ballast_values: <list> The ballast values of percentage difference.
            :param loaded_values: <list> The loaded values of percentage difference.

            :return: <number> Returns the 10% and the 90% mark values for both the
                            ballast and loaded lists.
        '''
        # print("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk")
        # print(loaded_values)
        # print(ballast_values)
        try:
            loaded_10 = (max(loaded_values) * 10) / 100
        except:
            loaded_10 = None
        try:
            loaded_90 = (max(loaded_values) * 90) / 100
        except:
            loaded_90 = None
        try:
            ballast_10 = (max(ballast_values) * 10) / 100
        except:
            ballast_10 = None
        try:
            ballast_90 = (max(ballast_values) * 90) / 100
        except:
            ballast_90 = None

        return loaded_10, loaded_90, ballast_10, ballast_90
