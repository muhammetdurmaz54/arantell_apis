# react application backend for performance page


from src.processors.config_extractor.configurator import Configurator
import numpy as np

class Performance():
    '''
        `This class deals with the Performance Page in the FrontEnd.
        All the functions with the exception of *process_data* will
        be used for the Reports Page`
    '''
    def __init__(self, ship_imo, type):
        self.ship_imo = int(ship_imo)
        self.type = type
    
    def instantiate_configurator(self):
        configuration = Configurator(self.ship_imo)
        return configuration
    
    def process_data(self):
        if self.type == 'actual_baseline_foc':
            baseline_values_ballast, baseline_values_loaded, fuel_cons_values_ballast, fuel_cons_values_loaded, x_axis_list_ballast, x_axis_list_loaded = self.get_actual_baseline_foc_data_lists()
            return baseline_values_ballast, baseline_values_loaded, fuel_cons_values_ballast, fuel_cons_values_loaded, x_axis_list_ballast, x_axis_list_loaded
        elif self.type == 'actual_baseline_foc_speed':
            baseline_values_speed_ballast, baseline_values_speed_loaded, fuel_cons_values_speed_ballast, fuel_cons_values_speed_loaded, speed_values_ballast, speed_values_loaded = self.get_actual_baseline_foc_speed_data_lists()
            return baseline_values_speed_ballast, baseline_values_speed_loaded, fuel_cons_values_speed_ballast, fuel_cons_values_speed_loaded, speed_values_ballast, speed_values_loaded
        elif self.type == 'deviation':
            percentage_values_ballast, percentage_values_loaded, date_list_ballast, date_list_loaded, loaded_shapes, ballast_shapes = self.get_deviation()
            return percentage_values_ballast, percentage_values_loaded, date_list_ballast, date_list_loaded, loaded_shapes, ballast_shapes
        elif self.type == 'table':
            table_loaded, table_ballast, column_headers = self.get_table_data()
            return table_loaded, table_ballast, column_headers

    
    def get_actual_baseline_foc_data_lists(self):
        '''
            Gets the lists required for the Actual v/s Baseline FOC graph.

            :return: <list> Return the data lists required for the graph.
        '''

        configuration = self.instantiate_configurator()
        baseline_values_ballast = configuration.get_baseline_pred('', False)
        baseline_values_loaded = configuration.get_baseline_pred('', True)
        fuel_cons_values_ballast = configuration.get_all_fuel_cons_data('', False)
        fuel_cons_values_loaded = configuration.get_all_fuel_cons_data('', True)

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
        baseline_values_ballast = configuration.get_baseline_pred('', False)
        baseline_values_loaded = configuration.get_baseline_pred('', True)
        fuel_cons_values_ballast = configuration.get_all_fuel_cons_data('', False)
        fuel_cons_values_loaded = configuration.get_all_fuel_cons_data('', True)
        speed_values_ballast = configuration.get_speed_data('', False)
        speed_values_loaded = configuration.get_speed_data('', True)

        return baseline_values_ballast, baseline_values_loaded, fuel_cons_values_ballast, fuel_cons_values_loaded, speed_values_ballast, speed_values_loaded
    
    def get_deviation(self):
        '''
            Gets the lists required for the Percentage Difference w.r.t
            date graph.

            :return: <list> Return the data lists required for the graph.
        '''

        configuration = self.instantiate_configurator()
        percentage_values_ballast = configuration.get_percentage_difference_for_all_values('', False)
        percentage_values_loaded = configuration.get_percentage_difference_for_all_values('', True)

        loaded_10, loaded_90, ballast_10, ballast_90 = self.divide_graph_10_90_percentage(percentage_values_ballast, percentage_values_loaded)

        # To account for the discrepancy that arises due to taking percentage of the maximum values
        if 0 < min(percentage_values_loaded) and 0 < loaded_10:
            loaded_y0 = 0
        elif min(percentage_values_loaded) < loaded_10:
            loaded_y0 = min(percentage_values_loaded)
        else:
            loaded_y0 = loaded_10

        if 0 < min(percentage_values_ballast) and 0 < ballast_10:
            ballast_y0 = 0
        elif min(percentage_values_ballast) < ballast_10:
            ballast_y0 = min(percentage_values_ballast)
        else:
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
                'y1': max(percentage_values_loaded),
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
                'y1': max(percentage_values_ballast),
                'fillcolor': 'rgb(255, 0, 0)',
                'opacity': 0.2,
                'line': {'width': 0}
            }
        ]

        date_list_ballast = configuration.get_date_list('', False)
        date_list_loaded = configuration.get_date_list('', True)

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

        loaded_10 = (max(loaded_values) * 10) / 100
        loaded_90 = (max(loaded_values) * 90) / 100

        ballast_10 = (max(ballast_values) * 10) / 100
        ballast_90 = (max(ballast_values) * 90) / 100

        return loaded_10, loaded_90, ballast_10, ballast_90
    
    def get_table_data(self):
        '''
            Gets the data for the table content in the Performance page.
        '''

        configuration = self.instantiate_configurator()

        table_data_loaded = configuration.get_fuel_cons_data('', True)
        table_data_ballast = configuration.get_fuel_cons_data('', False)

        column_headers = [
            'Date (yyyy-mm-dd)',
            'Displacement (t)',
            'Draft Aft (m)',
            'Draft Fwd (m)',
            '24 h Corrected F.O.C (t)',
            'Weather corrected Baseline (t)',
            'Difference (%)',
            'Torque Rich Index'
        ]

        return table_data_loaded, table_data_ballast, column_headers