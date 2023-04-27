from src.processors.config_extractor.configurator import Configurator
from pymongo import DESCENDING
import math

class AisData:
    def __init__(self, ship_imo):
        self.parameters = ['departure_port', 'departure_atd', 'arrival_port', 'arrival_atd', 'latitude',
                           'longitude', 'speed', 'course', 'position_received', 'heading'
        ]
        self.ais_params_list = ['Observed Date', 'Time', 'Latitude', 'Longitude', 'Distance', 'Speed SOG', 'Course', 'Departure Port',
                                'Departure Date', 'Departure Time', 'Arrival Port', 'Arrival Date', 'Arrival Time', 'Heading'
                                ]
        self.weather_params_list = ['Time', 'Beaufort', 'Current (Kts)', 'Rel Wind Dir (Deg)',
            'Rel current dir(Ship-Deg)', 'SWell Dir (Ship)', 'Swell (Ship).'
        ]
        self.calculated_distance = 0
        self.ship_imo = ship_imo

    def instantiate_configurator(self):
        configuration = Configurator(self.ship_imo)
        return configuration
    
    def get_ais_collection(self):
        configuration = self.instantiate_configurator()
        ais_collection = configuration.get_ais_collection()

        return ais_collection
    
    def process_data(self):
        configuration = self.instantiate_configurator()
        ais_collection = self.get_ais_collection()
        valueDict = {}
        decimal_lat_list=[]
        decimal_lon_list=[]
        distance_list=[]
        time_list=[]

        arrival_time_list=[]
        departure_time_list=[]
        position_time_list=[]

        # for param in self.parameters:
        #     tempList = []
        #     paramName=None
        #     for doc in maindb_collection.find({'ship_imo': self.ship_imo}, {'processed_daily_data.'+param: 1, 'final_rep_dt': 1}).sort('final_rep_dt', DESCENDING).limit(6):
        #         paramName = doc['processed_daily_data'][param]['name'] if param != 'final_rep_dt' else 'Observed Date'
        #         if param == 'final_rep_dt':
        #             tempList.append(doc['final_rep_dt'].strftime('%Y-%m-%d'))
        #             time_list.append(doc['final_rep_dt'].strftime('%H:%M:%S'))
        #         else:
        #             if type(doc['processed_daily_data'][param]['processed']) == int or type(doc['processed_daily_data'][param]['processed']) == float:
        #                 tempList.append(configuration.makeDecimal(doc['processed_daily_data'][param]['processed']))
        #             else:
        #                 tempList.append(doc['processed_daily_data'][param]['processed'])
        #     valueDict[paramName] = tempList
        
        # valueDict['Time'] = time_list

        for param in self.parameters:
            tempList = []
            tempDate = []
            tempTime = []
            for doc in ais_collection.find({'imo': self.ship_imo}, {param: 1}).limit(3):
                if param == 'departure_atd':
                    departure_split_list = doc[param].split(',')
                    tempList.append(departure_split_list[0][5:])
                    departure_time_list.append(departure_split_list[1])
                elif param == 'arrival_atd':
                    arrival_split_list = doc[param].split(',')
                    tempList.append(arrival_split_list[0][5:])
                    arrival_time_list.append(arrival_split_list[1])
                elif param == 'position_received':
                    if doc[param] != None:
                        position_split = doc[param].split(" ")
                        tempList.append(position_split[0])
                        position_split = position_split[1:]
                        time_param = " ".join(position_split)
                        position_time_list.append(time_param)
                    else:
                        tempList.append(None)
                        position_time_list.append(None)
                else:
                    tempList.append(doc[param])
            if param == 'departure_atd':
                valueDict['Departure Date'] = tempList
            elif param == 'arrival_atd':
                valueDict['Arrival Date'] = tempList
            elif param == 'position_received':
                valueDict['Observed Date'] = tempList
            elif param == 'departure_port':
                valueDict['Departure Port'] = tempList
            elif param == 'arrival_port':
                valueDict['Arrival Port'] = tempList
            elif param == 'latitude':
                valueDict['Latitude'] = tempList
            elif param == 'longitude':
                valueDict['Longitude'] = tempList
            elif param == 'speed':
                valueDict['Speed SOG'] = tempList
            elif param == 'course':
                valueDict['Course'] = tempList
            elif param == 'heading':
                valueDict['Heading'] = tempList
        valueDict['Arrival Time'] = arrival_time_list
        valueDict['Departure Time'] = departure_time_list
        valueDict['Time'] = position_time_list


        # for i in range(len(valueDict["LAT - Current"])):
        #     deg_min_sec_lat = self.separate_numerical_components(valueDict['LAT - Current'][i])
        #     deg_min_sec_lon = self.separate_numerical_components(valueDict['LONG- Current'][i])
        #     decimal_lat = self.convert_to_degrees_minutes_seconds(deg_min_sec_lat)
        #     decimal_lon = self.convert_to_degrees_minutes_seconds(deg_min_sec_lon)
        #     decimal_lat_list.append(decimal_lat)
        #     decimal_lon_list.append(decimal_lon)
        
        # for i in range(0, 5):
        #     distance = self.get_distance(decimal_lat_list[i], decimal_lon_list[i], decimal_lat_list[i+1], decimal_lon_list[i+1])
        #     distance_list.append(configuration.makeDecimal(distance/1000))
        
        # valueDict['Distance'] = distance_list
        
        return valueDict, self.ais_params_list, self.weather_params_list

    
    def separate_numerical_components(self, latlongstr):
        '''
            Separates the alphanumeric string of latitude or longitude into
            degrees, minutes, and seconds. Returns a list of the three values.
        '''

        direction_character_removed = latlongstr[0:len(latlongstr)-1]

        degree = direction_character_removed.split(':')[0]
        time_list = direction_character_removed.split(':')[1].split('.')

        deg_min_sec = [degree, time_list[0], time_list[1]]

        return deg_min_sec
    
    def convert_to_degrees_minutes_seconds(self, deg_min_sec_list):
        '''
            Converts the values of degrees, minutes, and seconds from the list
            `deg_min_sec_list` into decimals
        '''

        decimal_value = float(deg_min_sec_list[0]) + float(deg_min_sec_list[1])/60 + float(deg_min_sec_list[2])/3600

        return decimal_value
    
    def get_distance(self, start_latitude, start_longitude, end_latitude, end_longitude):
        '''
            Finds the distance between two locations
        '''

        # Radius of Earth in meters
        R = 6371e3

        rlat1 = start_latitude * (math.pi/180)
        rlat2 = end_latitude * (math.pi/180)
        # rlon1 = start_longitude * (math.pi/180)
        # rlon2 = end_longitude * (math.pi/180)
        dlat = (end_latitude - start_latitude) * (math.pi/180)
        dlon = (end_longitude - start_longitude) * (math.pi/180)

        # Haversine formula to find distance
        a = (math.sin(dlat/2) * math.sin(dlat/2)) + (math.cos(rlat1) * math.cos(rlat2) * (math.sin(dlon/2) * math.sin(dlon/2)))
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

        # Distance in meters
        distance = R * c

        return distance



