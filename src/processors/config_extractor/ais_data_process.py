from src.processors.config_extractor.configurator import Configurator
from pymongo import DESCENDING
import pandas as pd
from datetime import datetime
import math
from dateutil import parser

class AisData:
    def __init__(self, ship_imo):
        self.parameters = ['departure_port', 'departure_atd', 'arrival_port', 'arrival_atd', 'latitude',
                           'longitude', 'speed', 'course', 'position_received', 'heading', 'Distance since previous obs','Distance since last port','Distance over 1 calender day','Distance over 24 hrs'
        ]
        self.ais_params_list = ['Observed Date', 'Time', 'Latitude', 'Longitude', 'Distance', 'Speed SOG', 'Course', 'Departure Port',
                                'Departure Date', 'Departure Time', 'Arrival Port', 'Arrival Date', 'Arrival Time', 'Heading', 'Distance since previous obs' , 'Distance since last port','Distance over 1 calender day','Distance over 24 hrs'
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


        temp_dict=ais_collection.find({'Ship Imo': self.ship_imo})
        ais_dict=self.ais_docs(temp_dict)

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
        # new_ais=reversed(ais_dict)
        # new_ais=list(new_ais)
        for param in self.parameters:
            tempList = []
            tempDate = []
            tempTime = []
            # for doc in ais_collection.find({'Ship Imo': self.ship_imo}, {param: 1}).limit(3):
            for doc in ais_dict:
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
            elif param == 'Distance since previous obs':
                valueDict['Distance since previous obs'] = tempList
            elif param == 'Distance since last port':
                valueDict['Distance since last port'] = tempList
            # elif param == 'Cumulative Distance':
            #     valueDict['Cumulative Distance'] = tempList
            elif param == 'Distance over 1 calender day':
                valueDict['Distance over 1 calender day'] = tempList
            elif param == 'Distance over 24 hrs':
                valueDict['Distance over 24 hrs'] = tempList
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
        distance = round(R * c,2)

        return distance






    # ak

    def get_distance(self,lat1,lon1,lat2,lon2):
        # Approximate radius of earth in km
        R = 6373.0

        lat1 = math.radians(lat1)
        lon1 = math.radians(lon1)
        lat2 = math.radians(lat2)
        lon2 = math.radians(lon2)

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = round(R * c,2)
        return distance




    def ais_docs(self,temp_list):
        # ais_coll = database.get_collection("Ais_collection")
        # ais_coll.update_many( {}, { "$rename": { "imo": "Ship Imo" } } )
        # temp_list = ais_coll.find({"Ship Imo": "9205926"})
        ais_Docs=[]
        port_list=[]
        date_list=[]

        # # for ec2 count is deprecated so use this
        # for i in range(0,len(list(temp_list.clone()))):
        #     ais_Docs.append(temp_list[i])
        # flag=0

        #for local count runs 
        for i in range(0,temp_list.count()):
            ais_Docs.append(temp_list[i])
        flag=0

        for i in range(0,len(ais_Docs)):
            # print(i)
            ais_Docs[i]['Distance since previous obs']=None
            ais_Docs[i]['Cumulative Distance']=None
            ais_Docs[i]['Distance since last port']=None
            ais_Docs[i]['Distance over 1 calender day']=None
            ais_Docs[i]['Distance over 24 hrs']=None
            if i==0:
                port_list.append(ais_Docs[i]['departure_port'])
                if pd.isnull(ais_Docs[i]['position_received'])==False:
                    position_split = ais_Docs[i]['position_received'].split(" ")
                    date_list.append(position_split[0])
            if i>0:
                lat1 = ais_Docs[i-1]['latitude']
                lon1 = ais_Docs[i-1]['longitude']
                lat2 = ais_Docs[i]['latitude']
                lon2 = ais_Docs[i]['longitude']
                if pd.isnull(lat1)==False and pd.isnull(lon1)==False and pd.isnull(lat2)==False and pd.isnull(lon2)==False:

                    lat1 = float(lat1[:len(lat1)-1])
                    lat1 = math.radians(lat1)

                    lon1 = float(lon1[:len(lon1)-1])
                    lon1 = math.radians(lon1)
                    
                    lat2 = float(lat2[:len(lat2)-1])
                    lat2 = math.radians(lat2)
                    
                    lon2 = float(lon2[:len(lon2)-1])
                    lon2 = math.radians(lon2)
                    ais_Docs[i]['Distance since previous obs']=self.get_distance(lat1,lon1,lat2,lon2)
                    
                
                if ais_Docs[i]['Distance since previous obs']!=None and flag==0:
                    ais_Docs[i]['Cumulative Distance']=ais_Docs[i]['Distance since previous obs']
                    flag=1
                else:
                    try:
                        ais_Docs[i]['Cumulative Distance']=ais_Docs[i]['Distance since previous obs']+ais_Docs[i-1]['Cumulative Distance']
                    except:
                        ais_Docs[i]['Cumulative Distance']=ais_Docs[i-1]['Cumulative Distance']
                if pd.isnull(ais_Docs[i]['Cumulative Distance'])==False:
                    ais_Docs[i]['Cumulative Distance']=round(ais_Docs[i]['Cumulative Distance'],2)


                if port_list[0]==ais_Docs[i]['departure_port']:
                    ais_Docs[i]['Distance since last port']=ais_Docs[i]['Distance since previous obs']
                    if pd.isnull(ais_Docs[i-1]['Distance since last port'])==False:
                        if pd.isnull(ais_Docs[i]['Distance since last port'])==False:
                            ais_Docs[i]['Distance since last port']=ais_Docs[i]['Distance since last port']+ais_Docs[i-1]['Distance since last port']
                        else:
                            ais_Docs[i]['Distance since last port']=ais_Docs[i-1]['Distance since last port']
                elif port_list[0]!=ais_Docs[i]['departure_port']:
                    port_list=[]
                    port_list.append(ais_Docs[i]['departure_port'])
                if pd.isnull(ais_Docs[i]['Distance since last port'])==False:
                    ais_Docs[i]['Distance since last port']=round(ais_Docs[i]['Distance since last port'],2)


                if len(date_list)==0 and pd.isnull(ais_Docs[i]['position_received'])==False:
                    position_split = ais_Docs[i]['position_received'].split(" ")
                    date_list.append(position_split[0])
                if pd.isnull(ais_Docs[i]['position_received'])==False and date_list[0]==ais_Docs[i]['position_received'].split(" ")[0]:
                    ais_Docs[i]['Distance over 1 calender day']=ais_Docs[i]['Distance since previous obs']
                    if pd.isnull(ais_Docs[i-1]['Distance over 1 calender day'])==False:
                        if pd.isnull(ais_Docs[i]['Distance over 1 calender day'])==False:
                            ais_Docs[i]['Distance over 1 calender day']=ais_Docs[i]['Distance over 1 calender day']+ais_Docs[i-1]['Distance over 1 calender day']
                        else:
                            ais_Docs[i]['Distance over 1 calender day']=ais_Docs[i-1]['Distance over 1 calender day']
                elif pd.isnull(ais_Docs[i]['position_received'])==False and date_list[0]!=ais_Docs[i]['position_received'].split(" ")[0]:
                    date_list=[]
                    position_split = ais_Docs[i]['position_received'].split(" ")
                    date_list.append(position_split[0])

                if pd.isnull(ais_Docs[i]['Distance over 1 calender day'])==False:
                    ais_Docs[i]['Distance over 1 calender day']=round(ais_Docs[i]['Distance over 1 calender day'],2)
            
    
        ais_Docs=reversed(ais_Docs)
        ais_Docs=list(ais_Docs)
        date_mark=[]
        final_dict=[]
        # ais_Docs[i]['Distance over 24 hrs']="24hrsdist"
        for i in range(0,len(ais_Docs)):
            try:
                if pd.isnull(ais_Docs[i]['position_received'])==False:
                    current_position_split = ais_Docs[i]['position_received'].split(" ")
                    current_date_time=parser.parse(current_position_split[0]+" "+current_position_split[1])
                    if pd.isnull(ais_Docs[i]['Distance since previous obs'])==False:
                        dist_24_hrs=ais_Docs[i]['Distance since previous obs']
                    else:
                        dist_24_hrs=0
                    # print("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",i,current_date_time)
                    for j in range(0,len(ais_Docs)):
                        if j>i:
                            next_position_split = ais_Docs[j]['position_received'].split(" ")
                            next_date_time=parser.parse(next_position_split[0]+" "+next_position_split[1])
                            diff=current_date_time-next_date_time
                            diff_split=str(diff).split(":")
                            # print("diff",diff_split)
                            if len(diff_split)==3:
                                if int(diff_split[0])<23:
                                    if pd.isnull(ais_Docs[j]['Distance since previous obs'])==False:
                                        dist_24_hrs=dist_24_hrs+ais_Docs[j]['Distance since previous obs']
                                    else:
                                        dist_24_hrs=dist_24_hrs+0
                                    # print("addddddddddd",dist_24_hrs)
                                elif (int(diff_split[0])>=23 and int(diff_split[0])<=25):
                                    ais_Docs[i]['Distance over 24 hrs']=round(dist_24_hrs,2)
                                    # print("24 hrssss",dist_24_hrs)
                            elif len(diff_split)>3:
                                if diff_split[0]=="1 day" and int(diff_split[1])==0:
                                    # print("greteer than 3333333333333",diff_split)
                                    ais_Docs[i]['Distance over 24 hrs']=round(dist_24_hrs,2)

            except:
                pass





        for i in range(0,len(ais_Docs)):


            # if pd.isnull(ais_Docs[i]['position_received'])==False:
            #     strp_date=ais_Docs[i]['position_received'].split(" ")[0]
            #     final_date=datetime.strptime(strp_date,"%Y-%m-%d")
            #     print(i,final_date)
            if len(date_mark)==0:
                try:
                    date_mark.append(datetime.strptime(ais_Docs[i]['position_received'].split(" ")[0],"%Y-%m-%d"))
                except:
                    if 'data' in ais_Docs[i]:
                        date_mark.append(datetime.strptime(ais_Docs[i]['data']['position_received'].split(" ")[0],"%Y-%m-%d"))
            if i>0:
                if 'data' in ais_Docs[i]:
                    try:
                        diff=date_mark[0]-datetime.strptime(ais_Docs[i]['position_received'].split(" ")[0],"%Y-%m-%d")
                        if diff.days<=3:
                            final_dict.append(ais_Docs[i])
                    except:
                        diff=date_mark[0]-datetime.strptime(ais_Docs[i]['data']['position_received'].split(" ")[0],"%Y-%m-%d")
                        if diff.days<3:
                            final_dict.append(ais_Docs[i])
        final_dict=reversed(final_dict)
        final_dict=list(final_dict)
        return final_dict