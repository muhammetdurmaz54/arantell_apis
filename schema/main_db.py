
import datetime

document = {
   "ship_imo": 9876543,
   "date": datetime.datetime(2020, 5, 17),
   "historical":False,
   "daily_data": {
       'rpm': {
         "identifier":"rpm",
         "name": "RPM",
         "reported":70,
         "processed": 70,
         "is_outlier": False,
         "results":"Passed",
         "z_score": -2.1,
         "unit":"rpm",
         "statement":"RPM is Low",
         "type":"continuous",
         "predictions":{
                "m3":[71,72,73],
                "m6": [71,72,73],
                "m12": [71,72,73],
                "ly": [71,72,73],
                "dd": [71,72,73]
         }},
        'speed':  {
         "identifier":"speed",
         "name": "Speed",
         "reported":70,
         "processed": 70,
         "is_outlier": False,
         "preprocessor_results":"Passed",
         "z_score": -2.1,
         "unit":"rpm",
         "statement":"RPM is Low",
         "type": "continuous",
         "predictions":{
                    "m3":[71,72,73],
                    "m6": [71,72,73],
                    "m12": [71,72,73],
                    "ly": [71,72,73],
                    "dd": [71,72,73]
         }}
   },
    "weather_api":{
        "tempC": {
            "identifier":"tempC",
            "processed":12,
            "type": "continuous",
            },
        "PresC": {
                "identifier": "PresC",
                "processed": 12,
                "type": "continuous",
            },

    },
    "position_api": {
        "lat": {
            "identifier": "lat",
            "processed": 18.520430,
            "type": "location",
        },
        "long": {
            "identifier": "long",
            "processed": 73.856743,
            "type": "location",
        },

    },
    "indices": {
        "index1": {
            "identifier": "index1",
            "processed":70,
            "type": "continuous",
        },
        "index2": {
            "identifier": "index2",
            "processed": 23,
            "type": "continuous",
        },
    }
}


