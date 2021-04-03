
import datetime

document = {
    "ship_imo": 9876543,
    "date": datetime.datetime(2020, 5, 17), #add timestamp as well
    "historical":False,
    "daily_data": { #200
       'rpm': {
         "identifier":"rpm",
         "name": "RPM",
         "reported":70, #done
         "processed": 70, #done
         "is_outlier": False,
         "results":True,
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
       'foc':  {
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
            "type": "ml",
        },
        "index2": {
            "identifier": "index2",
            "processed": 23,
            "type": "formula",
        },
    },
    "faults":{
        "fault1":{
            "probability":20,
            "triggered":False,
            "resolved":None,
            "feedback_code": None,
            "feedback_remarks":None,
        },
        "fault2": {
            "probability": 90,
            "triggered": True,
            "resolved": False,
            "feedback_code": 2,
            "feedback_remarks": "Comments by user",
        },
        "fault3": {
            "probability": 10,
            "triggered": False,
            "resolved": None,
            "feedback_code": None,
            "feedback_remarks": None,
        },
    },
    "health_status":{
        "air_cooler":80,
        "sea_water_pump":90,
    }
}


