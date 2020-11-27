def process_rpm(self,value):
    self.mydict['reported'] =


document = {
   "ship_imo": 9876543,
   "date": Date("2016-05-18T16:00:00Z"),
   "historical":False,
   "daily_data": [
   {
         "identifier":"rpm",
         "name": "RPM",
         "reported":70,
         "processed": 70,
         "is_outlier": False,
         "results":"Passed",
         "z_score": -2.1,
         "unit":"rpm",
         "statement":"RPM is Low",
         "predictions":{
                "m3":[71,72,73],
                "m6": [71,72,73],
                "m12": [71,72,73],
                "ly": [71,72,73],
                "dd": [71,72,73]
         }
   },
  {
         "identifier":"speed",
         "name": "Speed",
         "reported":70,
         "processed": 70,
         "is_outlier": False,
         "preprocessor_results":"Passed",
         "z_score": -2.1,
         "unit":"rpm",
         "statement":"RPM is Low",
         "predictions":{
                "m3":[71,72,73],
                "m6": [71,72,73],
                "m12": [71,72,73],
                "ly": [71,72,73],
                "dd": [71,72,73]
         }
   }
   ],
   "weather_api": [
       {
           "identifier":"tempC",
           "value":25
       },
       {
           "identifier":"swell",
           "value":6
       }
   ],
    "position_api": [
       {
           "identifier":"lat",
           "value":18.520430
       },
       {
           "identifier":"long",
           "value":73.856743
       }
   ],
    "indices": [
       {
           "identifier":"index1",
           "value":5
       },
       {
           "identifier":"index2",
           "value":8
       }
   ],


}


