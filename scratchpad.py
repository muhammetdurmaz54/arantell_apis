document = {
   "ship_imo": 9876543,
   "date": Date("2016-05-18T16:00:00Z"),
   "ship_name": "RMTCourier",
   "data": [
   {
         "identifier":"rpm",
         "name": "RPM",
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

}


b = {
   "ship_imo": 9876543,
  # "date": Date("2016-05-18T16:00:00Z"),
   "ship_name": "RMTCourier",
   "data": {
       "rpm":{
             "name": "RPM",
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
       },
      "speed":{
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
   }


}
