"""
ship_stats:

Descriptive statistics for all ship's features.
Initially when ship is on-boarded, these will be calculated from historical data.
After daily data starts coming in, these will be updated daily.

Example: Historical data of about 2 years is received. i.e. 700 rows. then these 700 rows
will be used to calculate following stats and will be used untill some new daily data is
not started arriving. When daily_data starts receiving, these stats will be changed daily/or weekly.
So next time, sample size would be 701, then 702 and so on.

One document per ship.

"""

import datetime


document = {
		"ship_imo":9591301,
		"added_on":datetime.datetime(2020, 5, 17),
		"from_date":datetime.datetime(2020, 5, 17),
		"to_date":datetime.datetime(2020, 5, 17),
		"samples":1000,
		"daily_data":{ #200
			"rpm": {
				"mean": 70,
				"median": 72,
				"mode": 66,
				"variance": 100,
				"standard_deviation": 10,
				"min": 46,
				"max": 85,
			},
			"speed":{
				"mean": 12,
				"median": 11,
				"mode": 12,
				"variance": 5,
				"standard_deviation": 2.23,
				"min": 6,
				"max": 16,
		#		"updated_on": Date("2016-05-18T16:00:00Z"),
			},
		},
	"weather_api":{},
	"position_api":{},
	"indices":{}

}





