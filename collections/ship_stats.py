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

document = {
		"ship_imo":9591301,
		"data":{
			"rpm": {
				"mean": 70,
				"median": 72,
				"mode": 66,
				"variance": 100,
				"standard_deviation": 10,
				"min": 46,
				"max": 85,
				"sample_size": 1000,
				"updated_on": Date("2016-05-18T16:00:00Z"),
			},
			"speed":{
				"mean": 12,
				"median": 11,
				"mode": 12,
				"variance": 5,
				"standard_deviation": 2.23,
				"min": 6,
				"max": 16,
				"samples": 1000,
				"updated_on": Date("2016-05-18T16:00:00Z"),
			},
		}
}





