"""
Collection to store recieved daily data as it is. 

Process in short:
User uploads DD(Daily Data) for particular ship on particular data.
Before cleaning or analysing, first we have to store data as it is as we might need to afterwards for compliance.
So this collection is used to store received data as it is.

Whether to use fields names provided by user in DD file or to use fields names decided by us is not yet concluded.
As different ships might have different naming conventions. Example say for Wind Force it could be 'w_force','wind','wind_force'
in their file, but for it to be consistent we can store with our identifiers.
"""
document = {
	"ship_imo": 9591301,
	"ship_name": "RTM COOK",
#	"date": Date("2016-05-18T16:00:00Z"),
	"historical":True,
	"nav_data_available":True,
	"engine_data_available":True,
	"nav_data_details":{
			#	"upload_datetime": Date("2016-05-18T16:00:00Z"),
				"file_name":"daily_data19June20.xlsx",
				"file_url":"aws.s3.xyz.com",
				"uploader_details":{"userid":"xyz","company":"sdf"},
	},
	"engine_data_details":{
			#	"upload_datetime": Date("2016-05-18T16:00:00Z"),
				"file_name":"daily_data19June20engine.xlsx",
				"file_url":"aws.s3.xyz.com",
				"uploader_details":{"userid":"xyz","company":"sdf"},
	},
	"data_available_nav": ['rpm','speed','w_force'],
	"data_available_engine": ['er_temp','er_hum','jwc1_fwin_temp'],
	"data":{
		"rpm":70,
		"speed":12,
	}
}



