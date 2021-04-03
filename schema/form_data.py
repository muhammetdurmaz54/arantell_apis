# Generic Report. Each report will have it's own collection.

import datetime

document = {
	"ship_imo": 9876543,
	"ship_name": "RMTCourier",
	"date": datetime.datetime(2020, 5, 17),
	"historical":False,
	"upload_datetime": datetime.datetime(2020, 5, 17),
	"file_name":"lub_oil_data_xyz.xlsx",
	"file_url":"aws.s3.xyz.com",
	"uploader_details":{"userid":"xyz","company":"sdf"},
	"data_available": ['viscocity','temp','basicity'],
    "data": [
	{       "identifier":"viscocity",
			"reported": 70

	},
	{       "identifier":"temp",
			"reported": 46
	}
	]
}

