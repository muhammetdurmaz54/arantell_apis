# Generic Report. Each report will have it's own collection.


document = {
	"ship_imo": 9876543,
	"ship_name": "RMTCourier",
	"date": Date("2016-05-18T16:00:00Z"),
	"historical":False,
	"upload_datetime": Date("2016-05-18T16:00:00Z"),
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
