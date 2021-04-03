
"""
Few terms:

Daily Data: User upload data for their ship daily on Aranti. Data is right now in the form on Excel file.

"""



"""
Stores all static data about each ship. There are around 50 types of static fields for 
each of the ship, like length, year, port etc. Those are stored here.

Also, each ship might have different machinary, say about 10 systems with total 200 fields which we 
will recieve daily in form of excel file. So field 'data_available' will have list of all those fields
which are expected to recieve daily.

Suppose user wants to add new machinery to their ship i.e. he wants to upload new kind of daily data. 
Say new cooler is fitted, then this 'data_available' should be modified. 

Each time when pre procecssor code will be running, it will check this 'data_available' field to see which of the 
fields to extract from daily data.

A new ship document will be added to this collection once new ship is oboarded on Aranti.

"""
import datetime


document = {
		"ship_imo":9591301, # taken from sheet 1
		"ship_name":"RTM COOK", # taken from sheet 1
		"ship_description":"RTM COOK", # taken from sheet 1
		"added_on": datetime.datetime(2020, 5, 17), #Date when ship was added. We dont take this from excel sheet.

		"static_data":{
			"grt":1234, #static field #1
			"length":1234, #static field #2
			# About 19 identifiers fetched from Fuel data. Stored with destination identifers
		},

		"data_available_nav": ['amb_tmp','cpress','sea_st','rpm','speed'], #FUel==Nav data
		"data_available_engine": ['ext_temp1','ext_temp2','ext_temp3','sw1_pres'],
		"available_forms":[
			{
			'type':'Lub Oil Report',
			'file':'luboilreport.xls',
			},
			{
			'type':'Engine Decarbonization Report',
			'file':'enginedecarbonizationreport.xls',
			},
		],

		"identifier_mapping":{ #source:destination
			"amb_tmp":"amb_tmp",
			"cpress":"cpress",
			"sea_st":"sea_st",
			"exhtmp1":"ext_temp1",
			"exhtmp2":"ext_temp1",
			"exhtmp3":"ext_temp1",
			"rpm":"rpm",
			"speed":"speed"
		},

		"data":{
			"rpm":{
				"name":"RPM",
				"unit":"rpm",
				"category":"Distance And Time",
				"subcategory":"Distance And Time",
				"limits":{
					"type":"abs",
					"min":20,
					"max":120
			}
			},
			"speed":{
				"name":"Speed",
				"unit":"knots",
				"category":"Distance And Time",
				"subcategory":"Distance And Time",
				"limits":{
					"type":"abs",
					"min":5,
					"max":25
			},
			}
		},
}




