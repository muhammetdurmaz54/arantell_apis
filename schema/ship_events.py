"""
Events of Ship like Dry Docking or Cleaning

"""
import datetime

document = [{
        "ship_imo":9591301, # taken from sheet 1
        "date_added": datetime.datetime(2020, 5, 17),
        "event_type":"global",
        "event":"Dry Dock",
        "start_date":datetime.datetime(2020, 5, 17),
        "end_date":datetime.datetime(2020, 5, 17),
        "event_data":"Additional strings here"
},

{
        "ship_imo":9591301, # taken from sheet 1
        "date_added": datetime.datetime(2020, 5, 17),
        "event_type":"local",
        "event":"Air Cooler Cleaning",
        "start_date":datetime.datetime(2020, 5, 17),
        "end_date":datetime.datetime(2020, 5, 17),
        "event_data":"Additional strings here"
},
]