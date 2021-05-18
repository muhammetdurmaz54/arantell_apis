from mongoengine import *
from datetime import datetime

class Ship_Stats(Document):
    ship_imo= IntField(max_length = 7)
    added_on= DateTimeField()
    from_date= DateTimeField()
    to_date= DateTimeField()
    samples= IntField()
    daily_data= DictField()
    weather_api= DictField()
    position_api= DictField()
    indices= DictField()