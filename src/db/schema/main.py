from mongoengine import *
from datetime import datetime

class Main_db(Document):
    ship_imo= IntField(max_length = 7)
    date= DateTimeField(default = datetime.utcnow())
    historical= BooleanField()
    processed_daily_data= DictField()
    within_good_voyage_limits=BooleanField()
    weather_api= DictField()
    position_api= DictField()
    indices= DictField()
    faults= DictField()
    health_status= DictField()
