from mongoengine import *
from datetime import datetime

class Main_db(Document):
    ship_imo= IntField(max_length = 7)
    date= DateTimeField(default = datetime.utcnow())
    historical= BooleanField()
    daily_data= DictField()
    weather_api= DictField()
    position_api= DictField()
    indices= DictField()
    faults= DictField()
    health_status= DictField()
