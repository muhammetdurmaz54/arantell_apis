from mongoengine import *
import datetime


class DailyData(Document):
    ship_imo = IntField(max_length=7)
    ship_name = StringField()
    date = DateTimeField()
    historical = BooleanField()
    nav_data_available = BooleanField()
    engine_data_available = BooleanField()
    nav_data_details = DictField()
    engine_data_details = DictField()
    data_available_nav = ListField()
    data_available_engine = ListField()
    data = DictField()