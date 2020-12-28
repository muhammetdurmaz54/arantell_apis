from mongoengine import *
import datetime

class Ship(Document):
    ship_imo = IntField(max_length = 7)
    ship_name = StringField()
    ship_description = StringField()
    added_on = DateTimeField(default = datetime.datetime.utcnow())
    static_data = DictField()
    data_available_nav = ListField()
    data_available_engine = ListField()
    identifier_mapping = DictField()
    data = DictField()