from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from mongoengine import *
from src.helpers import check_status
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from shipconfig_schema import Ship_config #importing ship config schema

log = CommonLogger(__name__, debug=True).setup_logger()


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


class Extractor:

    def __init__(self,
                 ship_imo,
                 date,
                 type,
                 file):
        self.ship_imo = ship_imo
        # self.enginedataframe
        # self.navdataframe
        pass

    def do_steps(self):
        self.connect_db()
        self.get_ship_configs()
        self.process()
        self.write_dd()
        insertedId = self.write_dd()
        if self.error:
            return False, str(self.traceback_msg)
        else:
            return True, str(inserted_id)

    @check_status
    def connect_db(self):
        connect('dbname')
        pass

    @check_status
    def get_ship_configs(self):
        self.data_avail_navdd = []  # Stores data_available_nav from config collection in dd header format
        self.data_avail_enginedd = []  # Stores data_available_engine from config collection in dd header format
        for i in "Ship_schema_class".objects():
            if i.ship_imo == self.ship_imo:  # Checks if imo passed in POST exists in config db.
                self.ship_name = i.ship_name
                self.identifier_mapping = i.identifier_mapping

                # 3.1. Map config nav headers to dd headers
                data_available_nav_config = list(i.data_available_nav)  # list of avail nav headers in config file
                for k, v in self.identifier_mapping.items():
                    if v in data_available_nav_config:
                        self.data_avail_navdd.append(k)
                # 3.2. Map config engine headers to dd headers
                data_available_engine_config = list(
                    i.data_available_engine)  # list of avail engine headers in config file
                for k, v in self.identifier_mapping.items():
                    if v in data_available_engine_config:
                        self.data_avail_enginedd.append(k)
        pass

    @check_status
    def process(self):
        # creating list of available engine headers in dd from mapped config headers in 3.1
        data_avail_engine_dd = []
        for k in self.data_avail_enginedd:
            if k in list("self.engine_data".columns):
                data_avail_engine_dd.append(k)

        # creating list of available nav headers in dd from mapped config headers in 3.2
        data_avail_nav_dd = []
        for k in self.data_avail_navdd:
            if k in list("self.nav_data".columns):
                data_avail_nav_dd.append(k)

        self.document = {'ship_imo': self.ship_imo, 'vsl_name': self.ship_name, 'engine_data_available': False,
                         'nav_data_available': False,
                         "nav_data_details": {"upload_datetime": "Date(2016-05-18T16:00:00Z)",
                                              "file_name": "daily_data19June20.xlsx",
                                              "file_url": "aws.s3.xyz.com",
                                              "uploader_details": {"userid": "xyz", "company": "sdf"}},
                         "engine_data_details": {
                             "upload_datetime": "Date(2016-05-18T16:00:00Z)",
                             "file_name": "daily_data19June20engine.xlsx",
                             "file_url": "aws.s3.xyz.com",
                             "uploader_details": {"userid": "xyz", "company": "sdf"}}, 'data': {}}

        if len(data_avail_engine_dd) != 0:
            self.document['engine_data_available'] = True
            self.document['data_available_engine'] = data_avail_engine_dd
            self.document['data'].update("self.engine_data"[data_avail_engine_dd].to_dict(orient='records')[0])

        if len(data_avail_nav_dd) != 0:
            self.document['nav_data_available'] = True
            self.document['data_available_nav'] = data_avail_nav_dd
            self.document['data'].update("self.nav_data"[data_avail_nav_dd].to_dict(orient='records')[0])
        pass

    @check_status
    def write_dd(self):
        ship1 = DailyData()

        ship1.ship_imo = self.document['ship_imo']  # Retrieves the imo no from config file
        ship1.ship_name = self.document['vsl_name']
        ship1.engine_data_details = self.document["engine_data_details"]
        ship1.nav_data_details = self.document["nav_data_details"]
        # ship1.date = ,
        # ship1.historical = ,
        ship1.nav_data_available = self.document['nav_data_available']
        ship1.engine_data_available = self.document['engine_data_available']

        if self.document['engine_data_available']:
            ship1.data_available_engine = self.document['data_available_engine']

        if self.document['nav_data_available']:
            ship1.data_available_nav = self.document['data_available_nav']

        ship1.data = self.document['data']

        ship1.save()
        pass


#run = Extractor()
#run.do_steps()
