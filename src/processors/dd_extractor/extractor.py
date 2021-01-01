from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.helpers.check_status import check_status
from src.db.schema.ship import Ship # importing ship config schema
from src.db.schema.ddschema import DailyData  # importing dd schema
from mongoengine import *
from datetime import datetime

log = CommonLogger(__name__, debug=True).setup_logger()


class Extractor:

    def __init__(self,
                 ship_imo,
                 date,
                 type,
                 file,
                 override):
        self.ship_imo = ship_imo
        self.df = file
        self.type = str(type)
        self.date = datetime(date)
        self.override = override
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
            return True, str(insertedId)

    @check_status
    def connect_db(self):
        self.database = connect_db()
        pass

    @check_status
    def get_ship_configs(self):

        for config in self.database.ship_config.find(
                {"ship_imo": self.ship_imo}):  # Stores document from ship_config having same IMO passed in API
            break
        self.ship_name = config["ship_name"]
        self.identifier_mapping = config["identifier_mapping"]

        # 1. Map config nav headers to dd headers
        if self.type == "fuel":
            self.nav_configtoDDh = []  # To store data_available_nav from config collection in dd header format
            nav_config = list(config["data_available_nav"])  # extracts list of nav headers from Ship config MongoDB
            for k, v in self.identifier_mapping.items():  # k is dd header, v is config header
                if v in nav_config:
                    self.nav_configtoDDh.append(
                        k)  # Stores data_available_nav from config collection in dd header format

        # 2. Map config engine headers to dd headers
        if self.type == "engine":
            self.engine_configtoDDh = []  # To store data_available_engine from config collection in dd header format
            engine_config = config["data_available_engine"]  # extracts list of engine headers from Ship config MongoDB
            for k, v in self.identifier_mapping.items():  # v is config header, k is dd header
                if v in engine_config:
                    self.engine_configtoDDh.append(k)

        pass

    @check_status
    def process(self):

        self.document = {}

        # self.document['historical']
        # self.document["nav_data_details"] = {{"upload_datetime": "Date(2016-05-18T16:00:00Z)", "file_name": "daily_data19June20.xlsx","file_url": "aws.s3.xyz.com", "uploader_details": {"userid": "xyz", "company": "sdf"}}}
        # self.document["engine_data_details"] =  {{"upload_datetime": "Date(2016-05-18T16:00:00Z)","file_name": "daily_data19June20engine.xlsx", "file_url": "aws.s3.xyz.com", "uploader_details": {"userid": "xyz", "company": "sdf"}}}

        if self.database.daily_data.count_documents({"ship_imo": self.ship_imo, "date": self.date}) == 0:  # True if a record of ship does not exist DD. Insertion is performed.
            self.override = True
            self.document = {"ship_imo": self.ship_imo, "ship_name": self.ship_name, "date": self.date,
                             "engine_data_available": False, "nav_data_available": False, "data_available_engine": [],
                             'data': {}}
        else:  # To perform updation if record already exists
            for self.dd in self.database.daily_data.find({"ship_imo": self.ship_imo, "date": self.date}):

                if self.type == "fuel":
                    if self.override == False and self.dd["nav_data_available"] == True:
                        return  # If Override is false and nav/fuel data exists, aborts the function. self.override = False

                    self.override = True
                    data = self.dd["data_available_engine"]
                    self.document['data'] = dict((k, self.dd['data'][k]) for k in data if k in self.dd['data'])  # If nav data is to be updated/replaced, this line is to preserve the engine data of the ship.
                    # self.document["nav_data_available"] = False
                    break

                if self.type == "engine":
                    if self.override == False and self.dd["engine_data_available"] == True:
                        return  # If Override is false and engine data exists, aborts the function. self.override = False

                    self.override = True
                    data = self.dd["data_available_nav"]
                    self.document['data'] = dict((k, self.dd['data'][k]) for k in data if k in self.dd['data'])  # If engine data is to be updated/replaced, this line is to preserve the nav data of the ship.
                    # self.document["engine_data_available"] = False
                    break

        # Runs irrespective of insertion or updation
        if self.type == "fuel":
            self.document["data_available_nav"] = []
            for k in self.nav_configtoDDh:
                if k in list(self.df.columns):
                    self.document["data_available_nav"].append(k)
            if len(self.document["data_available_nav"]) != 0:
                self.document["nav_data_available"] = True
                self.document["data"].update(self.df[self.document["data_available_nav"]].to_dict(orient='records')[0])

        if self.type == "engine":
            self.document["data_available_engine"] = []
            for k in self.engine_configtoDDh:
                if k in list(self.df.columns):
                    self.document["data_available_engine"].append(k)
            if len(self.document["data_available_engine"]) != 0:
                self.document["engine_data_available"] = True
                self.document["data"].update(
                    self.df[self.document["data_available_engine"]].to_dict(orient='records')[0])

        return print(self.document)  # Prints the Dictionary created of only the records which need to be inserted/updated.

    @check_status
    def write_dd(self):
        if self.override == True:
            self.database.daily_data.update_one({"ship_imo": self.ship_imo, "date": self.date}, {'$set': self.document},upsert=True)
            return print('Insertion Successful')
        else:
            return print('Insertion aborted as record already exists')


# run = Extractor()
# run.do_steps()
