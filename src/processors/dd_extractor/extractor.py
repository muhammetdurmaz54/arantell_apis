from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.helpers.check_status import check_status
from src.db.schema.ship import Ship  # importing ship config schema
from src.db.schema.ddschema import DailyData  # importing dd schema

log = CommonLogger(__name__, debug=True).setup_logger()


class Extractor:

    def __init__(self,
                 ship_imo,
                 date,
                 type,
                 file):
        self.ship_imo = ship_imo
        self.df = file
        self.type = type
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

        for i in Ship.objects():  # Iterates over each MongoDB document of Ship_config MongoDB Collection
            if i.ship_imo == self.ship_imo:  # Checks if imo passed in API hit exists in Ship_config MongoDb.
                self.ship_name = i.ship_name
                self.identifier_mapping = i.identifier_mapping

                # 1. Map config nav headers to dd headers
                if self.type == "fuel":
                    self.nav_configtoDDh = []  # To store data_available_nav from config collection in dd header format
                    nav_config = list(i.data_available_nav)  # extracts list of nav headers from Ship config MongoDB
                    for k, v in self.identifier_mapping.items():  # k is dd header, v is config header
                        if v in nav_config:
                            self.nav_configtoDDh.append(k)  # Stores data_available_nav from config collection in dd header format

                # 2. Map config engine headers to dd headers
                if self.type == "engine":
                    self.engine_configtoDDh = []  # To store data_available_engine from config collection in dd header format
                    engine_config = list(i.data_available_engine)  # extracts list of engine headers from Ship config MongoDB
                    for k, v in self.identifier_mapping.items():  # v is config header, k is dd header
                        if v in engine_config:
                            self.engine_configtoDDh.append(k)
            else:
                return "There is no ship with IMO ", self.ship_imo, " in the Ship Config MongoDB collection"
        pass

    @check_status
    def process(self):

        self.data = {}

        if self.type == "fuel":
            self.data_available_nav = []
            for k in self.navconfig_DDh:
                if k in list(self.df.columns):
                    self.data_available_nav.append(k)
            if len(self.data_available_nav) != 0:
                self.nav_data_available = True
                self.data.update(self.df[self.data_available_nav].to_dict(orient='records')[0])

        if self.type == "engine":
            self.data_available_engine = []
            for k in self.engine_configtoDDh:
                if k in list(self.df.columns):
                    self.data_available_engine.append(k)
            if len(self.data_available_engine) != 0:
                self.engine_data_available = True
                self.data.update(self.df[self.data_available_engine].to_dict(orient='records')[0])

        self.nav_data_details = {"upload_datetime": "Date(2016-05-18T16:00:00Z)",
                                 "file_name": "daily_data19June20.xlsx",
                                 "file_url": "aws.s3.xyz.com",
                                 "uploader_details": {"userid": "xyz", "company": "sdf"}}

        self.engine_data_details = {"upload_datetime": "Date(2016-05-18T16:00:00Z)",
                                    "file_name": "daily_data19June20engine.xlsx",
                                    "file_url": "aws.s3.xyz.com",
                                    "uploader_details": {"userid": "xyz", "company": "sdf"}}
        pass

    @check_status
    def write_dd(self):

        ship1 = DailyData()

        ship1.ship_imo = self.ship_imo  # Retrieves the imo no from config file
        ship1.ship_name = self.ship_name
        ship1.engine_data_details = self.engine_data_details
        ship1.nav_data_details = self.nav_data_details
        # ship1.date = ,
        # ship1.historical = ,

        if self.type == "fuel":
            ship1.nav_data_available = self.nav_data_available
            ship1.data_available_nav = self.data_available_nav

        if self.type == "engine":
            ship1.engine_data_available = self.engine_data_available
            ship1.data_available_engine = self.data_available_engine

        ship1.data = self.data

        ship1.save()
        pass

# run = Extractor()
# run.do_steps()
