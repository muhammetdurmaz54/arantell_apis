from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger
from src.helpers.check_status import check_status

log = CommonLogger(__name__,debug=True).setup_logger()


class ConfigExtractor():

    def __init__(self,
                 ship_imo,
                 file):
        self.ship_imo = ship_imo
        pass


    def do_steps(self):
        self.connect()
        self.read_files()
        self.process_file()
        inserted_id = self.write_configs()
        if self.error:
            return False, str(self.traceback_msg)
        else:
            return True, str(inserted_id)


    @check_status
    def connect(self):

        pass

    @check_status
    def read_files(self):

        pass

    @check_status
    def process_file(self):

        pass

    @check_status
    def write_configs(self):

        pass