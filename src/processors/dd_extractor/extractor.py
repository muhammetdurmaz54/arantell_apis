from src.db.setup_mongo import connect_db
from src.configurations.logging_config import CommonLogger

log = CommonLogger(__name__,debug=True).setup_logger()

def check_status(func) -> object:
    """
    Decorator for functions in class.
    Working:
        Decorator check the error flag each time before executing the function. If error present it skips function.
        Error is set using raise_error() function whenever there is error and it is neede to return to request.
        This function is outside class ad it works with self parameters.

        Only few first and last function are not applied with this decorator.

        Example: There's error in set_data which is set using raise_error. Then next functions which have this decorator will \
        first check that flag to find that there was error set, hence it will skip.


    """
    def wrapper(self, *arg, **kw):
        if self.error == False:
            try:
                res = func(self, *arg, **kw)
                log.info(f"Executed {func.__name__}")
            except Exception as e:
                res =None
                self.error = True
                self.traceback_msg = f"Error in {func.__name__}(): {e}"
                log.info(f"Error in {func.__name__}(): {e}")

        else:
            res = None
            log.info(f"Did not execute {func.__name__}")
        return res
    return wrapper


class Extractor():

    def __init__(self,
                 ship_imo,
                 date,
                 type,
                 file):
        self.ship_imo = ship_imo
        pass

    def do_steps(self):
        self.connect_db()
        self.get_schcema()
        self.get_ship_configs()
        self.process()
        insertedId = self.write_dd()
        if self.error:
            return False, str(self.traceback_msg)
        else:
            return True, str(inserted_id)

    @check_status
    def connect_db(self):
        pass

    @check_status
    def get_schema(self):
        pass

    @check_status
    def get_ship_configs(self):
        pass

    @check_status
    def process(self):
        pass

    @check_status
    def write_dd(self):
        pass
