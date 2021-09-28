class ConfigExtractor():

    def __init__(self,
                 ship_imo,
                 file,
                 override):
        self.ship_imo = ship_imo
        self.file = file
        self.override = override
        self.error = False
        self.traceback_msg = None

    def connect(self):
        pass

    def do_steps(self):
        "defining steps to call each funtion one by one"

    def read_files(self):
        "reading config xl file"

    def stat(self,s):
        "creating dictioonary of static data"

    def process_file(self):
        "processing configuratoir file"
    
    def write_configs(self):
        "writing in mongodb(shipconfigs)"  #done till here

    def within_outlier_limits(self,identifier,identifier_value,formula_var):       #formula_var=apiformulas in limits value of variable required by limits
        "true false based on outlier input"
    
    def within_operational_limits(self,identifier,identifier_value):
        "true false based on operational limits"