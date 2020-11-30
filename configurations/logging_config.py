import os
import logging
import logging.config


class CommonLogger:
    """
    Common logger for all modules

    Usage:
        from logging_config import CommonLogger
        _logger = CommonLogger(__name__,debug=True).setup_logger()
        _logger.info("Hello logger")

    Sample Output:
        INFO __main__ at 19-Dec-19,13:35:09: Hello logger
    """
    def __init__(self, module_name=None, debug=False):
        self.env = os.environ.get("ENVIRONMENT","")
        self.mod_name = module_name if module_name else __name__
        self.debug = debug
        self.logger = None
        self.log_formatter = None
      #  self.file_handler = logging.FileHandler(log_file)
        self.log_handler = logging.StreamHandler()
        self.log_format = logging.Formatter("%(levelname)s %(name)s at %(asctime)s: "
                          "%(message)s",datefmt='%d-%b-%y,%H:%M:%S')

    def setup_logger(self):
        self.logger = logging.getLogger(self.mod_name)
        self.log_handler.setFormatter(self.log_format)
      #  self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.log_handler)
        self.logger.setLevel(logging.INFO)
        if self.debug:
            self.logger.setLevel(logging.DEBUG)
        return self.logger

