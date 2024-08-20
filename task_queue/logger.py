import logging
import sys
import datetime
import os

def create_logger(module_name: str, local_output_file: str, logger_level=logging.DEBUG):
    logger = logging.getLogger(module_name)
    file_handler = logging.FileHandler(local_output_file)
    stream_handler = logging.StreamHandler(sys.stdout)
    # Add formatting to the statements
    fmt = logging.Formatter(fmt="%(asctime)s [%(levelname)s]: %(message)s")
    stream_handler.setFormatter(fmt)
    file_handler.setFormatter(fmt)
    # Add handlers to logger
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    # set log level
    logger.setLevel(logger_level)
    return logger


def get_log_fp():
    log = str(datetime.datetime.now())
    log = log.replace(" ", "_")
    log = log.replace(":", "_")
    log = log.split(".")[0]
    log = log.replace("-", "_")
    log = log + ".log"

    log_dir = os.getcwd() + "/logs"
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)
    return os.path.join(log_dir, log)


# logger_level = sys.argv
# logger = create_logger(__name__, get_log_fp(), logger_level)

print(sys.argv)
