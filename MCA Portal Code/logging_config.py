import os
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime


def setup_logging():
    log_directory = r"C:\Users\mns-admin\Documents\Python\Logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    log_filename = os.path.join(log_directory, datetime.datetime.now().strftime("%d-%m-%Y.log"))

    # Configure logging to both console and file
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.StreamHandler(),  # Log to console
                            TimedRotatingFileHandler(log_filename, when="midnight", backupCount=7)  # Log to file
                        ])
