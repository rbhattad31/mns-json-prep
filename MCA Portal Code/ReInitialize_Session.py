from MCAPortalLogin import login_to_website
from DownloadFile import Navigate_to_Company
import logging
from logging_config import setup_logging


def session_restart(Url, chrome_driver_path, username, password, db_config,Cin,CompanyName):
    setup_logging()
    try:
        Login, driver, options, exception_message = login_to_website(Url, chrome_driver_path, username, password,
                                                                     db_config)
        if Login == "Pass":
            Navigation = Navigate_to_Company(Cin, CompanyName, driver, db_config)
            if Navigation:
                logging.info(f"Navigated successfully to {CompanyName}")
    except Exception as e:
        logging.error(f"Exception {e} occurred while reinstalling session")
        return False
    else:
        return True

