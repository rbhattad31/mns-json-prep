import traceback
import os
import datetime
from DBFunctions import connect_to_database
from DBFunctions import fetch_order_data_from_table
from DBFunctions import get_db_credentials
from Config import create_main_config_dictionary
from MCAPortalMainFunctions import Login_and_Download
from MCAPortalMainFunctions import XMLGeneration
from MCAPortalMainFunctions import insert_fields_into_db
from MCAPortalMainFunctions import sign_out
from MCAPortalMainFunctions import json_loader_generation
from DBFunctions import update_json_loader_db
from SendEmail import send_email
import logging
from logging_config import setup_logging
from DBFunctions import fetch_workflow_status
from DBFunctions import update_status
from DBFunctions import update_process_status
from DBFunctions import update_locked_by_empty
import sys
import traceback
from MCAPortalMainFunctions import update_download_status
from TransactionalLog import generate_transactional_log
from DBFunctions import fetch_download_status
from DBFunctions import fetch_order_download_data_from_table
from DBFunctions import update_locked_by
from DBFunctions import update_modified_date
from DBFunctions import update_retry_count
from DBFunctions import get_retry_count
from MCAPortalMainFunctions import update_completed_status_api
from FinalEmailTable import FinalTable
from FinancialsTable import financials_table

def main():
    excel_file = os.environ.get("MCA_Config")
    Sheet_name = "Sheet1"
    try:
        setup_logging()
        config_dict, config_status = create_main_config_dictionary(excel_file, Sheet_name)
        if config_status == "Pass":
            db_config = get_db_credentials(config_dict)
            next_cin = True
            while next_cin:
                connection, cursor = connect_to_database(db_config)
                download_columnnames, downloadData, downloadFetchStatus = fetch_order_download_data_from_table(connection)
                cin = None
                receipt_number = None
                company_name = None
                for downloaddata in downloadData:
                    try:
                        cin = downloaddata[2]
                        receipt_number = downloaddata[1]
                        user = downloaddata[15]
                        company_name = downloaddata[3]
                        workflow_status = downloaddata[5]
                        download_status = downloaddata[66]
                        logging.info(workflow_status)
                        emails = config_dict['to_email']
                        emails = str(emails).split(',')
                        if (workflow_status == 'Payment_success' or workflow_status == 'XML_Pending') and download_status == 'N':
                            logging.info(f"Starting to download for {cin}")
                            update_locked_by(db_config, cin)
                            subject_start = str(config_dict['subject_start']).format(cin, receipt_number)
                            body_start = str(config_dict['Body_start']).format(cin, receipt_number, company_name)
                            try:
                                send_email(config_dict, subject_start, body_start, emails, None)
                            except Exception as e:
                                logging.info(f"Error sending email {e}")
                            Download_Status, driver, exception_message = Login_and_Download(config_dict, downloaddata)
                            if Download_Status:
                                logging.info("Downloaded Successfully")
                                # update_status(user,'XML_Pending',db_config,cin)
                                update_download_status(db_config, cin)
                                update_locked_by_empty(db_config,cin)
                                update_modified_date(db_config,cin)
                                try:
                                    sign_out(driver, config_dict, downloaddata)
                                except:
                                    pass
                            else:
                                retry_counter_db = get_retry_count(db_config, cin)
                                if retry_counter_db is None:
                                    retry_counter_db = 0
                                try:
                                    retry_counter_db = int(retry_counter_db)
                                except:
                                    pass
                                retry_counter_db = retry_counter_db + 1
                                logging.info("Not Downloaded")
                                update_locked_by_empty(db_config, cin)
                                update_modified_date(db_config, cin)
                                update_retry_count(db_config,cin,retry_counter_db)
                                if retry_counter_db > 3:
                                    update_process_status('Exception',db_config,cin)
                                try:
                                    sign_out(driver, config_dict, downloaddata)
                                except:
                                    pass
                                raise Exception(f"Download failed for {cin} {exception_message}")
                    except Exception as e:
                        logging.info(f"Error in downloading{e}")
                        update_locked_by_empty(db_config,cin)
                        update_modified_date(db_config,cin)
                connection, cursor = connect_to_database(db_config)
                columnnames,CinDBData , CinFetchStatus = fetch_order_data_from_table(connection)
                if len(CinDBData) < 1:
                    next_cin = False
                    continue
                if CinFetchStatus == "Pass":
                    cin = None
                    receipt_number = None
                    company_name = None
                    hidden_attachments = []
                    emails = []
                    for CinData in CinDBData:
                        try:
                            cin = CinData[2]
                            receipt_number = CinData[1]
                            user = CinData[15]
                            company_name = CinData[3]
                            workflow_status = CinData[5]
                            download_status = CinData[66]
                            logging.info(workflow_status)
                            emails = config_dict['to_email']
                            emails = str(emails).split(',')
                            if workflow_status == 'XML_Pending' and download_status == 'Y':
                                update_locked_by(db_config, cin)
                                XML_Generation, hidden_attachments = XMLGeneration(db_config, CinData, config_dict)
                                if XML_Generation:
                                    logging.info("XML Generated successfully")
                                    update_status(user,'db_insertion_pending',db_config,cin)
                                    update_locked_by_empty(db_config,cin)
                                else:
                                    update_modified_date(db_config,cin)
                                    update_locked_by_empty(db_config,cin)
                                    logging.info("XML Not Generated successfully")
                                    if 'driver' in locals():
                                        sign_out(driver, config_dict, CinData)
                                    continue
                            if workflow_status == 'db_insertion_pending':
                                update_locked_by(db_config, cin)
                                Insert_fields_into_DB,exception_message_db = insert_fields_into_db(hidden_attachments, config_dict, CinData,excel_file)
                                if Insert_fields_into_DB:
                                    logging.info("Successfully Inserted into DB")
                                    update_status(user,'Loader_pending',db_config,cin)
                                    update_locked_by_empty(db_config,cin)
                                else:
                                    update_modified_date(db_config,cin)
                                    logging.info("Not Successfully Inserted into DB")
                                    update_locked_by_empty(db_config,cin)
                                    raise Exception(exception_message_db)
                            if workflow_status == 'Loader_pending':
                                update_locked_by(db_config, cin)
                                json_loader,json_file_path,exception_message = json_loader_generation(CinData, db_config, config_dict,excel_file)
                                if json_loader:
                                    logging.info("JSON Loader generated succesfully")
                                    update_json_loader_db(CinData, config_dict)
                                    cin_complete_subject = str(config_dict['cin_Completed_subject']).format(cin,receipt_number)
                                    table = FinalTable(db_config,cin)
                                    financials_Table = financials_table(db_config,cin)
                                    cin_completed_body = str(config_dict['cin_Completed_body']).format(cin,receipt_number,company_name,table,financials_Table)
                                    update_process_status('Completed',db_config,cin)
                                    update_locked_by_empty(db_config,cin)
                                    config_transactional_log_path = config_dict['config_transactional_log_path']
                                    root_path = config_dict['Root path']
                                    transaction_log_path = generate_transactional_log(db_config,config_transactional_log_path,root_path)
                                    update_api = update_completed_status_api(receipt_number,config_dict)
                                    if update_api:
                                        logging.info(f"Updated successfully in API for {receipt_number}")
                                    else:
                                        logging.info(f"Not Updated successfully in API for {receipt_number}")
                                    try:
                                        attachments = []
                                        attachments.append(json_file_path)
                                        attachments.append(transaction_log_path)
                                        emails_end = config_dict['end_email']
                                        emails_end = str(emails_end).split(',')
                                        send_email(config_dict,cin_complete_subject,cin_completed_body,emails_end,attachments)
                                    except Exception as e:
                                        logging.info(f"Exception occured while sending end email {e}")
                                else:
                                    update_modified_date(db_config,cin)
                                    logging.info("JSON Loader not generated")
                                    update_locked_by_empty(db_config,cin)
                                    raise Exception(f"Exception occured for json loader generation {cin} {exception_message}")
                        except Exception as e:
                            retry_counter_db = get_retry_count(db_config, cin)
                            if retry_counter_db is None:
                                retry_counter_db = 0
                            try:
                                retry_counter_db = int(retry_counter_db)
                            except:
                                pass
                            retry_counter_db = retry_counter_db + 1
                            update_modified_date(db_config,cin)
                            update_retry_count(db_config, cin, retry_counter_db)
                            if retry_counter_db > 3:
                                update_process_status('Exception', db_config, cin)
                            logging.info(f"Exception occured for cin {cin} {e}")
                            update_locked_by_empty(db_config, cin)
                            exc_type, exc_value, exc_traceback = sys.exc_info()

                            # Get the formatted traceback as a string
                            traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

                            # logging.info the traceback details
                            for line in traceback_details:
                                logging.error(line.strip())
                            exception_subject = str(config_dict['Exception_subject']).format(cin,receipt_number)
                            exception_body = str(config_dict['Exception_message']).format(cin,receipt_number,company_name,e)
                            try:
                                if 'driver' in locals():
                                    sign_out(driver, config_dict, CinData)
                            except:
                                pass
                            try:
                                send_email(config_dict,exception_subject,exception_body,emails,None)
                            except Exception as e:
                                print(f"Error sending email {e}")
        else:
            print("Unable to fetch data")
            raise Exception("Unable to fetch data")
    except FileNotFoundError:
        print(f"Configuration file '{excel_file}' not found. Please make sure it exists.")
    except Exception as e:
        traceback.print_exc()
        print(f"An unexpected error occurred: {str(e)}")


if __name__ == "__main__":
    main()


