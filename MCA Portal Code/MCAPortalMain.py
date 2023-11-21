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

def main():
    excel_file = r"C:\MCA Portal\Config.xlsx"
    Sheet_name = "Sheet1"
    try:
        config_dict, config_status = create_main_config_dictionary(excel_file, Sheet_name)
        if config_status == "Pass":
            db_config = get_db_credentials(config_dict)
            connection,cursor = connect_to_database(db_config)
            columnnames,CinDBData , CinFetchStatus = fetch_order_data_from_table(connection)
            if CinFetchStatus == "Pass":
                subject_start = config_dict['subject_start']
                body_start = config_dict['Body_start']
                emails = config_dict['to_email']
                emails = str(emails).split(',')
                try:
                    send_email(config_dict,subject_start,body_start,emails,None)
                except Exception as e:
                    print(f"Error sending email {e}")
                cin = None
                for CinData in CinDBData:
                    try:
                        cin = CinData[2]
                        """
                        Download_Status, driver,exception_message = Login_and_Download(config_dict, CinData)
                        if Download_Status:
                            print("Downloaded Successfully")
                        else:
                            print("Not Downloaded")
                            if 'driver' in locals():
                                sign_out(driver, config_dict, CinData)
                            raise Exception(f"Download failed for {cin} {exception_message}")
                        """
                        XML_Generation, hidden_attachments = XMLGeneration(db_config, CinData, config_dict)
                        if XML_Generation:
                            print("XML Generated successfully")
                        else:
                            print("XML Not Generated successfully")
                            """
                            if 'driver' in locals():
                                sign_out(driver, config_dict, CinData)
                            continue
                            """
                        Insert_fields_into_DB = insert_fields_into_db(hidden_attachments, config_dict, CinData)
                        if Insert_fields_into_DB:
                            print("Successfully Inserted into DB")
                        else:
                            print("Not Successfully Inserted into DB")
                            """
                            if 'driver' in locals():
                                sign_out(driver, config_dict, CinData)
                            continue
                        json_loader,json_file_path,exception_message = json_loader_generation(CinData, db_config, config_dict)
                        if json_loader:
                            print("JSON Loader generated succesfully")
                            update_json_loader_db(CinData, config_dict)
                            cin_complete_subject = str(config_dict['cin_Completed_subject']).format(cin)
                            cin_completed_body = str(config_dict['cin_Completed_body']).format(cin)
                            try:
                                send_email(config_dict,cin_complete_subject,cin_completed_body,emails,json_file_path)
                            except Exception as e:
                                print(f"Exception occured while sending end email {e}")
                        else:
                            print("JSON Loader not generated")
                            if 'driver' in locals():
                                sign_out(driver, config_dict, CinData)
                            raise Exception(f"Exception occured for json loader generation {cin} {exception_message}")
                        sign_out(driver, config_dict, CinData)
                        """
                    except Exception as e:
                        print(f"Exception occured for cin {cin} {e}")
                        exception_subject = str(config_dict['Exception_subject']).format(cin)
                        exception_body = str(config_dict['Exception_message']).format(cin,e)
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


