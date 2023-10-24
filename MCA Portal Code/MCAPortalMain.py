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
                for CinData in CinDBData:
                    """
                    Download_Status,driver = Login_and_Download(config_dict,CinData)
                    if Download_Status:
                        print("Downloaded Successfully")
                    else:
                        print("Not Downloaded")
                        continue
                    """
                    XML_Generation,hidden_attachments = XMLGeneration(db_config,CinData,config_dict)
                    if XML_Generation:
                        print("XML Generated successfully")
                    else:
                        print("XML Not Generated successfully")
                        continue
                    Insert_fields_into_DB = insert_fields_into_db(hidden_attachments,config_dict,CinData)
                    if Insert_fields_into_DB:
                        print("Successfully Inserted into DB")
                    """
                    if Insert_fields_into_DB:
                        if 'driver' in locals():
                            sign_out(driver,config_dict,CinData)
                    """
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


