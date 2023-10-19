from Config import create_main_config_dictionary
from DownloadFile import insert_Download_Details,download_documents,update_form_extraction_status
from PDFToXML import PDFtoXML,CheckHiddenAttachemnts,fetch_form_extraction_file_data_from_table
from MCAPortalLogin import login_to_website
from XMLToDB import xml_to_excel
from DBFunctions import connect_to_database
from DBFunctions import fetch_order_data_from_table
from DBFunctions import update_status
from DBFunctions import update_logout_status
from DBFunctions import fetch_user_credentials_from_db
from DBFunctions import get_db_credentials
from DBFunctions import update_xml_extraction_status
from DBFunctions import get_xml_to_insert
import os


def sign_out(driver):
    try:
        sign_out_button = driver.find_element(By.XPATH, '//a[@id="loginAnchor" and text()="Signout"]')

        if sign_out_button.is_displayed():
            sign_out_button.click()
            print("Signed Out")

        if 'driver' in locals():
            driver.delete_all_cookies()
            driver.quit()

    except Exception as e:
        print(f"Error Signing out {e}")
        if 'driver' in locals():
            driver.delete_all_cookies()
            driver.quit()

def Login_and_Download(config_dict,CinData):
    try:
        root_path = config_dict['Root path']
        if not os.path.exists(root_path):
            os.makedirs(root_path)
        retry_count = config_dict['Retry Count']
        chrome_driver_path = config_dict['chrome_driver_path']
        Url = config_dict['Url']
        Cin, CompanyName, User = CinData[2], CinData[3], CinData[15]
        db_config = get_db_credentials(config_dict)
        last_logged_in_user = None
        if last_logged_in_user is None or last_logged_in_user != User:
            username, password, Status = fetch_user_credentials_from_db(db_config, User)
            if Status == "Pass":
                Login, driver = login_to_website(Url, chrome_driver_path, username, password, db_config)
            else:
                print("Already Logged in")
                # update_status(User,"Exception",db_config)
                return False,None
            print(Login)
            if Login == "Pass":
                print("Successfully Logged in")
                last_logged_in_user = User
            else:
                update_status(User, "Login Failed", db_config)
                return False,None
        else:
            print("Already Logged in so carrying on with the same credentials")
        category_list = ['Annual Returns and Balance Sheet eForms']
        for item in category_list:
            try:
                download_status = insert_Download_Details(driver, Cin, CompanyName, db_config, item)
                if download_status:
                    file_download = download_documents(driver, db_config, Cin, CompanyName, item, root_path)
                else:
                    continue
                if file_download:
                    print(f"Downloaded for {item} ")
                else:
                    continue
            except Exception as e:
                print(f"Exception Occured {e}")
                continue
        return True,driver
    except Exception as e:
        print(f"Exception Occured {e}")
        return False,None

def XMLGeneration(db_config,CinData,config_dict):
    try:
        connection,cursor = connect_to_database(db_config)
        Cin, CompanyName, User = CinData[2], CinData[3], CinData[15]
        print(Cin)
        print(CompanyName)
        root_path = config_dict['Root path']
        update_extraction_status = update_form_extraction_status(db_config, Cin, CompanyName)
        if update_extraction_status:
            files_to_be_extracted, Fetch_File_Data_Status = fetch_form_extraction_file_data_from_table(connection, Cin,CompanyName)
            if Fetch_File_Data_Status == "Pass":
                for files in files_to_be_extracted:
                    try:
                        pdf_path = files[8]
                        file_name = files[4]
                        folder_path = os.path.dirname(pdf_path)
                        xml_file_path, PDF_to_XML = PDFtoXML(pdf_path, file_name)
                        if PDF_to_XML:
                            update_xml_extraction_status(Cin,file_name,config_dict,'Success')
                            hidden_attachments = CheckHiddenAttachemnts(xml_file_path, folder_path, pdf_path, file_name)
                        else:
                            update_xml_extraction_status(Cin,file_name,config_dict,'Failure')
                            continue
                    except Exception as e:
                        print(f"Exception Occured while converting to xml{e}")
                        continue
                return True,hidden_attachments
        else:
            print("Error in updating data base")
            return False, []
    except Exception as e:
        print(f"Exception Occured {e}")
        return False,[]

def insert_fields_into_db(hiddenattachmentslist,config_dict,CinData):
    try:
        db_config = get_db_credentials(config_dict)
        connection, db_cursor = connect_to_database(db_config)
        connection.autocommit = True
        Cin, CompanyName, User = CinData[2], CinData[3], CinData[15]
        if len(hiddenattachmentslist) != 0:
            Subsidiary_Config = config_dict['Subsidiary_Config']
            Business_Activity_Config = config_dict['Business_Activity_Config']
            Subsidiary_Config_Sheet_Name = config_dict['Subsidiary_Config_Sheet_Name']
            Business_Activity_Config_Sheet_Name = config_dict['Business_Activity_Config_Sheet_Name']
            for hiddenattachment in hiddenattachmentslist:
                if "Subsidiaries" in hiddenattachment or "Holding" in hiddenattachment or "Associate" in hiddenattachment or "Joint Venture" in hiddenattachment:
                    map_file_path = Subsidiary_Config
                    map_file_sheet_name = Subsidiary_Config_Sheet_Name
                elif "Business Activity" in hiddenattachment:
                    map_file_path = Business_Activity_Config
                    map_file_sheet_name = Business_Activity_Config_Sheet_Name
                else:
                    pass
                output_excel_name = str(hiddenattachment).replace('.xml', '.xlsx')
                folder_path = os.path.dirname(hiddenattachment)
                output_excel_path = os.path.join(folder_path, output_excel_name)
                xml_to_excel(db_cursor, config_dict, map_file_path, map_file_sheet_name, hiddenattachment,
                             output_excel_path, Cin, CompanyName)

        xml_files_to_insert = get_xml_to_insert(Cin, config_dict)
        for xml in xml_files_to_insert:
            try:
                path = xml[8]
                map_file_path = config_dict['map_file_path']
                map_file_sheet_name = config_dict['map_file_sheet_name']
                xml_file_path = str(path).replace('.pdf', '.xml')
                output_excel_path = str(path).replace('.pdf', '.xlsx')
                xml_to_excel(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,output_excel_path, Cin, CompanyName)
            except Exception as e:
                print(f"Exception occured while inserting into DB {e}")
                continue
        return True
    except Exception as e:
        print(f"Exception Occured while inserting for hidden attachments {e}")
        return False









