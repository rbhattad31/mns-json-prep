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
from DownloadFile import Navigate_to_Company
import os
import mysql.connector
from selenium.webdriver.common.by import By
from MSME_XMLToDB import msme_xml_to_db
from AOC_XMLtoDB import AOC_xml_to_db
from ChangeOfName_XMLtoDB import ChangeOfName_xml_to_db
def sign_out(driver,config_dict,CinData):
    try:
        sign_out_button = driver.find_element(By.XPATH, '//a[@id="loginAnchor" and text()="Signout"]')

        if sign_out_button.is_displayed():
            sign_out_button.click()
            print("Signed Out")

        if 'driver' in locals():
            driver.delete_all_cookies()
            driver.quit()
        db_config = get_db_credentials(config_dict)
        username = CinData[15]
        update_logout_status(db_config,username)
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
                Login, driver,options = login_to_website(Url, chrome_driver_path, username, password, db_config)
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
                Navigation = Navigate_to_Company(Cin,CompanyName,item,driver,db_config)
                if Navigation:
                    download_status = insert_Download_Details(driver, Cin, CompanyName, db_config, item)
                    Download_Status = "Pass"
                else:
                    Download_Status = "Fail"
                    return False,None
                if download_status:
                    update_extraction_status = update_form_extraction_status(db_config, Cin, CompanyName)
                    Download_Status = "Pass"
                else:
                    Download_Status = "Fail"
                    continue
                if update_extraction_status:
                    file_download = download_documents(driver, db_config, Cin, CompanyName, item, root_path,options)
                    Download_Status = "Pass"
                else:
                    Download_Status = "Fail"
                    continue
                if file_download:
                    Download_Status = "Pass"
                    print(f"Downloaded for {item} ")
                else:
                    Download_Status = "Fail"
                    continue
            except Exception as e:
                print(f"Exception Occured {e}")
                continue
        if Download_Status == "Pass":
            return True,driver
        else:
            return False,driver
    except Exception as e:
        print(f"Exception Occured {e}")
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        Check_download_files_query = "select * from documents where cin=%s and company=%s and Download_Status='Downloaded' and form_data_extraction_needed='Y'"
        Download_Check_Values = (Cin, CompanyName)
        print(Check_download_files_query % Download_Check_Values)
        cursor.execute(Check_download_files_query, Download_Check_Values)
        Downloaded_Files = cursor.fetchall()
        cursor.close()
        connection.close()
        if len(Downloaded_Files) > 0:
            return True, driver
        else:
            return False, None

def XMLGeneration(db_config,CinData,config_dict):
    try:
        connection,cursor = connect_to_database(db_config)
        Cin, CompanyName, User = CinData[2], CinData[3], CinData[15]
        print(Cin)
        print(CompanyName)
        update_extraction_status = update_form_extraction_status(db_config, Cin, CompanyName)
        root_path = config_dict['Root path']
        files_to_be_extracted, Fetch_File_Data_Status = fetch_form_extraction_file_data_from_table(connection, Cin,CompanyName)
        if Fetch_File_Data_Status == "Pass":
            hidden_attachments = []
            for files in files_to_be_extracted:
                try:
                    pdf_path = files[8]
                    file_name = files[4]
                    folder_path = os.path.dirname(pdf_path)
                    xml_file_path, PDF_to_XML = PDFtoXML(pdf_path, file_name)
                    if PDF_to_XML:
                        update_xml_extraction_status(Cin, file_name, config_dict, 'Success')
                        hidden_attachments = CheckHiddenAttachemnts(xml_file_path, folder_path, pdf_path, file_name)
                    else:
                        update_xml_extraction_status(Cin, file_name, config_dict, 'Failure')
                        continue
                except Exception as e:
                    print(f"Exception Occured while converting to xml{e}")
                    continue
    except Exception as e:
        print(f"Exception Occured {e}")
        return False,[]
    else:
        return True, hidden_attachments

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
                xml_file_path = str(path).replace('.pdf', '.xml')
                output_excel_path = str(path).replace('.pdf', '.xlsx')
                if 'MGT'.lower() in str(path).lower():
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "MGT"
                    config_dict_MGT, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    map_file_path = config_dict_MGT['map_file_path']
                    map_file_sheet_name = config_dict_MGT['map_file_sheet_name']
                    xml_to_excel(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,output_excel_path, Cin, CompanyName)
                elif 'MSME'.lower() in str(path).lower():
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "MSME"
                    config_dict_MSME, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    map_file_path_MSME = config_dict_MSME['mapping_file_path']
                    map_sheet_name_MSME = config_dict_MSME['mapping _file_sheet_name']
                    print("Inserting MSME to DB")
                    msme_xml_to_db(db_cursor,config_dict_MSME,map_file_path_MSME,map_sheet_name_MSME,xml_file_path,output_excel_path,Cin,CompanyName)
                elif 'AOC-4'.lower() in str(path).lower():
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "AOC"
                    config_dict_AOC, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    map_file_path_AOC = config_dict_AOC['mapping_file_path']
                    map_sheet_name_AOC = config_dict_AOC['mapping _file_sheet_name']
                    AOC_xml_to_db(db_config,config_dict_AOC,map_file_path_AOC,map_sheet_name_AOC,xml_file_path,output_excel_path,Cin,CompanyName)
                elif 'CHANGE OF NAME'.lower() in str(path).lower():
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "Change of name"
                    config_dict_Change_of_name,config_status = create_main_config_dictionary(excel_file,Sheet_name)
                    map_file_path_Change_of_name = config_dict_Change_of_name['mapping_file_path']
                    map_sheet_name_Change_of_name = config_dict_Change_of_name['mapping _file_sheet_name']
                    ChangeOfName_xml_to_db(db_config,config_dict_Change_of_name,map_file_path_Change_of_name,map_sheet_name_Change_of_name,xml_file_path,output_excel_path,Cin,CompanyName)
            except Exception as e:
                print(f"Exception occured while inserting into DB {e}")
                continue
    except Exception as e:
        print(f"Exception Occured while inserting for hidden attachments {e}")
        return False
    else:
        return True









