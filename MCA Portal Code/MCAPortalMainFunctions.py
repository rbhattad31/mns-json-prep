from Config import create_main_config_dictionary
from DownloadFile import insert_Download_Details,download_documents,update_form_extraction_status
from PDFToXML import PDFtoXML,CheckHiddenAttachemnts,fetch_form_extraction_file_data_from_table
from MCAPortalLogin import login_to_website
from MGT_XMLToDB import mgt7_xml_to_db
from DBFunctions import connect_to_database
from DBFunctions import fetch_order_data_from_table
from DBFunctions import update_status
from DBFunctions import update_logout_status
from DBFunctions import fetch_user_credentials_from_db
from DBFunctions import get_db_credentials
from DBFunctions import update_xml_extraction_status
from DBFunctions import get_xml_to_insert
from DBFunctions import update_locked_by
from DownloadFile import Navigate_to_Company
from DownloadFile import select_category
import os
import mysql.connector
from selenium.webdriver.common.by import By
from MSME_XMLToDB import msme_xml_to_db
from AOC_XMLtoDB import AOC_xml_to_db
from ChangeOfName_XMLtoDB import ChangeOfName_xml_to_db
from CHG1_XMLToDB import chg1_xml_to_db
from AOC4_NBFC_XMLToDB import aoc_nbfc_xml_to_db
from AOC_XBRL_JSONToDB import aoc_xbrl_db_update
from TableToJson import json_generation
from AOC_XBRL_JSONToDB import AOC_XBRL_JSON_to_db
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
        update_logout_status(username,db_config)
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
        update_locked_by(db_config,Cin)
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
        Navigation = Navigate_to_Company(Cin, CompanyName, driver, db_config)
        if Navigation:
            print(f"Navigated succesfully to {CompanyName}")
        else:
            raise Exception(f"Failed to Navigate to {CompanyName}")
        category_list = ['Annual Returns and Balance Sheet eForms','Certificates','Change in Directors','Incorporation Documents','Charge Documents','LLP Forms(Conversion of company to LLP)','Other eForm Documents','Other Attachments']
        for item in category_list:
            try:
                category_selection = select_category(item,driver)
                if category_selection:
                    download_status = insert_Download_Details(driver, Cin, CompanyName, db_config, item)
                else:
                    continue
                if download_status:
                    update_extraction_status = update_form_extraction_status(db_config, Cin, CompanyName)
                else:
                    continue
                if update_extraction_status:
                    file_download = download_documents(driver, db_config, Cin, CompanyName, item, root_path,options)
                else:
                    continue
                if file_download:
                    print(f"Downloaded for {item} ")
                else:
                    continue
            except Exception as e:
                print(f"Exception Occured {e}")
                continue
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
    else:
        return True,driver

def XMLGeneration(db_config,CinData,config_dict):
    try:
        connection,cursor = connect_to_database(db_config)
        Cin, CompanyName, User = CinData[2], CinData[3], CinData[15]
        print(Cin)
        print(CompanyName)
        root_path = config_dict['Root path']
        files_to_be_extracted, Fetch_File_Data_Status = fetch_form_extraction_file_data_from_table(connection, Cin,CompanyName)
        if Fetch_File_Data_Status == "Pass":
            hidden_attachments = []
            for files in files_to_be_extracted:
                try:
                    pdf_path = files[8]
                    file_name = files[4]
                    file_date = files[5]
                    if 'XBRL document in respect Consolidated' in file_name or 'XBRL financial statements' in file_name:
                        continue
                    folder_path = os.path.dirname(pdf_path)
                    xml_file_path, PDF_to_XML = PDFtoXML(pdf_path, file_name)
                    if PDF_to_XML:
                        update_xml_extraction_status(Cin, file_name, config_dict, 'Success')
                        if 'AOC-4(XBRL)'.lower() in str(pdf_path).lower():
                            XBRL_db_update = aoc_xbrl_db_update(db_config,config_dict,Cin,CompanyName,xml_file_path,file_date)
                        hidden_attachments = CheckHiddenAttachemnts(xml_file_path, folder_path, pdf_path, file_name)
                    else:
                        update_xml_extraction_status(Cin, file_name, config_dict, 'Failure')
                        continue
                except Exception as e:
                    print(f"Exception Occured while converting to xml{e}")
                    continue
        # query_for_xbrl = "select * from documents where cin=%s and company_name=%s and (%%XBRL document in respect Consolidated%% in document or %%XBRL financial statements%% in document) and form_data_extraction_needed='Y'"
        # values_xbrl = (Cin,CompanyName)
        # print(query_for_xbrl % values_xbrl)
        # cursor.execute(query_for_xbrl,values_xbrl)
        # xbrl_files = cursor.fetchall()
        # for xbrl_file in xbrl_files:
        #     xbrl_pdf_path = xbrl_file[8]
        #     xbrl_file_name = xbrl_file[4]
        #     xbrl_file_date = xbrl_file[5]
        #     xbrl_json_path = str(xbrl_pdf_path).replace('.pdf','.json')
        #     json_generation_xbrl = json_generation(xbrl_pdf_path,xbrl_json_path)
        #     if json_generation_xbrl:
        #         update_xml_extraction_status(Cin,xbrl_file_name,config_dict,'Success')
        #     else:
        #         update_xml_extraction_status(Cin,xbrl_file_name,config_dict,'Failure')
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
            for hiddenattachment in hiddenattachmentslist:
                if "Subsidiaries" in hiddenattachment or "Holding" in hiddenattachment or "Associate" in hiddenattachment or "Joint Venture" in hiddenattachment:
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "MGT"
                    config_dict_Subs, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    Subsidiary_Config = config_dict_Subs['Subsidiary_Config']
                    Subsidiary_Config_Sheet_Name = config_dict_Subs['Subsidiary_Config_Sheet_Name']
                    map_file_path = Subsidiary_Config
                    map_file_sheet_name = Subsidiary_Config_Sheet_Name
                elif "Business Activity" in hiddenattachment:
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "MGT"
                    config_dict_Business, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    Business_Activity_Config = config_dict_Business['Business_Activity_Config']
                    Business_Activity_Config_Sheet_Name = config_dict_Business['Business_Activity_Config_Sheet_Name']
                    map_file_path = Business_Activity_Config
                    map_file_sheet_name = Business_Activity_Config_Sheet_Name
                else:
                    pass
                output_excel_name = str(hiddenattachment).replace('.xml', '.xlsx')
                folder_path = os.path.dirname(hiddenattachment)
                output_excel_path = os.path.join(folder_path, output_excel_name)
                mgt7_xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, hiddenattachment,
                             output_excel_path, Cin, CompanyName)

        xml_files_to_insert = get_xml_to_insert(Cin, config_dict)
        AOC_4_first_file_found = False
        for xml in xml_files_to_insert:
            try:
                path = xml[8]
                date = xml[5]
                xml_file_path = str(path).replace('.pdf', '.xml')
                output_excel_path = str(path).replace('.pdf', '.xlsx')
                if 'MGT'.lower() in str(path).lower():
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "MGT"
                    config_dict_MGT, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    map_file_path = config_dict_MGT['map_file_path']
                    map_file_sheet_name = config_dict_MGT['map_file_sheet_name']
                    mgt7_xml_to_db(db_cursor, config_dict_MGT, map_file_path, map_file_sheet_name, xml_file_path,output_excel_path, Cin, CompanyName)
                elif 'MSME'.lower() in str(path).lower():
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "MSME"
                    config_dict_MSME, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    map_file_path_MSME = config_dict_MSME['mapping_file_path']
                    map_sheet_name_MSME = config_dict_MSME['mapping _file_sheet_name']
                    print("Inserting MSME to DB")
                    msme_xml_to_db(db_cursor,config_dict_MSME,map_file_path_MSME,map_sheet_name_MSME,xml_file_path,output_excel_path,Cin,CompanyName)
                elif 'AOC-4'.lower() in str(path).lower():
                    if 'AOC-4-NBFC'.lower() in str(path).lower():
                        excel_file = r"C:\MCA Portal\Config.xlsx"
                        Sheet_name = "AOC NBFC"
                        config_dict_AOC_NBFC, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                        map_file_path_AOC_NBFC = config_dict_AOC_NBFC['mapping file path']
                        map_sheet_name_AOC_NBFC = config_dict_AOC_NBFC['mapping file sheet name']
                        try:
                            aoc_nbfc_xml_to_db(db_config, config_dict_AOC_NBFC, map_file_path_AOC_NBFC, map_sheet_name_AOC_NBFC,
                                           xml_file_path, output_excel_path, Cin, CompanyName,AOC_4_first_file_found)
                        except Exception as e:
                            print(f"Excpetion occured while processing AOC-4-NBFC {e}")
                        else:
                            AOC_4_first_file_found = True
                    else:
                        excel_file = r"C:\MCA Portal\Config.xlsx"
                        Sheet_name = "AOC"
                        config_dict_AOC, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                        map_file_path_AOC = config_dict_AOC['mapping_file_path']
                        map_sheet_name_AOC = config_dict_AOC['mapping _file_sheet_name']
                        try:
                            AOC_xml_to_db(db_config, config_dict_AOC, map_file_path_AOC, map_sheet_name_AOC, xml_file_path,output_excel_path, Cin, CompanyName,AOC_4_first_file_found)
                        except Exception as e:
                            print(f"Excpetion occured while processing AOC-4 {e}")
                        else:
                            AOC_4_first_file_found = True
                elif 'CHANGE OF NAME'.lower() in str(path).lower():
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "Change of name"
                    config_dict_Change_of_name,config_status = create_main_config_dictionary(excel_file,Sheet_name)
                    map_file_path_Change_of_name = config_dict_Change_of_name['mapping_file_path']
                    map_sheet_name_Change_of_name = config_dict_Change_of_name['mapping _file_sheet_name']
                    ChangeOfName_xml_to_db(db_config,config_dict_Change_of_name,map_file_path_Change_of_name,map_sheet_name_Change_of_name,xml_file_path,output_excel_path,Cin,CompanyName)
                elif 'CHG'.lower() in str(path).lower():
                    excel_file = r"C:\MCA Portal\Config.xlsx"
                    Sheet_name = "CHG1"
                    config_dict_CHG,config_status = create_main_config_dictionary(excel_file,Sheet_name)
                    map_file_path_CHG = config_dict_CHG['mapping_file_path']
                    map_sheet_name_CHG = config_dict_CHG['mapping_file_sheet_name']
                    chg1_xml_to_db(db_cursor,config_dict_CHG,map_file_path_CHG,map_sheet_name_CHG,xml_file_path,output_excel_path,Cin,CompanyName,date)
                # elif 'XBRL document in respect Consolidated'.lower() in str(path).lower() or 'XBRL financial statements'.lower() in str(path).lower():
                #     excel_file = r"C:\MCA Portal\Config.xlsx"
                #     Sheet_name = "AOC XBRL"
                #     config_dict_Xbrl, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                #     output_json_path = str(path).replace('.pdf', '.json')
                #     map_file_path_xbrl = config_dict_Xbrl['mapping file path']
                #     map_sheet_name_xbrl = config_dict_Xbrl['mapping file sheet name']
                #     AOC_XBRL_JSON_to_db(db_config,config_dict_Xbrl,map_file_path_xbrl,map_sheet_name_xbrl,output_json_path,output_excel_path,Cin,CompanyName)
            except Exception as e:
                print(f"Exception occured while inserting into DB {e}")
                continue
    except Exception as e:
        print(f"Exception Occured while inserting for hidden attachments {e}")
        return False
    else:
        return True









