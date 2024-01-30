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
from JSONLoaderGeneration import JSON_loader
from DBFunctions import update_db_insertion_status
from DIR_XMLTo_DB import dir_xml_to_db
from DIR_XMLTo_DB import dir_attachment_xml_to_db
from AOC4_NBFC_CFS_XMLToDB import aoc_nbfc_cfs_xml_to_db
from Form8_annual_XMLToDB import form8_annual_xml_to_db
from Form8_interim_XMLToDB import form8_interim_xml_to_db
from Form11_XMLToDB import form_11_xml_to_db
from Form_Fillip_XMLToDB import form_fillip_xml_to_db
from GSTAPI import insert_gst_number
from logging_config import setup_logging
import logging
from OrderJson import order_json
from AddressSplitUsingOpenAI import split_address
from EPFO_order import order_epfo
import sys
import traceback
from DBFunctions import check_files_and_update
import re
from DIRAddressHiddenAttachment import mgt_address_main
from DirectorShareholdingsHiddenAttachment import mgt_director_shareholdings_main
from Form18_xml_to_db import form_18_xml_to_db
from FreshCertificateOpenAI import fresh_name_main
from DIR_11_xml_to_db import dir11_main
from DIR2PDFToDB import dir2_main
from AOC4_CFS_XMLToDB import AOC_cfs_xml_to_db
from DBFunctions import update_locked_by_empty

def sign_out(driver,config_dict,CinData):
    try:
        sign_out_button = driver.find_element(By.XPATH, '//a[@id="loginAnchor" and text()="Signout"]')

        if sign_out_button.is_displayed():
            sign_out_button.click()
            print("Signed Out")

        if 'driver' in locals():
            driver.delete_all_cookies()
            driver.quit()
            driver.close()
        db_config = get_db_credentials(config_dict)
        username = CinData[15]
        update_logout_status(username,db_config)
    except Exception as e:
        print(f"Error Signing out {e}")
        db_config = get_db_credentials(config_dict)
        try:
            if 'driver' in locals():
                driver.delete_all_cookies()
                driver.quit()
                driver.close()
        except:
            pass
        username = CinData[15]
        update_logout_status(username, db_config)


def Login_and_Download(config_dict,CinData):
    try:
        setup_logging()
        root_path = config_dict['Root path']
        if not os.path.exists(root_path):
            os.makedirs(root_path)
        retry_count = config_dict['Retry Count']
        chrome_driver_path = config_dict['chrome_driver_path']
        Url = config_dict['Url']
        Cin, CompanyName, User = CinData[2], CinData[3], CinData[15]
        workflow_status = CinData[5]
        db_insertion_status = CinData[67]
        db_config = get_db_credentials(config_dict)
        update_locked_by(db_config,Cin)
        last_logged_in_user = None
        if last_logged_in_user is None or last_logged_in_user != User:
            username, password, Status = fetch_user_credentials_from_db(db_config, User)
            if Status == "Pass":
                Login, driver,options,exception_message = login_to_website(Url, chrome_driver_path, username, password, db_config)
            else:
                logging.warning("Already Logged in")
                # update_status(User,"Exception",db_config)
                exception_message = "Already Logged in"
                update_locked_by_empty(db_config, Cin)
                return False,None,exception_message
            print(Login)
            if Login == "Pass":
                logging.info("Successfully Logged in")
                last_logged_in_user = User
            else:
                # update_status(User, "Login Failed", db_config,Cin)
                update_locked_by_empty(db_config,Cin)
                return False,None,exception_message
        else:
            logging.info("Already Logged in so carrying on with the same credentials")
        Navigation = Navigate_to_Company(Cin, CompanyName, driver, db_config)
        if Navigation:
            logging.info(f"Navigated succesfully to {CompanyName}")
        else:
            raise Exception(f"Failed to Navigate to {CompanyName}")
        category_list = ['Annual Returns and Balance Sheet eForms','Certificates','Charge Documents','Change in Directors','Incorporation Documents','LLP Forms(Conversion of company to LLP)','Other eForm Documents','Other Attachments']
        insertion_counter = 0
        if db_insertion_status != 'Y':
            for item in category_list:
                try:
                    category_selection = select_category(item,driver)
                    if category_selection:
                        download_status = insert_Download_Details(driver, Cin, CompanyName, db_config, item)
                    else:
                        continue
                    if download_status:
                        insertion_counter += 1
                    else:
                        continue
                except Exception as e:
                    print(f"Exception Occured for category {item}{e}")
                    continue
            if len(category_list) == insertion_counter:
                #update_status(User, 'download_insertion_success', db_config, Cin)
                update_download_insertion_status(db_config,Cin)
            else:
                raise Exception(f"Download Insertion failed for {Cin}")
            update_extraction_status = update_form_extraction_status(db_config, Cin, CompanyName)
            if update_extraction_status:
                logging.info("Successfully changed form extraction status")
        for category in category_list:
            try:
                category_selection = select_category(category, driver)
                if category_selection:
                    file_download = download_documents(driver, db_config, Cin, CompanyName, category, root_path, options)
                else:
                    continue
                if file_download:
                    print(f"Downloaded for {category} ")
                else:
                    continue
            except Exception as e:
                print(f"Excpetion occured while downloading{e}")
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        download_file_details = "select * from documents where cin = %s and form_data_extraction_needed='Y' and Download_Status='Pending'"
        values = (Cin,)
        cursor.execute(download_file_details,values)
        download_files = cursor.fetchall()
        cursor.close()
        connection.close()
        check_files_and_update(Cin,db_config)
        if len(download_files) < 10:
            return True,driver,None
        else:
            exception_message = f"Download failed for {Cin}"
            return False,driver,exception_message
    except Exception as e:
        print(f"Exception Occured in downloading Main {e}")
        return False, driver, e
    else:
        return True,driver,None

def XMLGeneration(db_config,CinData,config_dict):
    try:
        setup_logging()
        connection,cursor = connect_to_database(db_config)
        Cin, CompanyName, User = CinData[2], CinData[3], CinData[15]
        print(Cin)
        print(CompanyName)
        root_path = config_dict['Root path']
        files_to_be_extracted, Fetch_File_Data_Status = fetch_form_extraction_file_data_from_table(connection, Cin,CompanyName)
        cursor.close()
        connection.close()
        hidden_attachments_list = []
        if Fetch_File_Data_Status == "Pass":
            hidden_attachments = []
            for files in files_to_be_extracted:
                try:
                    pdf_path = files[8]
                    file_name = files[4]
                    file_date = files[5]
                    if 'XBRL document in respect Consolidated' in file_name or 'XBRL financial statements' in file_name:
                        continue
                    if 'Fresh Certificate of Incorporation'.lower() in str(file_name).lower():
                        update_xml_extraction_status(Cin, file_name, config_dict, 'Success')
                        continue
                    folder_path = os.path.dirname(pdf_path)
                    xml_file_path, PDF_to_XML = PDFtoXML(pdf_path, file_name)
                    if PDF_to_XML:
                        update_xml_extraction_status(Cin, file_name, config_dict, 'Success')
                        hidden_attachments = CheckHiddenAttachemnts(xml_file_path, folder_path, pdf_path, file_name,db_config,Cin,CompanyName)
                        for element in hidden_attachments:
                            hidden_attachments_list.append(element)
                    else:
                        update_xml_extraction_status(Cin, file_name, config_dict, 'Failure')
                        continue
                except Exception as e:
                    print(f"Exception Occured while converting to xml{e}")
                    continue
        connection, cursor = connect_to_database(db_config)
        query_for_xbrl = "select * from documents where cin=%s and company=%s and (document like '%%XBRL document in respect Consolidated%%'  or document like '%%XBRL financial statements%%') and form_data_extraction_needed='Y' and Download_Status='Downloaded' and form_data_extraction_status='Pending'"
        values_xbrl = (Cin,CompanyName)
        print(query_for_xbrl % values_xbrl)
        try:
            cursor.execute(query_for_xbrl,values_xbrl)
            xbrl_files = cursor.fetchall()
            for xbrl_file in xbrl_files:
                try:
                    xbrl_pdf_path = xbrl_file[8]
                    xbrl_file_name = xbrl_file[4]
                    xbrl_file_date = xbrl_file[5]
                    print(f"Generating JSON for {xbrl_pdf_path}")
                    xbrl_json_path = str(xbrl_pdf_path).replace('.pdf', '.json')
                    json_generation_xbrl = json_generation(xbrl_pdf_path, xbrl_json_path)
                    if json_generation_xbrl:
                        update_xml_extraction_status(Cin, xbrl_file_name, config_dict, 'Success')
                    else:
                        update_xml_extraction_status(Cin, xbrl_file_name, config_dict, 'Failure')
                except Exception as e:
                    print(f"Exception Occured for XBRL JSON Generation{e}")
            cursor.close()
            connection.close()
        except Exception as e:
            print(f"Exception occured in query for XBRL JSON Generation{e}")

    except Exception as e:
        print(f"Exception Occured {e}")
        return False,[]
    else:
        return True, hidden_attachments_list


def insert_fields_into_db(hiddenattachmentslist,config_dict,CinData,excel_file):
    try:
        setup_logging()
        db_config = get_db_credentials(config_dict)
        connection = mysql.connector.connect(**db_config)
        db_cursor = connection.cursor()
        connection.autocommit = True

        Cin, CompanyName, User = CinData[2], CinData[3], CinData[15]

        try:
            company_update_query = "Update charge_sequence set company_name = %s where cin = %s"
            company_values = (CompanyName, Cin)
            print(company_update_query % company_values)
            db_cursor.execute(company_update_query, company_values)
        except Exception as e:
            print(f"Exception occurred in updating company name {e}")

        try:
            company_update_query = "Update open_charges_latest_event set company_name = %s where cin = %s"
            company_values = (CompanyName, Cin)
            print(company_update_query % company_values)
            db_cursor.execute(company_update_query, company_values)
        except Exception as e:
            print(f"Exception occurred in updating company name {e}")
        xml_files_to_insert = get_xml_to_insert(Cin, config_dict)
        AOC_4_first_file_found = False
        AOC_XBRL_first_file_found = False
        AOC_4_NBFC_first_file_found = False
        Form8_first_file_found = False

        for xml in xml_files_to_insert:
            try:
                path = xml[8]
                date = xml[5]
                file_name = xml[4]
                xml_file_path = str(path).replace('.pdf', '.xml')
                output_excel_path = str(path).replace('.pdf', '.xlsx')
                if 'MGT'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    Sheet_name = "MGT"
                    config_dict_MGT, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    if 'MGT-7A' in path:
                        map_file_path = config_dict_MGT['map_file_path_mgt7A']
                    else:
                        map_file_path = config_dict_MGT['map_file_path']
                    map_file_sheet_name = config_dict_MGT['map_file_sheet_name']
                    mgt_7_db_insertion = mgt7_xml_to_db(db_config, config_dict_MGT, map_file_path, map_file_sheet_name, xml_file_path,output_excel_path, Cin, CompanyName)
                    if mgt_7_db_insertion:
                        update_db_insertion_status(Cin,file_name,config_dict,'Success')
                    db_connection = mysql.connector.connect(**db_config)
                    db_cursor = db_connection.cursor()
                    dir_check_query = "select * from documents where cin = %s and form_data_extraction_needed = 'Y' and LOWER(document) like '%%dir-12%%'"
                    dir_values = (Cin,)
                    print(dir_check_query % dir_values)
                    db_cursor.execute(dir_check_query,dir_values)
                    dir_result = db_cursor.fetchall()
                    db_cursor.close()
                    db_connection.close()
                    if len(dir_result) == 0:
                        Sheet_name_MGT_address = "OpenAI"
                        config_dict_MGT_address, config_status = create_main_config_dictionary(excel_file, Sheet_name_MGT_address)
                        output_directory = os.path.dirname(path)
                        mgt_address = mgt_address_main(db_config,config_dict_MGT_address,output_directory,path,Cin)
                    db_connection = mysql.connector.connect(**db_config)
                    db_cursor = db_connection.cursor()
                    director_shareholdings_query = "select * from director_shareholdings where cin = %s and din_pan != ''"
                    director_shareholdings_values = (Cin,)
                    print(director_shareholdings_query % director_shareholdings_values)
                    db_cursor.execute(director_shareholdings_query,director_shareholdings_values)
                    director_shareholdings_result = db_cursor.fetchall()
                    if len(director_shareholdings_result) == 0:
                        logging.info("Going to shareholdings hidden attachment")
                        Sheet_name_MGT_address = "OpenAI"
                        config_dict_shareholdings, config_status = create_main_config_dictionary(excel_file,Sheet_name_MGT_address)
                        output_directory = os.path.dirname(path)
                        shareholdings = mgt_director_shareholdings_main(db_config,config_dict_shareholdings,output_directory,path,Cin)
                    db_cursor.close()
                    db_connection.close()
                elif 'MSME'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    Sheet_name = "MSME"
                    config_dict_MSME, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    map_file_path_MSME = config_dict_MSME['mapping_file_path']
                    map_sheet_name_MSME = config_dict_MSME['mapping _file_sheet_name']
                    print("Inserting MSME to DB")
                    msme_db_insertion = msme_xml_to_db(db_config,config_dict_MSME,map_file_path_MSME,map_sheet_name_MSME,xml_file_path,output_excel_path,Cin,CompanyName)
                    if msme_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                elif 'AOC'.lower() in str(file_name).lower() and 'AOC-4(XBRL)'.lower() not in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    if 'AOC-4 NBFC'.lower() in str(path).lower():
                        Sheet_name = "AOC NBFC"
                        config_dict_AOC_NBFC, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                        map_file_path_AOC_NBFC = config_dict_AOC_NBFC['mapping file path']
                        map_sheet_name_AOC_NBFC = config_dict_AOC_NBFC['mapping file sheet name']
                        try:
                            aoc_nbfc_db_insertion = aoc_nbfc_xml_to_db(db_config, config_dict_AOC_NBFC, map_file_path_AOC_NBFC, map_sheet_name_AOC_NBFC,
                                           xml_file_path, output_excel_path, Cin, CompanyName,AOC_4_first_file_found)
                            if aoc_nbfc_db_insertion:
                                update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                        except Exception as e:
                            print(f"Excpetion occured while processing AOC-4-NBFC {e}")
                        else:
                            AOC_4_first_file_found = True
                    elif 'AOC-4 CFS NBFC'.lower() in str(file_name).lower():
                        Sheet_name = "AOC CFS"
                        config_dict_AOC_CFS, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                        map_file_path_AOC_CFS = config_dict_AOC_CFS['mapping file path']
                        map_sheet_name_AOC_CFS = config_dict_AOC_CFS['mapping file sheet name']
                        try:
                            print(AOC_4_NBFC_first_file_found)
                            aoc_cfs_db_insertion = aoc_nbfc_cfs_xml_to_db(db_config,config_dict_AOC_CFS,map_file_path_AOC_CFS,map_sheet_name_AOC_CFS,xml_file_path,output_excel_path,Cin,CompanyName,AOC_4_NBFC_first_file_found)
                            if aoc_cfs_db_insertion:
                                update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                        except Exception as e:
                            print(f"Excpetion occured while processing AOC-4-NBFC-CFS {e}")
                        else:
                            AOC_4_NBFC_first_file_found = True
                    elif 'AOC-4 CSR'.lower() in str(file_name).lower():
                        continue
                    elif 'CFS'.lower() in str(file_name).lower():
                        logging.info("Going to AOC 4 CFS")
                        Sheet_name = "AOC-4-CFS"
                        config_dict_cfs,config_status = create_main_config_dictionary(excel_file,Sheet_name)
                        map_file_path_cfs = config_dict_cfs['mapping file path']
                        map_sheet_cfs = config_dict_cfs['mapping file sheet name']
                        try:
                            aoc_cfs_insertion = AOC_cfs_xml_to_db(db_config,config_dict_cfs,map_file_path_cfs,map_sheet_cfs,xml_file_path,output_excel_path,Cin,CompanyName)
                            if aoc_cfs_insertion:
                                update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                        except Exception as e:
                            logging.info(f"Excpetion {e} occured while inserting for AOC CFS")
                    else:
                        Sheet_name = "AOC"
                        config_dict_AOC, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                        map_file_path_AOC = config_dict_AOC['mapping_file_path']
                        map_sheet_name_AOC = config_dict_AOC['mapping _file_sheet_name']
                        try:
                            aoc_db_insertion = AOC_xml_to_db(db_config, config_dict_AOC, map_file_path_AOC, map_sheet_name_AOC, xml_file_path,output_excel_path, Cin, CompanyName,AOC_4_first_file_found)
                            if aoc_db_insertion:
                                update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                        except Exception as e:
                            print(f"Excpetion occured while processing AOC-4 {e}")
                        else:
                            AOC_4_first_file_found = True

                elif 'CHANGE OF NAME'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    Sheet_name = "Change of name"
                    config_dict_Change_of_name,config_status = create_main_config_dictionary(excel_file,Sheet_name)
                    if 'Fresh Certificate of Incorporation'.lower() in str(path).lower():
                        sheet_name = 'OpenAI'
                        config_dict_fresh_name, status = create_main_config_dictionary(excel_file, sheet_name)
                        fresh_name = fresh_name_main(db_config,config_dict_fresh_name,path,Cin)
                        if fresh_name:
                            update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                    else:
                        if len(Cin) == 21:
                            map_file_path_Change_of_name = config_dict_Change_of_name['mapping_file_path']
                        else:
                            map_file_path_Change_of_name = config_dict_Change_of_name['mapping_file_path_llp']
                        map_sheet_name_Change_of_name = config_dict_Change_of_name['mapping _file_sheet_name']
                        change_of_name_db_insertion = ChangeOfName_xml_to_db(db_config,config_dict_Change_of_name,map_file_path_Change_of_name,map_sheet_name_Change_of_name,xml_file_path,output_excel_path,Cin,CompanyName)
                        if change_of_name_db_insertion:
                            update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                elif 'CERTIFICATE OF INCORPORATION CONSEQUENT'.lower() in str(file_name).lower() and 'FRESH'.lower() not in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    logging.info("Going to Certificate of incorporation")
                    Sheet_name = "Change of name"
                    config_dict_certificate, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    map_file_path_certificate = config_dict_certificate['certificate_incorporation_config']
                    map_sheet_name_certificate = config_dict_certificate['mapping _file_sheet_name']
                    certificate_db_insertion = ChangeOfName_xml_to_db(db_config, config_dict_certificate,
                                                                         map_file_path_certificate,
                                                                         map_sheet_name_certificate, xml_file_path,
                                                                         output_excel_path, Cin, CompanyName)
                    if certificate_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                elif 'CHG'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    Sheet_name = "CHG1"
                    config_dict_CHG,config_status = create_main_config_dictionary(excel_file,Sheet_name)
                    digit_count = sum(c.isdigit() for c in file_name)
                    print(digit_count)
                    if digit_count == 7:
                        map_file_path_CHG = config_dict_CHG['old_file_config']
                        print("old file")
                    else:
                        map_file_path_CHG = config_dict_CHG['mapping_file_path']
                        print("new file")
                    map_sheet_name_CHG = config_dict_CHG['mapping_file_sheet_name']
                    chg_db_insertion = chg1_xml_to_db(db_config,config_dict_CHG,map_file_path_CHG,map_sheet_name_CHG,xml_file_path,output_excel_path,Cin,CompanyName,date,file_name)
                    if chg_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                elif 'XBRL document in respect Consolidated'.lower() in str(file_name).lower() or 'XBRL financial statements'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    Sheet_name = "AOC XBRL"
                    config_dict_Xbrl, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    output_json_path = str(path).replace('.pdf', '.json')
                    map_file_path_xbrl = config_dict_Xbrl['mapping file path']
                    map_sheet_name_xbrl = config_dict_Xbrl['mapping file sheet name']
                    try:
                        aoc_xbrl_db_insertion = AOC_XBRL_JSON_to_db(db_config, config_dict_Xbrl, map_file_path_xbrl,
                                                                    map_sheet_name_xbrl, output_json_path,
                                                                    output_excel_path, Cin, CompanyName,AOC_XBRL_first_file_found,file_name)
                        if aoc_xbrl_db_insertion:
                            update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                    except Exception as e:
                        print(f"Exception occured while inserting into db for XBRL")
                    else:
                        AOC_XBRL_first_file_found = True
                elif 'DIR'.lower() in str(file_name).lower():
                    if 'DIR_2'.lower() in file_name.lower() or 'DIR-2'.lower() in file_name.lower() or 'DIR 2'.lower() in file_name.lower() or 'DIR-2-'.lower() in file_name.lower():
                        logging.info("Going to extract for Dir-2 hidden attachment")
                        Sheet_name = "OpenAI"
                        config_dict_dir, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                        dir2 = dir2_main(db_config, config_dict_dir, None, path, Cin)
                        if dir2:
                            update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                    elif 'DIR-11'.lower() in str(file_name).lower():
                        continue
                    else:
                        logging.info(f"Going to extract DIR for {file_name}")
                        Sheet_name = "DIR"
                        config_dict_DIR,config_status = create_main_config_dictionary(excel_file,Sheet_name)
                        digit_count = sum(c.isdigit() for c in file_name)
                        logging.info(digit_count)
                        if digit_count == 8:
                            map_file_path_DIR = config_dict_DIR['Form32_config']
                            logging.info("old file")
                        else:
                            map_file_path_DIR = config_dict_DIR['mapping file path']
                            logging.info("new file")
                        map_sheet_name_dir = config_dict_DIR['mapping file sheet name']
                        xml_hidden_file_path = xml_file_path.replace('.xml', '_hidden.xml')
                        dir_db_insertion = dir_xml_to_db(db_config,config_dict_DIR,map_file_path_DIR,map_sheet_name_dir,xml_file_path,xml_hidden_file_path,output_excel_path,Cin,date,file_name)
                        if dir_db_insertion:
                            update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                elif 'Form8'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    Sheet_name = 'Form_8_annual'
                    config_dict_form8,config_status = create_main_config_dictionary(excel_file,Sheet_name)
                    map_file_path_form8 = config_dict_form8['mapping file path']
                    map_sheet_name_form8 = config_dict_form8['mapping file sheet name']
                    form8_db_insertion = form8_annual_xml_to_db(db_config,config_dict_form8,map_file_path_form8,map_sheet_name_form8,xml_file_path,output_excel_path,Cin,Form8_first_file_found)
                    if form8_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                        Form8_first_file_found = True
                    Sheet_name_interim = 'Form_8_Interim'
                    config_dict_form8_interim,config_status = create_main_config_dictionary(excel_file,Sheet_name_interim)
                    map_file_path_form8_interim = config_dict_form8_interim['mapping file path']
                    map_sheet_name_form8_interim = config_dict_form8_interim['mapping file sheet name']
                    form8_interim_db_insertion = form8_interim_xml_to_db(db_config,config_dict_form8_interim,map_file_path_form8_interim,map_sheet_name_form8_interim,xml_file_path,output_excel_path,Cin,date)
                    if form8_interim_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                elif 'Form11'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    sheet_name_form11 = 'Form_11'
                    config_dict_form11,config_status = create_main_config_dictionary(excel_file,sheet_name_form11)
                    map_file_path_form11 = config_dict_form11['mapping file path']
                    map_sheet_name_form11 = config_dict_form11['mapping file sheet name']
                    xml_hidden_file_path = xml_file_path.replace('.xml', '_hidden.xml')
                    form11_db_insertion = form_11_xml_to_db(db_config,config_dict_form11,map_file_path_form11,map_sheet_name_form11,xml_file_path,output_excel_path,Cin,xml_hidden_file_path)
                    if form11_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                elif 'FiLLiP'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    sheet_name_fillip = 'Form_Fillip'
                    config_dict_fillip,config_status = create_main_config_dictionary(excel_file,sheet_name_fillip)
                    map_file_path_fillip = config_dict_fillip['mapping file path']
                    map_sheet_name_fillip = config_dict_fillip['mapping file sheet name']
                    fillip_db_insertion = form_fillip_xml_to_db(db_config,config_dict_fillip,map_file_path_fillip,map_sheet_name_fillip,xml_file_path,output_excel_path,Cin)
                    if fillip_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')

                elif 'Form 8'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    Sheet_name = "CHG1"
                    config_dict_form8_charge, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    map_file_path_Form8 = config_dict_form8_charge['Form8_Config']
                    map_sheet_name_Form8 = config_dict_form8_charge['mapping_file_sheet_name']
                    form8_charge_db_insertion = chg1_xml_to_db(db_config, config_dict_form8_charge, map_file_path_Form8, map_sheet_name_Form8,
                                                      xml_file_path, output_excel_path, Cin, CompanyName, date,file_name)
                    if form8_charge_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                    else:
                        logging.info("Going to Form 8 other type")
                        map_file_path_form8_other_type = config_dict_form8_charge['form8_other_type_config']
                        form8_other_type = chg1_xml_to_db(db_config, config_dict_form8_charge, map_file_path_form8_other_type, map_sheet_name_Form8,
                                                      xml_file_path, output_excel_path, Cin, CompanyName, date,file_name)
                        if form8_other_type:
                            update_db_insertion_status(Cin, file_name, config_dict, 'Success')

                elif 'Form 18'.lower() in str(file_name).lower() or 'INC-22'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for {file_name}")
                    sheet_name = 'Form18'
                    config_dict_form18,config_status = create_main_config_dictionary(excel_file,sheet_name)
                    xml_hidden_file_path = xml_file_path.replace('.xml', '_hidden.xml')
                    if 'Form 18'.lower() in str(file_name).lower():
                        logging.info("Going to extract data for Form 18 new files")
                        map_file_path_form18 = config_dict_form18['mapping file path']
                    elif 'INC-22'.lower() in str(file_name).lower():
                        digit_count = sum(c.isdigit() for c in file_name)
                        logging.info(digit_count)
                        if digit_count == 10:
                            logging.info("Going to extract for new inc files")
                            map_file_path_form18 = config_dict_form18['inc_new_config']
                        else:
                            logging.info("Going to extract for old inc files")
                            map_file_path_form18 = config_dict_form18['inc_config']
                        logging.info("Going to extract data for INC 22 files")
                    else:
                        map_file_path_form18 = None
                    map_sheet_name_form18 = config_dict_form18['mapping file sheet name']
                    form18_db_insertion = form_18_xml_to_db(db_config,config_dict_form18,map_file_path_form18,map_sheet_name_form18,xml_file_path,output_excel_path,Cin,xml_hidden_file_path,file_name)
                    if form18_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')
                    else:
                        if 'Form 18'.lower() in str(file_name).lower():
                            logging.info(f"Going to Form 18 old file")
                            map_file_path_form18_old = config_dict_form18['form18_old_files_config']
                            form18_old_db_insertion = form_18_xml_to_db(db_config, config_dict_form18, map_file_path_form18_old,
                                                                    map_sheet_name_form18, xml_file_path,
                                                                    output_excel_path, Cin,xml_hidden_file_path,file_name)
                            if form18_old_db_insertion:
                                update_db_insertion_status(Cin, file_name, config_dict, 'Success')

                elif 'Form 32'.lower() in str(file_name).lower():
                    logging.info(f"Going to extract for Form 32 for {file_name}")
                    Sheet_name = "DIR"
                    config_dict_form32, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                    map_file_path_form32 = config_dict_form32['Form32_config']
                    map_sheet_name_form32 = config_dict_form32['mapping file sheet name']
                    logging.info(map_file_path_form32)
                    logging.info(map_sheet_name_form32)
                    xml_hidden_file_path = xml_file_path.replace('.xml', '_hidden.xml')
                    form32_db_insertion = dir_xml_to_db(db_config, config_dict_form32, map_file_path_form32, map_sheet_name_form32,
                                                     xml_file_path, xml_hidden_file_path, output_excel_path, Cin, date,file_name)
                    if form32_db_insertion:
                        update_db_insertion_status(Cin, file_name, config_dict, 'Success')
            except Exception as e:
                print(f"Exception occured while inserting into DB {e}")
                continue
        try:
            if len(hiddenattachmentslist) != 0:
                for hiddenattachment in hiddenattachmentslist:
                    if "Subsidiaries" in hiddenattachment or "Holding" in hiddenattachment or "Associate" in hiddenattachment or "Joint Venture" in hiddenattachment:
                        Sheet_name = "MGT"
                        config_dict_Subs, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                        Subsidiary_Config = config_dict_Subs['Subsidiary_Config']
                        Subsidiary_Config_Sheet_Name = config_dict_Subs['Subsidiary_Config_Sheet_Name']
                        map_file_path = Subsidiary_Config
                        map_file_sheet_name = Subsidiary_Config_Sheet_Name
                        output_excel_name = str(hiddenattachment).replace('.xml', '.xlsx')
                        folder_path = os.path.dirname(hiddenattachment)
                        output_excel_path = os.path.join(folder_path, output_excel_name)
                        mgt7_xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, hiddenattachment,
                                 output_excel_path, Cin, CompanyName)
                    elif "Business Activity" in hiddenattachment:
                        Sheet_name = "MGT"
                        config_dict_Business, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                        Business_Activity_Config = config_dict_Business['Business_Activity_Config']
                        Business_Activity_Config_Sheet_Name = config_dict_Business['Business_Activity_Config_Sheet_Name']
                        map_file_path = Business_Activity_Config
                        map_file_sheet_name = Business_Activity_Config_Sheet_Name
                        output_excel_name = str(hiddenattachment).replace('.xml', '.xlsx')
                        folder_path = os.path.dirname(hiddenattachment)
                        output_excel_path = os.path.join(folder_path, output_excel_name)
                        mgt7_xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, hiddenattachment,
                                 output_excel_path, Cin, CompanyName)
                    elif 'DIR_2'.lower() in hiddenattachment.lower() or 'DIR-2'.lower() in hiddenattachment.lower() or 'DIR 2'.lower() in hiddenattachment.lower() or 'DIR-2-'.lower() in hiddenattachment.lower():
                        Sheet_name = "OpenAI"
                        config_dict_dir, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                        # map_file_path_dir2 = config_dict_dir['DIR2_map_file_path']
                        # map_file_sheet_name = config_dict_dir['mapping file sheet name']
                        # output_excel_path = str(hiddenattachment).replace('.xml', '.xlsx')
                        #dir_hidden_xml = dir_attachment_xml_to_db(db_config,config_dict_dir,map_file_path_dir2,map_file_sheet_name,hiddenattachment,output_excel_path,Cin)
                        dir2 = dir2_main(db_config, config_dict_dir, None, hiddenattachment, Cin)
                    else:
                        pass
        except Exception as e:
            logging.info(f"Exception Occured {e}")

        try:
            dir11_connection = mysql.connector.connect(**db_config)
            dir11_cursor = dir11_connection.cursor()
            dir11_connection.autocommit = True
            dir11_query = "SELECT * FROM documents where cin=%s and Download_Status='Downloaded' and form_data_extraction_status='Success' and DB_insertion_status='Pending' and form_data_extraction_needed = 'Y' and document like '%%DIR-11%%'"
            dir11_values = (Cin,)
            logging.info(dir11_query % dir11_values)
            dir11_cursor.execute(dir11_query,dir11_values)
            dir11_result = dir11_cursor.fetchall()
            dir11_cursor.close()
            dir11_connection.close()
            for dir11 in dir11_result:
                Sheet_name = "DIR"
                config_dict_dir11, config_status = create_main_config_dictionary(excel_file, Sheet_name)
                map_file_sheet_name = config_dict_dir11['mapping file sheet name']
                path = dir11[8]
                date = dir11[5]
                file_name = dir11[4]
                digit_count = sum(c.isdigit() for c in file_name)
                logging.info(digit_count)
                if digit_count == 10:
                    logging.info("Going to extract for new DIR 11 files")
                    map_file_path_dir11 = config_dict_dir11['DIR11_config']
                else:
                    logging.info("Going to extract for old DIR 11 files")
                    map_file_path_dir11 = config_dict_dir11['DIR11_config_old']
                xml_file_path = str(path).replace('.pdf', '.xml')
                output_excel_path = str(path).replace('.pdf', '.xlsx')
                cin_column_name = 'cin'
                dir11 = dir11_main(db_config,config_dict_dir11,map_file_path_dir11,map_file_sheet_name,xml_file_path,cin_column_name,Cin,date)
                if dir11:
                    update_db_insertion_status(Cin, file_name, config_dict, 'Success')
        except Exception as e:
            logging.info(f"Exception occured while inserting for Dir 11 {e}")

        for _ in range(0,3):
            try:
                gst_connection = mysql.connector.connect(**db_config)
                gst_cursor = gst_connection.cursor()
                gst_query = """SELECT * FROM orders
                               WHERE MONTH(created_date) = MONTH(CURRENT_DATE())
                                AND YEAR(created_date) = YEAR(CURRENT_DATE()) AND cin=%s AND gst_status='Y'"""
                cin_value = (Cin,)
                print(gst_query % cin_value)
                gst_cursor.execute(gst_query,cin_value)
                gst_result = gst_cursor.fetchall()
                gst_cursor.close()
                gst_connection.close()
                if len(gst_result) == 0:
                    sheet_name_gst = 'GST'
                    config_dict_GST,status = create_main_config_dictionary(excel_file,sheet_name_gst)
                    root_path = config_dict['Root path']
                    gst = insert_gst_number(db_config,config_dict_GST,Cin,CompanyName,root_path)
                    if gst:
                        print("Successfully inserted for GST")
                        break
            except Exception as e:
                print(f"Exception occured while inserting GST {e}")
                continue
        try:
            sheet_name = 'OpenAI'
            config_dict_openai,status = create_main_config_dictionary(excel_file,sheet_name)
            split_address(Cin,config_dict_openai,db_config)
        except Exception as e:
            print(f"Exception occured in updating address using open AI {e}")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # Get the formatted traceback as a string
            traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

            # logging.info the traceback details
            for line in traceback_details:
                logging.info(line.strip())
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        db_insert_check_query = "select * from documents where cin=%s and form_data_extraction_needed='Y' and DB_insertion_status='Pending' and Download_Status='Downloaded'"
        values_check = (Cin,)
        print(db_insert_check_query % values_check)
        cursor.execute(db_insert_check_query,values_check)
        result_db_insertion = cursor.fetchall()
        print(len(result_db_insertion))
        if len(result_db_insertion) < 20:
            return True,None
        else:
            db_insertion_exception_message = f"Db insertion failed for {Cin}"
            return False,db_insertion_exception_message
    except Exception as e:
        print(f"Exception Occured while inserting for hidden attachments {e}")
        logging.info(f"Exception in concerting pdf to xml {e}")
        exc_type, exc_value, exc_traceback = sys.exc_info()
            # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

            # logging.info the traceback details
        for line in traceback_details:
            logging.info(line.strip())
        return False,e
    else:
        return True,None

def json_loader_generation(cindata,dbconfig,config_dict,excel_file_path):
    try:
        root_path = config_dict['Root path']
        Cin, CompanyName, User = cindata[2], cindata[3], cindata[15]
        if len(Cin) == 21:
            json_config_path = config_dict['config_json_path_nonllp']
            excel_sheet_name = 'JSON Loader Queries'
            json_loader, json_file_path, exception_message,json_nodes = JSON_loader(dbconfig, json_config_path, Cin, root_path,
                                                                         excel_file_path, excel_sheet_name)
        elif len(Cin) == 8:
            json_config_path = config_dict['config_json_path_llp']
            excel_sheet_name = 'LLP JSON Loader Queries'
            json_loader,json_file_path,exception_message,json_nodes = JSON_loader(dbconfig,json_config_path,Cin,root_path,excel_file_path,excel_sheet_name)
        else:
            json_loader = False
            json_file_path = None
            json_nodes = []
            exception_message = 'Invalid Cin to generate loader'
        if json_loader:
            print("JSON Loader generated successfully")
            if len(Cin) == 21:
                order_sheet_name = "JSON Non-LLP Order"
                logging.info("Starting to order Non-LLP")
            elif len(Cin) == 8:
                order_sheet_name = "JSON LLP Order"
                logging.info("Starting to order LLP")
            else:
                order_Sheet_name = None
                raise Exception("Invalid Cin")
            config_dict_order, config_status = create_main_config_dictionary(excel_file_path, order_sheet_name)
            for json_node in json_nodes:
                try:
                    json_order = order_json(config_dict_order,json_node,json_file_path)
                    print(json_node)
                    if json_order:
                        print("Json ordered successfully")
                        if json_node == 'epfo':
                            epfo_order = order_epfo(json_file_path,json_node)
                            if epfo_order:
                                print("Ordered successfully for epfo")
                except Exception as e:
                    print(f"Exception occured for {json_node} {e}")
        else:
            print("JSON Loader not generated successfully")
            return False,None,exception_message
    except Exception as e:
        print(f"Exception in JSON Loader Main Part {e}")
        return False,None,e
    else:
        return True,json_file_path,None


def update_download_status(db_config,cin):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        query = "UPDATE orders SET document_download_status = 'Y' WHERE cin=%s"
        logging.info(query)
        cursor.execute(query, (cin,))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
    finally:
        cursor.close()
        connection.close()

def update_download_insertion_status(db_config,cin):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        query = "UPDATE orders SET document_db_insertion_status = 'Y' WHERE cin=%s"
        logging.info(query)
        cursor.execute(query, (cin,))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
    finally:
        cursor.close()
        connection.close()





