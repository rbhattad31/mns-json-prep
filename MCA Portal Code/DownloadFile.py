import os
import shutil
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import NoSuchElementException
import mysql.connector
from easyocr import easyocr
import re
from datetime import datetime
import shutil
import sys
import traceback
from selenium.common.exceptions import NoSuchWindowException
from logging_config import setup_logging
import logging

current_date = datetime.now()
today_date = current_date.strftime('%d/%m/%Y %H:%M:%S')
user_name = os.getlogin()


def initialize_driver(chrome_driver_path):
    options = Options()
    options.add_argument('--incognito')
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def select_category(category,driver):
    try:
        setup_logging()
        certificates_button_xpath = f'//a[@class="dashboardlinks" and contains(text(),"{category}")]'
        if click_element_with_retry(driver, certificates_button_xpath):
            logging.info(f"Clicked on {category}")
            time.sleep(4)
            return True
        else:
            return False
    except Exception as e:
        logging.warning(f"Exception while selecting category {e}")
        return False


def Navigate_to_Company(Cin,CompanyName,driver,dbconfig):
    try:
        setup_logging()
        my_workspace_button_xpath = '//a[@href="/mcafoportal/" and text()="My Workspace"]'
        if click_element_with_retry(driver, my_workspace_button_xpath):
            logging.info("Clicked on My Workspace")
            time.sleep(5)
            k = 1
            page_number = 1
            main_page_number = 1
            while True:
                CompanyName = CompanyName.lower()
                download_button_xpath = f'//table[@id="publicDocuments" and @name="publicDocuments"]/tbody/tr[contains(translate(., "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "{CompanyName}")]/td/a'
                if click_element_with_retry(driver, download_button_xpath):
                    logging.info("Clicked on Download")
                    time.sleep(5)
                    break
                else:
                    try:
                        main_page_number_xpath = driver.find_element(By.XPATH,
                                                                     f'//a[@class="paginate_active"]')
                        main_page_number = main_page_number_xpath.text
                        main_page_number = int(main_page_number)
                        logging.info("Page Number", main_page_number)
                    except NoSuchElementException:
                        logging.info("No page number found")
                        pass
                    logging.info("Loop Counter", k)
                    if k != main_page_number and k != 1:
                        logging.info("Completed Navigation")
                        connection = mysql.connector.connect(**dbconfig)
                        cursor = connection.cursor()
                        Update_no_company_query = "update orders set workflow_status='Download Failed' and bot_comments='Company Not found' where cin = %s"
                        Update_no_company_values = (Cin,)
                        logging.info(Update_no_company_query % Update_no_company_values)
                        cursor.execute(Update_no_company_query, Update_no_company_values)
                        connection.commit()
                        cursor.close()
                        connection.close()
                        return False
                    try:
                        # Find and click the "Next" button using XPath
                        next_button = driver.find_element(By.XPATH, '//a[@id="publicDocuments_next"]')
                        next_button.click()
                        k = k + 1
                        continue
                    except Exception as e:
                        # If the button is not clickable, you've reached the last page
                        connection = mysql.connector.connect(**dbconfig)
                        cursor = connection.cursor()
                        Update_no_company_query = "update orders set workflow_status='Download Failed' and bot_comments='Company Not found' where cin = %s"
                        Update_no_company_values = (Cin,)
                        logging.info(Update_no_company_query % Update_no_company_values)
                        cursor.execute(Update_no_company_query, Update_no_company_values)
                        connection.commit()
                        cursor.close()
                        connection.close()
                        logging.warning("No Company Found")
                        return False
    except Exception as e:
        logging.warning(f"Exception occured while navigating{e}")
        return False
    else:
        return True


def insert_Download_Details(driver,Cin, Company,db_config,category):
    try:
        setup_logging()
        current_date = datetime.now()
        current_date = current_date.strftime('%Y-%m-%d %H:%M:%S')
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        j = 1
        i = 0
        page_number = 1
        main_page_number = 1
        date_xpath_counter = 0
        while True:
            try:
                page_number_xpath = driver.find_element(By.XPATH, f'//span[@id="pg{j}" and @class="pg-selected"]')
                page_number = page_number_xpath.text
                page_number = int(page_number)
                logging.info("Page Number", page_number)
            except NoSuchElementException:
                logging.info("No page number found")
                pass
            logging.info("Loop Counter", j)
            if j != page_number and j != 1:
                logging.info("Completed Navigation")
                break
            # document_name_elements = driver.find_elements(By.XPATH,'//table[@id="reportsTab1"]/tbody/tr/td[1]/a')
            for _ in range(0, 10):
                try:
                    document_xpath = f'//table[@id="reportsTab1"]/tbody/tr/td[1]/a[@id="open{i}\'"]'
                    logging.info(document_xpath)
                    document_name_elements = driver.find_element(By.XPATH, document_xpath)
                    logging.info(document_name_elements)
                    document_name = document_name_elements.text
                    parts = document_name.rsplit('.', 1)
                    if len(parts) == 2:
                        # If a dot is found, remove everything after it
                        document_name = parts[0]
                    logging.info("Document Name", document_name)
                    date_xpath = f'//*[@id="reportsTab1"]/tbody/tr[td[*]/a[@id="open{date_xpath_counter}\'"]]/td[2]'
                    index_id = f'open{i}'
                    logging.info(date_xpath)
                    logging.info(index_id)
                    date_of_filing = driver.find_element(By.XPATH, date_xpath)
                    date = date_of_filing.text
                    try:
                        date_obj = datetime.strptime(date, '%d/%m/%Y')
                        formatted_date = date_obj.strftime('%d-%m-%Y')
                    except Exception as e:
                        logging.info(f"Error in converting date {e}")
                        formatted_date = date
                    logging.info("Date Name: " + formatted_date)
                    duplicate_query = "select * from documents where cin=%s and document=%s and company=%s and Category=%s"
                    Value1 = Cin
                    Value2 = document_name
                    Value3 = Company
                    Value4 = category
                    cursor.execute(duplicate_query, (Value1, Value2, Value3, Value4))
                    result = cursor.fetchall()
                    logging.info("Result from db", result)
                    if len(result) == 0:
                        query = "Insert into documents(cin,company,Category,document,document_date_year,form_data_extraction_status,created_date,created_by,form_data_extraction_needed,Page_Number,Download_Status,DB_insertion_status) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        values = (Cin, Company, category, document_name, formatted_date, 'Pending', current_date,user_name, 'N', j, 'Pending','Pending')
                        logging.info(query % values)
                        cursor.execute(query, values)
                        connection.commit()
                    elif len(result) > 0:
                        logging.info("-------------")
                        duplicate_query1 = "select * from documents where cin=%s and document=%s and company=%s and Category=%s and document_date_year = %s"
                        Value1 = Cin
                        Value2 = document_name
                        Value3 = Company
                        Value4 = category
                        Value5 = formatted_date
                        logging.info(duplicate_query1 % (Value1, Value2, Value3, Value4, Value5))
                        cursor.execute(duplicate_query1, (Value1, Value2, Value3, Value4, Value5))
                        result1 = cursor.fetchall()
                        if len(result1) == 0:
                            query = "Insert into documents(cin,company,Category,document,document_date_year,form_data_extraction_status,created_date,created_by,form_data_extraction_needed,Page_Number,Download_Status,DB_insertion_status,path_index) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            values = (
                            Cin, Company, category, document_name, formatted_date, 'Pending', current_date, user_name,
                            'N', j, 'Pending', 'Pending', index_id)
                            logging.info(query % values)
                            cursor.execute(query, values)
                            connection.commit()

                        logging.info("Result from db: ", result1)
                        current_date_check = False

                        for item in result1:
                            # Convert to datetime object
                            logging.info("create Date time" + str(item[13]))

                            # Format as date-only string
                            created_date = str(item[13]).split()[0]

                            logging.info("created date : " + created_date)
                            current_date = current_date.split()[0]
                            logging.info("current date : " + current_date)
                            if str(created_date) == str(current_date):
                                current_date_check = True
                                logging.info("Equal")

                        index_check = False
                        if current_date_check == True:
                            for item1 in result1:
                                if item1[16] != index_id:
                                    index_check = True
                        if index_check == True:
                            query = "Insert into documents(cin,company,Category,document,document_date_year,form_data_extraction_status,created_date,created_by,form_data_extraction_needed,Page_Number,Download_Status,DB_insertion_status,path_index) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            values = (
                            Cin, Company, category, document_name, formatted_date, 'Pending', current_date, user_name,
                            'N', j, 'Pending', 'Pending', index_id)
                            logging.info(query % values)
                            cursor.execute(query, values)
                            connection.commit()
                    else:
                        logging.info("Value already there so not inserting")
                    i = i + 1
                    date_xpath_counter = date_xpath_counter + 1
                except Exception as e:
                    logging.info(f"Exception Occured {e}")
                    i = i + 1
                    date_xpath_counter = date_xpath_counter + 1
                    continue
            j = j + 1
            try:
                next_button_download = driver.find_element(By.XPATH, '//span[@id="next"]')
                time.sleep(2)
                next_button_download.click()
                logging.info("Next Button clicked")
            except NoSuchElementException:
                logging.info("Next Button not clicking")
                pass
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        logging.info(f"Exception occured {e}")
        return False
    else:
        try:
            back_xpath = '//input[@type="submit" and @value="Back"]'
            back_button = driver.find_element(By.XPATH, back_xpath)
            back_button.click()
            logging.info("Clicked on back button")
        except NoSuchElementException:
            back_xpath = '//input[@type="submit" and @value="Back"]'
            back_button = driver.find_element(By.XPATH, back_xpath)
            back_button.click()
            logging.info("Trying to click but not clicking")
        return True


def download_documents(driver,dbconfig,Cin,CompanyName,Category,rootpath,options):
    try:
        j = 1
        setup_logging()
        first_xpath = '//span[@id="first"]'
        try:
            first_button = driver.find_element(By.XPATH, first_xpath)
            first_button.click()
        except Exception as e:
            logging.info("No first button")
            pass
        folder_path = os.path.join(rootpath, Cin, Category)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        page_number = 1
        download_retry = 0
        while True:
            try:
                try:
                    page_number_xpath = driver.find_element(By.XPATH, f'//span[@id="pg{j}" and @class="pg-selected"]')
                    page_number = page_number_xpath.text
                    page_number = int(page_number)
                    logging.info("Page Number", page_number)
                except NoSuchElementException:
                    logging.info("No page number found")
                    pass
                logging.info("Loop Counter", j)
                connection = mysql.connector.connect(**dbconfig)
                cursor = connection.cursor()
                if j != page_number and j != 1:
                    logging.info("Completed Navigation")
                    try:
                        back_xpath = '//input[@type="submit" and @value="Back"]'
                        back_button = driver.find_element(By.XPATH, back_xpath)
                        back_button.click()
                        break
                    except NoSuchElementException:
                        back_xpath = '//input[@type="submit" and @value="Back"]'
                        back_button = driver.find_element(By.XPATH, back_xpath)
                        back_button.click()
                        break
                download_details_query = "select * from documents where cin=%s and company=%s and Category=%s and Page_Number=%s and Download_Status=%s and form_data_extraction_needed='Y'"
                values = (Cin, CompanyName, Category, j, 'Pending')
                logging.info(download_details_query % values)
                cursor.execute(download_details_query, values)
                result = cursor.fetchall()
                cursor.close()
                connection.close()
                for element in result:
                    try:
                        filename = element[4]
                        index_id = element[16]
                        parts = filename.rsplit('.', 1)
                        if len(parts) == 2:
                            # If a dot is found, remove everything after it
                            filename = parts[0]
                        logging.info(filename)
                        download_filepath = os.path.join(folder_path, filename)
                        filepath = download_filepath + '.pdf'
                        if index_id == '' or index_id is None:
                            file_xpath = f'//a[contains(text(),"{filename}")]'
                            try:
                                file_download_button = driver.find_element(By.XPATH, file_xpath)
                                file_download_button.click()
                            except:
                                try:
                                    if 'change of name'.lower() in str(filename).lower():
                                        pattern = re.compile(r'Change of Name.*$', re.IGNORECASE)
                                        logging.info("For change of name going for different x path")
                                        # Use re.search to find the first occurrence of the pattern in the input string
                                        match = pattern.search(filename)
                                        if match:
                                            # Extract the matched portion from the input string
                                            result = match.group(0)

                                            # Print the result
                                            logging.info(result)
                                            file_xpath = f'//a[contains(text(),"{result}")]'
                                            file_download_button = driver.find_element(By.XPATH, file_xpath)
                                            file_download_button.click()
                                        else:
                                            logging.info("Pattern not found in the input string.")
                                            continue
                                    else:
                                        continue
                                except Exception as e:
                                    logging.info(f"Error for change of name x path {e}")
                                    continue
                            file_directory = os.path.dirname(filepath)
                            duplicate_file_path = ''
                        else:
                            duplicate_folder_path = rf"{rootpath}\{Cin}\duplicates"
                            logging.info("duplicate_folder_path" + duplicate_folder_path)

                            duplicate_file_path = os.path.join(duplicate_folder_path, filename)

                            # duplicate_folder_file = duplicate_file_path + "_" + index_id + '.pdf'
                            duplicate_folder_file = duplicate_file_path + '.pdf'

                            logging.info("Changed Filepath : " + filepath)
                            cancated_xpath = "'"
                            file_xpath = f'//*[@id="{index_id}{cancated_xpath}"]'
                            logging.info("Xpath : " + file_xpath)
                            time.sleep(2)
                            file_download_button = driver.find_element(By.XPATH, file_xpath)
                            file_download_button.click()
                            time.sleep(10)
                            file_directory = os.path.dirname(duplicate_folder_file)

                        file_directory = file_directory
                        logging.info(file_directory)
                        params = {
                            "behavior": "allow",
                            "downloadPath": file_directory
                        }
                        driver.execute_cdp_cmd("Page.setDownloadBehavior", params)
                        download = download_captcha_and_enter_text(CompanyName, driver, filepath, filename, Cin,dbconfig,Category, download_filepath,duplicate_file_path,index_id,folder_path,retry_count=10)
                        if download:
                            logging.info("Downloaded Successfully")
                            if 'MGT-7' in filename or 'MGT-7A' in filename:
                                MGT_folder = os.path.join(folder_path, 'MGT')
                                if not os.path.exists(MGT_folder):
                                    os.makedirs(MGT_folder)
                                try:
                                    shutil.copy(filepath, MGT_folder)
                                except Exception as e:
                                    logging.info(f"Error {e}")
                        else:
                            logging.info("Not Downloaded")
                            continue
                    except Exception as e:
                        logging.info(f"Exception occured {e}")
                        continue
                connection = mysql.connector.connect(**dbconfig)
                cursor = connection.cursor()
                check_pending_query = "select * from documents where cin=%s and company=%s and Category=%s and Page_Number=%s and Download_Status=%s and form_data_extraction_needed='Y'"
                pending_values = (Cin, CompanyName, Category, j, 'Pending')
                logging.info(check_pending_query % pending_values)
                cursor.execute(check_pending_query, pending_values)
                pending_result = cursor.fetchall()
                logging.info(pending_result)
                cursor.close()
                connection.close()
                if len(pending_result) == 0:
                    j = j + 1
                    try:
                        next_button_download = driver.find_element(By.XPATH, '//span[@id="next"]')
                        time.sleep(2)
                        next_button_download.click()
                        logging.info("Next Button clicked")
                    except NoSuchElementException:
                        next_button_download = driver.find_element(By.XPATH, '//span[@id="next"]')
                        time.sleep(2)
                        next_button_download.click()
                        logging.info("Next Button not clicking")
                        pass
                else:
                    download_retry += 1
                    if download_retry > 5:
                        j = j + 1
                        try:
                            next_button_download = driver.find_element(By.XPATH, '//span[@id="next"]')
                            time.sleep(2)
                            next_button_download.click()
                            logging.info("Next Button clicked")
                        except NoSuchElementException:
                            next_button_download = driver.find_element(By.XPATH, '//span[@id="next"]')
                            time.sleep(2)
                            next_button_download.click()
                            logging.info("Next Button not clicking")
                            pass
                    else:
                        continue
            except Exception as e:
                logging.info(f"Excpetion {e} occured while navigating")
                exc_type, exc_value, exc_traceback = sys.exc_info()

                # Get the formatted traceback as a string
                traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

                # logging.info the traceback details
                for line in traceback_details:
                    logging.info(line.strip())
                download_retry += 1
                if download_retry > 2:
                    connection = mysql.connector.connect(**dbconfig)
                    cursor = connection.cursor()
                    Check_download_files_query = "select * from documents where cin=%s and company=%s and Download_Status='Downloaded' and form_data_extraction_needed='Y'"
                    Download_Check_Values = (Cin, CompanyName)
                    logging.info(Check_download_files_query % Download_Check_Values)
                    cursor.execute(Check_download_files_query, Download_Check_Values)
                    Downloaded_Files = cursor.fetchall()
                    cursor.close()
                    connection.close()
                    if len(Downloaded_Files) > 0:
                        logging.info("Exception occured but we got the required files")
                        try:
                            back_xpath = '//input[@type="submit" and @value="Back"]'
                            back_button = driver.find_element(By.XPATH, back_xpath)
                            back_button.click()
                            logging.info("Clicked on back button")
                        except NoSuchElementException:
                            back_xpath = '//input[@type="submit" and @value="Back"]'
                            back_button = driver.find_element(By.XPATH, back_xpath)
                            back_button.click()
                            logging.info("Trying to click but not clicking")
                        return True
                    else:
                        try:
                            back_xpath = '//input[@type="submit" and @value="Back"]'
                            back_button = driver.find_element(By.XPATH, back_xpath)
                            back_button.click()
                            logging.info("Clicked on back button")
                        except NoSuchElementException:
                            back_xpath = '//input[@type="submit" and @value="Back"]'
                            back_button = driver.find_element(By.XPATH, back_xpath)
                            back_button.click()
                            logging.info("Trying to click but not clicking")
                        return False
                else:
                    continue
    except Exception as e:
        logging.info(f"Exception {e} occured")
        connection = mysql.connector.connect(**dbconfig)
        cursor = connection.cursor()
        Check_download_files_query = "select * from documents where cin=%s and company=%s and Download_Status='Downloaded' and form_data_extraction_needed='Y'"
        Download_Check_Values = (Cin,CompanyName)
        logging.info(Check_download_files_query % Download_Check_Values)
        cursor.execute(Check_download_files_query,Download_Check_Values)
        Downloaded_Files = cursor.fetchall()
        check_total_files = "select * from documents where cin=%s and company=%s and form_data_extraction_needed='Y'"
        logging.info(check_total_files % Download_Check_Values)
        cursor.execute(check_total_files,Download_Check_Values)
        total_files = cursor.fetchall()
        cursor.close()
        connection.close()
        if len(Downloaded_Files) == len(total_files):
            logging.info("Exception occured but we got the required files")
            try:
                back_xpath = '//input[@type="submit" and @value="Back"]'
                back_button = driver.find_element(By.XPATH, back_xpath)
                back_button.click()
                logging.info("Clicked on back button")
            except NoSuchElementException:
                back_xpath = '//input[@type="submit" and @value="Back"]'
                back_button = driver.find_element(By.XPATH, back_xpath)
                back_button.click()
                logging.info("Trying to click but not clicking")
            return True
        else:
            return False
    else:
        return True


def update_form_extraction_status(db_config, cin, CompanyName):
    setup_logging()
    try:
        values = (cin, CompanyName, cin, CompanyName)
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query_MGT = """START TRANSACTION;

                                UPDATE documents AS t1
                                JOIN (
                                    SELECT id
                                    FROM documents
                                    WHERE document LIKE '%MGT%' AND `cin` = '{}' AND `company` = '{}' AND Category = 'Annual Returns and Balance Sheet eForms'
                                    ORDER BY STR_TO_DATE(document_date_year, '%d-%m-%Y') DESC
                                    LIMIT 1
                                    FOR UPDATE
                                ) AS t2 ON t1.id = t2.id
                                SET t1.form_data_extraction_needed = 'Y'
                                WHERE t1.document LIKE '%MGT%' AND `cin` = '{}' AND `company` = '{}' AND Category = 'Annual Returns and Balance Sheet eForms';

                                COMMIT;""".format(cin, CompanyName, cin, CompanyName)
            logging.info(update_query_MGT)
            cursor.execute(update_query_MGT)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Error Updating form extraction status for MGT {e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query_MSME = """UPDATE documents AS d1
JOIN (
    SELECT MAX(STR_TO_DATE(document_date_year, '%d-%m-%Y')) AS latest_date
    FROM documents
    WHERE cin = '{}' AND document LIKE '%MSME%'
    GROUP BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y'))
    ORDER BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y')) DESC
    LIMIT 2
) AS d2 ON STR_TO_DATE(d1.document_date_year, '%d-%m-%Y') = d2.latest_date
SET d1.form_data_extraction_needed = 'Y'
WHERE d1.cin = '{}' AND d1.document LIKE '%MSME%';""".format(cin, cin)
            cursor.execute(update_query_MSME)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for MSME{e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query_AOC = """UPDATE documents AS d1
JOIN (
    SELECT MAX(STR_TO_DATE(document_date_year, '%d-%m-%Y')) AS latest_date
    FROM documents
    WHERE cin = '{}' AND document LIKE '%AOC%' AND document not like '%AOC-4 CSR%'
    GROUP BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y'))
    ORDER BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y')) DESC
    LIMIT 4
) AS d2 ON STR_TO_DATE(d1.document_date_year, '%d-%m-%Y') = d2.latest_date
SET d1.form_data_extraction_needed = 'Y'
WHERE d1.cin = '{}' AND d1.document LIKE '%AOC%' AND d1.document not LIKE '%AOC-4 CSR%';""".format(cin, cin)
            logging.info(update_query_AOC)
            cursor.execute(update_query_AOC)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for AOC{e}")
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query_Change_of_name = "UPDATE documents set form_data_extraction_needed = 'Y' where LOWER(document) LIKE '%%change of name%%' and `cin`=%s and `company`=%s;"
            two_values = (cin, CompanyName)
            cursor.execute(update_query_Change_of_name, two_values)
            update_query_CHG = "UPDATE documents set form_data_extraction_needed = 'Y' where document LIKE '%%CHG-1%%' and `cin`=%s and `company`=%s;"
            cursor.execute(update_query_CHG, two_values)
            connection.commit()
            update_query_DIR = """UPDATE documents 
                SET form_data_extraction_needed = 'Y' 
                WHERE LOWER(document) LIKE '%%dir%%' 
                AND LOWER(document) NOT LIKE '%%directors%%' 
                AND LOWER(document) NOT LIKE '%%director%%' 
                AND cin = %s 
                AND company = %s;"""
            cursor.execute(update_query_DIR, two_values)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for change of name{e}")
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query_Xbrl_consolidated = """UPDATE documents AS d1
JOIN (
    SELECT MAX(STR_TO_DATE(document_date_year, '%d-%m-%Y')) AS latest_date
    FROM documents
    WHERE cin = '{}' AND document LIKE '%XBRL document in respect Consolidated%'
    GROUP BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y'))
    ORDER BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y')) DESC
    LIMIT 4
) AS d2 ON STR_TO_DATE(d1.document_date_year, '%d-%m-%Y') = d2.latest_date
SET d1.form_data_extraction_needed = 'Y'
WHERE d1.cin = '{}' AND d1.document LIKE '%XBRL document in respect Consolidated%';""".format(cin, cin)
            logging.info(update_query_Xbrl_consolidated)
            cursor.execute(update_query_Xbrl_consolidated)
            connection.commit()
            update_query_Xbrl = """UPDATE documents AS d1
JOIN (
    SELECT MAX(STR_TO_DATE(document_date_year, '%d-%m-%Y')) AS latest_date
    FROM documents
    WHERE cin = '{}' AND document LIKE '%XBRL financial statements%'
    GROUP BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y'))
    ORDER BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y')) DESC
    LIMIT 4
) AS d2 ON STR_TO_DATE(d1.document_date_year, '%d-%m-%Y') = d2.latest_date
SET d1.form_data_extraction_needed = 'Y'
WHERE d1.cin = '{}' AND d1.document LIKE '%XBRL financial statements%';""".format(cin, cin)
            logging.info(update_query_Xbrl)
            cursor.execute(update_query_Xbrl)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for XBRL{e}")
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            form11_query = "UPDATE documents set form_data_extraction_needed = 'Y' where document LIKE '%%Form11%%' and `cin`=%s and Category = 'Annual Returns and Balance Sheet eForms'"
            value_form11 = (cin,)
            logging.info(form11_query % value_form11)
            cursor.execute(form11_query, value_form11)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for form11 status updating {e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            form_fillip_query = "UPDATE documents set form_data_extraction_needed = 'Y' where document LIKE '%%Form Fillip%%' and `cin`=%s and Category = 'Incorporation Documents'"
            value_formfillip = (cin,)
            logging.info(form_fillip_query % value_formfillip)
            cursor.execute(form_fillip_query, value_formfillip)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for formfillip status updating {e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            form8_annual_query = """UPDATE documents AS d1
JOIN (
    SELECT MAX(STR_TO_DATE(document_date_year, '%d-%m-%Y')) AS latest_date
    FROM documents
    WHERE cin = '{}' AND document LIKE '%Form8%' AND Category = 'Annual Returns and Balance Sheet eForms'
    GROUP BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y'))
    ORDER BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y')) DESC
    LIMIT 4
) AS d2 ON STR_TO_DATE(d1.document_date_year, '%d-%m-%Y') = d2.latest_date
SET d1.form_data_extraction_needed = 'Y'
WHERE d1.cin = '{}' AND d1.document LIKE '%Form8%' AND Category = 'Annual Returns and Balance Sheet eForms' ;""".format(
                cin, cin)
            logging.info(form8_annual_query)
            cursor.execute(form8_annual_query)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for form8 annual status updating {e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            form8_interim_query = "UPDATE documents set form_data_extraction_needed = 'Y' where document LIKE '%%Form8%%' and `cin`=%s and Category = 'Charge Documents'"
            values_form8 = (cin,)
            logging.info(form8_interim_query % values_form8)
            cursor.execute(form8_interim_query, values_form8)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for form8 interim status updating {e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query_AOC_CFS = """UPDATE documents AS d1
        JOIN (
            SELECT MAX(STR_TO_DATE(document_date_year, '%d-%m-%Y')) AS latest_date
            FROM documents
            WHERE cin = '{}' AND document LIKE '%AOC-4 CFS NBFC%' AND document not like '%AOC-4 CSR%'
            GROUP BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y'))
            ORDER BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y')) DESC
            LIMIT 4
        ) AS d2 ON STR_TO_DATE(d1.document_date_year, '%d-%m-%Y') = d2.latest_date
        SET d1.form_data_extraction_needed = 'Y'
        WHERE d1.cin = '{}' AND d1.document LIKE '%AOC-4 CFS NBFC%' AND d1.document not LIKE '%AOC-4 CSR%';""".format(
                cin, cin)
            logging.info(update_query_AOC_CFS)
            cursor.execute(update_query_AOC_CFS)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for AOC CFS{e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query_AOC_nbfc = """UPDATE documents AS d1
        JOIN (
            SELECT MAX(STR_TO_DATE(document_date_year, '%d-%m-%Y')) AS latest_date
            FROM documents
            WHERE cin = '{}' AND document LIKE '%AOC-4 NBFC%' AND document not like '%AOC-4 CSR%'
            GROUP BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y'))
            ORDER BY YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y')) DESC
            LIMIT 4
        ) AS d2 ON STR_TO_DATE(d1.document_date_year, '%d-%m-%Y') = d2.latest_date
        SET d1.form_data_extraction_needed = 'Y'
        WHERE d1.cin = '{}' AND d1.document LIKE '%AOC-4 NBFC%' AND d1.document not LIKE '%AOC-4 CSR%';""".format(cin,
                                                                                                                  cin)
            logging.info(update_query_AOC_nbfc)
            cursor.execute(update_query_AOC_nbfc)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for AOC NBFC{e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            form_8_charge_query = "UPDATE documents set form_data_extraction_needed = 'Y' where document LIKE '%%Form 8%%' and `cin`=%s"
            value_form_8_charge_query = (cin,)
            logging.info(form_8_charge_query % value_form_8_charge_query)
            cursor.execute(form_8_charge_query, value_form_8_charge_query)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for form 8 charge status updating {e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            form_32_query = "UPDATE documents set form_data_extraction_needed = 'Y' where document LIKE '%%Form 32%%' and `cin`=%s"
            value_form_32_query = (cin,)
            logging.info(form_32_query % value_form_32_query)
            cursor.execute(form_32_query, value_form_32_query)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for form 32  status updating {e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            form_18_query = "UPDATE documents set form_data_extraction_needed = 'Y' where document LIKE '%%Form 18%%' and `cin`=%s"
            value_form_18_query = (cin,)
            logging.info(form_18_query % value_form_18_query)
            cursor.execute(form_18_query, value_form_18_query)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for form 18  status updating {e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            Certificate_of_Incorporation_Consequent_query = "UPDATE documents set form_data_extraction_needed = 'Y' where LOWER(document) LIKE '%%certificate of incorporation consequent%%' and `cin`=%s"
            value_Certificate_of_Incorporation = (cin,)
            logging.info(Certificate_of_Incorporation_Consequent_query % value_Certificate_of_Incorporation)
            cursor.execute(Certificate_of_Incorporation_Consequent_query, value_Certificate_of_Incorporation)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for form 18  status updating {e}")

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            INC22_query = "UPDATE documents set form_data_extraction_needed = 'Y' where document LIKE '%%INC-22%%' and `cin`=%s"
            value_INC22_query = (cin,)
            logging.info(INC22_query % value_INC22_query)
            cursor.execute(INC22_query, value_INC22_query)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Exception occured for form 18  status updating {e}")
    except Exception as e:
        logging.info(f"Error updating login status in the database: {str(e)}")
        return False
    else:
        return True


def check_already_Downloaded_db(dbconfig, Cin, filename, CompanyName):
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    query = "select * from documents where cin=%s and document=%s and company=%s"
    Value1 = Cin
    Value2 = filename
    Value3 = CompanyName
    cursor.execute(query, (Value1, Value2, Value3))
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result


def click_element_with_retry(driver, xpath, retry_count=3):
    setup_logging()
    for attempt in range(1, retry_count + 1):
        try:
            element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, xpath)))
            element.click()
            return True
        except:
            logging.info(f"Failed attempt {attempt} to click element with xpath: {xpath}")
            time.sleep(3)
    return False


def download_captcha_and_enter_text(CompanyName, driver, file_path, filename, Cin, dbconfig, category,download_file_path,duplicate_file_path,index_id,folder_path,
                                    retry_count=5):
    try:
        #options = webdriver.ChromeOptions()
        setup_logging()
        window_handles = driver.window_handles
        new_window_handle = window_handles[-1]
        driver.switch_to.window(new_window_handle)
        for attempt in range(1, retry_count + 1):
            try:
                captcha_image = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//img[@alt="Captcha" and @id="captcha"]')))
                captcha_image.screenshot("download_captcha.png")
                time.sleep(2)
                reader = easyocr.Reader(["en"])
                image = 'download_captcha.png'
                result = reader.readtext(image)
                captcha_text = " ".join([text[1] for text in result])
                logging.info(f"Captcha Text: {captcha_text}")
                time.sleep(3)

                captcha_enter_download = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//input[@type="text" and @name="userEnteredCaptcha"]')))
                captcha_enter_download.clear()
                time.sleep(1)
                captcha_enter_download.send_keys(captcha_text)
                time.sleep(2)
                Captcha_submit_xpath = '//input[@type="submit" and @value="Submit"]'
                click_element_with_retry(driver, Captcha_submit_xpath, retry_count)
                logging.info("Clicked on Submit")
                time.sleep(5)
                try:
                    no_captcha = driver.find_element(By.XPATH, '//li[text()="Please enter letters shown."]')
                    if no_captcha.is_displayed():
                        logging.info("No Captcha Entered")
                        time.sleep(2)
                        no_captcha_close_button = driver.find_element(By.XPATH, '//a[@class="boxclose" and @id="alertboxclose"]')
                        time.sleep(2)
                        no_captcha_close_button.click()
                        time.sleep(1)
                        continue
                except:
                    pass
                try:
                    incorrect_captcha = driver.find_element(By.XPATH, '//li[text()="Enter valid Letters shown."]')
                    if incorrect_captcha.is_displayed():
                        logging.info("Incorrect Captcha entered")
                        time.sleep(3)
                        close_box_button = driver.find_element(By.XPATH, '//a[@class="boxclose" and @id="msgboxclose"]')
                        time.sleep(2)
                        close_box_button.click()
                        time.sleep(2)
                        Keys.ESCAPE
                        continue
                    else:
                        break
                except:
                    break
            except Exception as e:
                logging.info(e)
                logging.info(f"Failed attempt {attempt} to download captcha and enter text")
        time.sleep(2)
        try:
            driver.close()
        except NoSuchWindowException:
            logging.info("Due to some issue window closed")
            original_window_handle = window_handles[0]
            # Switch back to the original window
            driver.switch_to.window(original_window_handle)
            return False
        # Assuming the original window is the first one in the list
        original_window_handle = window_handles[0]

        # Switch back to the original window
        driver.switch_to.window(original_window_handle)

        time.sleep(2)

        duplicate_file_folder = duplicate_file_path + '.pdf'
        duplicate_file_path_directory = os.path.dirname(duplicate_file_folder)
        if os.path.exists(duplicate_file_folder):
            duplicate_renamed_file = filename + "_" + str(index_id) + '.pdf'
            updated_file_path = os.path.join(duplicate_file_path_directory, duplicate_renamed_file)
            os.rename(duplicate_file_folder, updated_file_path)
            time.sleep(4)
            if os.path.exists(updated_file_path):
                logging.info("-------------------")
                moved_file_path = shutil.move(updated_file_path, folder_path)
                if os.path.exists(moved_file_path):
                    file_path = moved_file_path
                    logging.info('Success')

        if index_id == '' or index_id is None:
            if os.path.exists(download_file_path):
                os.rename(download_file_path,file_path)
                logging.info(f"Renamed from {download_file_path} to {file_path}")
        time.sleep(1)
        if index_id == '' or index_id is None:
            download_filename = os.path.basename(file_path)
            download_filename = str(download_filename).replace('.pdf', '')
            logging.info(download_filename)
            if download_filename != filename:
                logging.info("Download name not same so renaming")
                update_download_filename = filename
                old_file_path = file_path
                logging.info(f"old file path:{old_file_path}")
                file_path = str(file_path).replace(download_filename, update_download_filename)
                if '.pdf' not in file_path:
                    file_path = file_path + '.pdf'
                logging.info(f"New file path:{file_path}")
                os.rename(old_file_path, file_path)
            else:
                logging.info("Same name so not changing")
        if os.path.exists(file_path):
            connection = mysql.connector.connect(**dbconfig)
            cursor = connection.cursor()
            if index_id == '' or index_id is None:
                logging.info("Downloaded for duplicate")
                update_query = 'update documents set Download_Status=%s where cin=%s and document=%s and company=%s and path_index IS NULL'
                path_update_query = 'update documents set document_download_path=%s where cin=%s and document=%s and company=%s and path_index IS NULL'
                values_update = ('Downloaded', Cin, filename, CompanyName)
                values_path_update = (file_path, Cin, filename, CompanyName)
                logging.info(values_update)
                cursor.execute(update_query, values_update)
                cursor.execute(path_update_query, values_path_update)
                connection.commit()
            else:
                logging.info("Downloaded successfully")
                update_query = 'update documents set Download_Status=%s where cin=%s and document=%s and company=%s and path_index = %s'
                path_update_query = 'update documents set document_download_path=%s where cin=%s and document=%s and company=%s and path_index = %s'
                values_update = ('Downloaded', Cin, filename, CompanyName, index_id)
                values_path_update = (file_path, Cin, filename, CompanyName, index_id)
                logging.info(values_update)
                cursor.execute(update_query, values_update)
                cursor.execute(path_update_query, values_path_update)
                connection.commit()
            cursor.close()
            connection.close()
            return True
        else:
            if 'XBRL financial statements' in filename:
                other_attachments_directory = os.path.dirname(file_path)
                if os.path.exists(other_attachments_directory):
                    files = [os.path.join(other_attachments_directory, file) for file in os.listdir(other_attachments_directory)]
                    latest_file = max(files, key=os.path.getctime)
                    if 'XBRL financial statements' in latest_file:
                        updated_filename = filename
                        updated_file_path = os.path.join(other_attachments_directory, updated_filename)
                        if '.pdf' not in updated_file_path:
                            updated_file_path = updated_file_path + '.pdf'
                        os.rename(latest_file, updated_file_path)
                        logging.info(f"Renamed from {latest_file} to {updated_file_path}")
                        time.sleep(4)
                        if os.path.exists(updated_file_path):
                            connection = mysql.connector.connect(**dbconfig)
                            cursor = connection.cursor()
                            update_query = 'update documents set Download_Status=%s where cin=%s and document=%s and company=%s and path_index IS NULL'
                            path_update_query = 'update documents set document_download_path=%s where cin=%s and document=%s and company=%s and path_index IS NULL'
                            values_update = ('Downloaded', Cin, filename, CompanyName)
                            values_path_update = (updated_file_path, Cin, filename, CompanyName)
                            logging.info(values_update)
                            cursor.execute(update_query, values_update)
                            cursor.execute(path_update_query, values_path_update)
                            connection.commit()
                            cursor.close()
                            connection.close()
                        return True
                else:
                    logging.info("Not Downloaded successfully")
                    return False
            else:
                try:
                    file_path = str(file_path).replace('.pdf','.PDF')
                    if os.path.exists(file_path):
                        connection = mysql.connector.connect(**dbconfig)
                        cursor = connection.cursor()
                        update_query = 'update documents set Download_Status=%s where cin=%s and document=%s and company=%s and path_index IS NULL'
                        path_update_query = 'update documents set document_download_path=%s where cin=%s and document=%s and company=%s and path_index IS NULL'
                        values_update = ('Downloaded', Cin, filename, CompanyName)
                        values_path_update = (file_path, Cin, filename, CompanyName)
                        logging.info(values_update)
                        cursor.execute(update_query, values_update)
                        cursor.execute(path_update_query, values_path_update)
                        connection.commit()
                        cursor.close()
                        connection.close()
                        return True
                    else:
                        logging.info("Not Downloaded successfully")
                        return False
                except Exception as e:
                    logging.info("Not Downloaded successfully")
                    return False
    except Exception as e:
        logging.info(f"Exception Occured {e}")
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.info(line.strip())
        return False


def sign_out(driver):
    setup_logging()
    sign_out_button = driver.find_element(By.XPATH, '//a[@id="loginAnchor" and text()="Signout"]')

    if sign_out_button.is_displayed():
        sign_out_button.click()
        logging.info("Signed Out")
    time.sleep(4)
    if 'driver' in locals():
        driver.delete_all_cookies()
        driver.quit()

