import os
import shutil
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from PIL import Image
import pytesseract
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import sys
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import NoSuchElementException
import pyautogui
import mysql.connector
#from MCAPortalLogin import login_to_website
from easyocr import easyocr
import re
from datetime import datetime
import datetime
import shutil
from selenium.webdriver.common.action_chains import ActionChains

current_date = datetime.date.today()
today_date = current_date.strftime("%d-%m-%Y")
user_name = os.getlogin()


def initialize_driver(chrome_driver_path):
    options = Options()
    options.add_argument('--incognito')
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    return driver
def insert_Download_Details(driver,Cin, Company,db_config,category):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        my_workspace_button_xpath = '//a[@href="/mcafoportal/" and text()="My Workspace"]'
        if click_element_with_retry(driver, my_workspace_button_xpath):
            print("Clicked on My Workspace")
            time.sleep(5)
            k = 1
            page_number = 1
            main_page_number = 1
            while True:
                CompanyName = Company.lower()
                download_button_xpath = f'//table[@id="publicDocuments" and @name="publicDocuments"]/tbody/tr[contains(translate(., "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "{CompanyName}")]/td/a'
                if click_element_with_retry(driver, download_button_xpath):
                    print("Clicked on Download")
                    time.sleep(5)
                    break
                else:
                    try:
                        main_page_number_xpath = driver.find_element(By.XPATH,f'//span[@id="pg{k}" and @class="pg-selected"]')
                        main_page_number = main_page_number_xpath.text
                        main_page_number = int(main_page_number)
                        print("Page Number", main_page_number)
                    except NoSuchElementException:
                        print("No page number found")
                        pass
                    print("Loop Counter", k)
                    if k != page_number and k != 1:
                        print("Completed Navigation")
                        return False,None
                    try:
                        # Find and click the "Next" button using XPath
                        next_button = driver.find_element(By.XPATH, '//a[@id="publicDocuments_next"]')
                        next_button.click()
                        k = k + 1
                        continue
                    except ElementNotInteractableException or NoSuchElementException:
                        # If the button is not clickable, you've reached the last page
                        print("No Company Found")
                        return False, None
            certificates_button_xpath = f'//a[@class="dashboardlinks" and contains(text(),"{category}")]'
            current_date = datetime.date.today()
            if click_element_with_retry(driver, certificates_button_xpath):
                print("Clicked on Certificates")
                time.sleep(4)
                j = 1
                i = 0
                while True:
                    try:
                        page_number_xpath = driver.find_element(By.XPATH, f'//span[@id="pg{j}" and @class="pg-selected"]')
                        page_number = page_number_xpath.text
                        page_number = int(page_number)
                        print("Page Number" ,page_number)
                    except NoSuchElementException:
                        print("No page number found")
                        pass
                    print("Loop Counter" ,j)
                    if j != page_number and j != 1:
                        print("Completed Navigation")
                        break
                    #document_name_elements = driver.find_elements(By.XPATH,'//table[@id="reportsTab1"]/tbody/tr/td[1]/a')
                    for _ in range(0,10):
                        try:
                            document_xpath=f'//table[@id="reportsTab1"]/tbody/tr/td[1]/a[@id="open{i}\'"]'
                            print(document_xpath)
                            document_name_elements = driver.find_element(By.XPATH,document_xpath)
                            print(document_name_elements)
                            document_name = document_name_elements.text
                            print("Document Name", document_name)
                            date_pattern1 = r'\d{6}'  # Matches 6 digits (e.g., 041215)
                            date_pattern2 = r'\d{8}'  # Matches 8 digits (e.g., 01042021)
                            match_date_pattern1 = re.search(date_pattern1, document_name)
                            # print(match_date_pattern1)
                            match_date_pattern2 = re.search(date_pattern2, document_name)
                            # print(match_date_pattern2)

                            if match_date_pattern2:
                                date_format = match_date_pattern2.group()
                                print(date_format)
                                formatted_date = datetime.datetime.strptime(date_format, '%d%m%Y').strftime('%d-%m-%Y')
                                print("date", formatted_date)
                            else:
                                date_format = match_date_pattern1.group()
                                print(date_format)
                                formatted_date = datetime.datetime.strptime(date_format, '%d%m%y').strftime('%d-%m-%Y')
                                print("date", formatted_date)

                            duplicate_query = "select * from documents where cin=%s and document=%s and company=%s and Category=%s"
                            Value1 = Cin
                            Value2 = document_name
                            Value3 = Company
                            Value4 = category
                            cursor.execute(duplicate_query, (Value1, Value2, Value3, Value4))
                            result = cursor.fetchall()
                            print("Result from db", result)
                            if len(result) == 0:
                                query = "Insert into documents(cin,company,Category,document,document_date_year,form_data_extraction_status,created_date,created_by,form_data_extraction_needed,Page_Number,Download_Status) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                values = (
                                Cin, Company, category, document_name, formatted_date, 'Pending', current_date,
                                user_name, 'N', j,'Pending')
                                print(query % values)
                                cursor.execute(query, values)
                                connection.commit()
                            else:
                                print("Value already there so not inserting")
                            i=i+1
                        except Exception as e:
                            print(f"Exception Occured {e}")
                            i=i+1
                            continue
                    j = j + 1
                    try:
                        next_button_download = driver.find_element(By.XPATH, '//span[@id="next"]')
                        time.sleep(2)
                        next_button_download.click()
                        print("Next Button clicked")
                    except NoSuchElementException:
                        print("Next Button not clicking")
                        pass
        return True
    except Exception as e:
        print(f"Exception occured {e}")
        return False


def download_documents(driver,dbconfig,Cin,CompanyName,Category,rootpath):
    try:
        connection = mysql.connector.connect(**dbconfig)
        cursor = connection.cursor()
        j = 1
        first_xpath = '//span[@id="first"]'
        try:
            first_button = driver.find_element(By.XPATH, first_xpath)
            first_button.click()
        except Exception as e:
            print("No first button")
            pass
        folder_path = os.path.join(rootpath, Cin, CompanyName, Category)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        page_number = 1
        while True:
            try:
                page_number_xpath = driver.find_element(By.XPATH, f'//span[@id="pg{j}" and @class="pg-selected"]')
                page_number = page_number_xpath.text
                page_number = int(page_number)
                print("Page Number", page_number)
            except NoSuchElementException:
                print("No page number found")
                pass
            print("Loop Counter", j)
            if j != page_number and j != 1:
                print("Completed Navigation")
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
            download_details_query = 'select * from documents where cin=%s and company=%s and Category=%s and Page_Number=%s and Download_Status=%s'
            values = (Cin, CompanyName, Category, j, 'Pending')
            print(download_details_query % values)
            cursor.execute(download_details_query, values)
            result = cursor.fetchall()
            for element in result:
                filename = element[4]
                print(filename)
                filepath = os.path.join(folder_path, filename)
                filepath = filepath + '.pdf'
                file_xpath = f'//a[contains(text(),"{filename}")]'
                try:
                    file_download_button = driver.find_element(By.XPATH, file_xpath)
                    file_download_button.click()
                except:
                    continue
                download = download_captcha_and_enter_text(CompanyName, driver, filepath, filename, Cin, dbconfig,
                                                           Category, retry_count=10)
                if download:
                    print("Downloaded Successfully")
                    update_query = 'update documents set Download_Status=%s where cin=%s and document=%s and company=%s'
                    path_update_query = 'update documents set document_download_path=%s where cin=%s and document=%s and company=%s'
                    values_update = ('Downloaded', Cin, filename, CompanyName)
                    values_path_update = (filepath, Cin, filename, CompanyName)
                    print(values_update)
                    cursor.execute(update_query, values_update)
                    cursor.execute(path_update_query,values_path_update)
                    connection.commit()
                    if 'MGT-7' in filename or 'MGT-7A' in filename:
                        MGT_folder = os.path.join(folder_path,'MGT')
                        if not os.path.exists(MGT_folder):
                            os.makedirs(MGT_folder)
                        try:
                            shutil.copy(filepath, MGT_folder)
                        except Exception as e:
                            print(f"Error {e}")
                else:
                    print("Not Downloaded")
                    continue
            check_pending_query = 'select * from documents where cin=%s and company=%s and Category=%s and Page_Number=%s and Download_Status=%s'
            pending_values = (Cin, CompanyName, Category, j, 'Pending')
            print(check_pending_query % pending_values)
            cursor.execute(check_pending_query, pending_values)
            pending_result = cursor.fetchall()
            print(pending_result)
            if len(pending_result) == 0:
                j = j + 1
                try:
                    next_button_download = driver.find_element(By.XPATH, '//span[@id="next"]')
                    time.sleep(2)
                    next_button_download.click()
                    print("Next Button clicked")
                except NoSuchElementException:
                    next_button_download = driver.find_element(By.XPATH, '//span[@id="next"]')
                    time.sleep(2)
                    next_button_download.click()
                    print("Next Button not clicking")
                    pass
            else:
                continue
        return True
    except Exception as e:
        print(f"Excpetion {e} occured")
        return False



def update_form_extraction_status(db_config, cin,CompanyName):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        get_details_query = "select * from documents where cin=%s and company=%s"
        values = (cin, CompanyName)
        print(get_details_query % values)
        cursor.execute(get_details_query, values)
        file_details = cursor.fetchall()
        print(file_details)
        i=0
        for condition_range in [(0, 365), (365, 730),(730,1460),(1460,2920)]:
            near_date_flag = False
            start_range, end_range = condition_range

            for details in file_details:
                file_date = details[5]
                file_name = details[4]
                filepath = details[8]
                date = datetime.datetime.strptime(file_date, '%d-%m-%Y')
                today_date_with_timestamp = datetime.datetime.today()
                days_difference = (today_date_with_timestamp - date).days
                print(days_difference)
                if 'MGT' in file_name:
                    if start_range <= days_difference <= end_range:
                        update_query = "update documents set form_data_extraction_needed=%s where document=%s"
                        update_values = ('Y', file_name)
                        print(update_query % update_values)
                        cursor.execute(update_query, update_values)
                        connection.commit()
                        print("Latest MGT File Found")
                        near_date_flag = True
                        break
                elif 'MSME' in file_name:
                    if start_range <= days_difference <= end_range:
                        update_query = "update documents set form_data_extraction_needed=%s where document=%s"
                        update_values = ('Y', file_name)
                        print(update_query % update_values)
                        cursor.execute(update_query, update_values)
                        connection.commit()
                        print(f"{i} Latest File Found for MSME")
                        i = i+1
                    if i == 2:
                        near_date_flag = True
                        break

                elif 'AOC-4' in file_name or 'AOC-4 CFS NBFC' in file_name or 'AOC-4 NBFC' in file_name:
                    if start_range <= days_difference <= end_range:
                        update_query = "update documents set form_data_extraction_needed=%s where document=%s"
                        update_values = ('Y', file_name)
                        print(update_query % update_values)
                        cursor.execute(update_query, update_values)
                        connection.commit()
                        print(f"{i} Latest File Found for AOC")
                        i = i+1
                    if i == 4:
                        near_date_flag = True
                        break

                elif 'CHG' in file_name or 'DIR' in file_name or 'CHANGE OF NAME' in file_name:
                    print("Proocessing all files")
                    update_query = "update documents set form_data_extraction_needed=%s where document=%s"
                    update_values = ('Y', file_name)
                    print(update_query % update_values)
                    cursor.execute(update_query, update_values)
                    connection.commit()
                    near_date_flag = True

                else:
                    print("Proocessing all files")
                    update_query = "update documents set form_data_extraction_needed=%s where document=%s"
                    update_values = ('Y', file_name)
                    print(update_query % update_values)
                    cursor.execute(update_query, update_values)
                    connection.commit()
                    near_date_flag = True
                #else condition put temporarily
            if near_date_flag:
                print("Successfully found all files so breaking")
                break
            if not near_date_flag:
                print("Latest File not found checking other files in DB")
        return True
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
        return False
    finally:
        cursor.close()
        connection.close()


def check_already_Downloaded_db(dbconfig, Cin, filename, CompanyName):
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    query = "select * from documents where cin=%s and document=%s and company=%s"
    Value1 = Cin
    Value2 = filename
    Value3 = CompanyName
    cursor.execute(query, (Value1, Value2, Value3))
    result = cursor.fetchall()
    return result


def click_element_with_retry(driver, xpath, retry_count=3):
    for attempt in range(1, retry_count + 1):
        try:
            element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, xpath)))
            element.click()
            return True
        except:
            print(f"Failed attempt {attempt} to click element with xpath: {xpath}")
            time.sleep(3)
    return False


def download_captcha_and_enter_text(CompanyName, driver, file_path, filename, Cin, dbconfig, category,
                                    retry_count=5):
    try:
        #options = webdriver.ChromeOptions()
        window_handles = driver.window_handles
        new_window_handle = window_handles[-1]
        driver.switch_to.window(new_window_handle)
        options = webdriver.ChromeOptions()
        for attempt in range(1, retry_count + 1):
            try:
                prefs = {"download.default_directory": file_path}
                options.add_experimental_option("prefs", prefs)
                time.sleep(2)
                captcha_image = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//img[@alt="Captcha" and @id="captcha"]')))
                captcha_image.screenshot("download_captcha.png")
                time.sleep(2)
                # captcha_text = pytesseract.image_to_string(Image.open("download_captcha.png"))
                reader = easyocr.Reader(["en"])
                image = 'download_captcha.png'
                result = reader.readtext(image)
                captcha_text = " ".join([text[1] for text in result])
                print(f"Captcha Text: {captcha_text}")
                time.sleep(3)

                captcha_enter_download = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//input[@type="text" and @name="userEnteredCaptcha"]')))
                captcha_enter_download.clear()
                time.sleep(1)
                captcha_enter_download.send_keys(captcha_text)
                time.sleep(2)
                Captcha_submit_xpath = '//input[@type="submit" and @value="Submit"]'
                click_element_with_retry(driver, Captcha_submit_xpath, retry_count)
                print("Clicked on Submit")
                time.sleep(5)
                try:
                    incorrect_captcha = driver.find_element(By.XPATH, '//li[text()="Enter valid Letters shown."]')
                    if incorrect_captcha.is_displayed():
                        print("Incorrect Captcha entered")
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
            except:
                print(f"Failed attempt {attempt} to download captcha and enter text")
        time.sleep(5)

        #time.sleep(2)
        #pyautogui.typewrite(file_path)
        #time.sleep(3)
        #pyautogui.hotkey('enter')
        #time.sleep(1)
        #pyautogui.hotkey('enter')

        time.sleep(5)
        driver.close()

        # Assuming the original window is the first one in the list
        original_window_handle = window_handles[0]

        # Switch back to the original window
        driver.switch_to.window(original_window_handle)

        time.sleep(4)

        if os.path.exists(file_path):
            Download_Status = "Downloaded"
            print("Downloaded successfully")
            # insert_Download_Details(Cin, CompanyName, category, filename, dbconfig, file_path, date)
            return True
        else:
            print("Not Downloaded successfully")
            return False
    except Exception as e:
        print(f"Exception Occured {e}")
        return False


def sign_out(driver):
    sign_out_button = driver.find_element(By.XPATH, '//a[@id="loginAnchor" and text()="Signout"]')

    if sign_out_button.is_displayed():
        sign_out_button.click()
        print("Signed Out")
    time.sleep(4)
    if 'driver' in locals():
        driver.delete_all_cookies()
        driver.quit()

