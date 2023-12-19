import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from PIL import Image
import pytesseract
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from easyocr import easyocr
import mysql.connector
import sys
import traceback
from selenium.webdriver.common.keys import Keys
from logging_config import setup_logging
import logging
def update_login_status(username, db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        query = "UPDATE user_details SET login_status = 'Yes' WHERE user_name = %s"
        cursor.execute(query, (username,))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
    finally:
        cursor.close()
        connection.close()

def login_to_website(url, chrome_driver_path,username,password,db_config):
    try:
        setup_logging()
        options = Options()
        options.headless = False
        options.add_argument('--start-maximized')
        service = Service(chrome_driver_path)
        pytesseract.pytesseract.tesseract_cmd = r"C:\Users\BRADSOL123\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
        retry_count = 10
        for attempt in range(1, retry_count + 1):
            driver = webdriver.Chrome(service=service, options=options)
            driver.get(url)
            time.sleep(2)
            driver.set_window_size(1920, 1080)
            driver.maximize_window()
            logging.info(f"Attempt {attempt}:")
            try:
                upload_forms_button = driver.find_element(By.XPATH,
                                                          '//input[@type="button" and @value="Upload e-Forms"]')
                if upload_forms_button.is_displayed():
                    logging.info("Already logged in!")
                    sign_out_button = driver.find_element(By.XPATH, '//a[@id="loginAnchor" and text()="Signout"]')
                    sign_out_button.click()
                    logging.info("Logged out. Retrying login...")
            except:
                pass
                # Find and input the username
            for attempt in range(1, retry_count + 1):
                username_input = driver.find_element(By.XPATH, '//input[@type="text" and @id="userName"]')
                username_input.clear()
                username_input.send_keys(username)
                logging.info("UserName Entered", username)
                time.sleep(5)

                # Get the password from environment variables
                password_input = driver.find_element(By.XPATH, '//input[@type="password" and @id="password"]')
                password_input.clear()
                password_input.send_keys(password)
                logging.info("Password Entered", password)
                time.sleep(5)

                # Get the captcha image element
                captcha_image = driver.find_element(By.XPATH, '//img[@alt="Captcha" and @id="captcha"]')

                # Capture the screenshot of the captcha image
                captcha_image.screenshot("captcha.png")
                time.sleep(5)

                # Perform OCR on the captcha image
                # captcha_text = pytesseract.image_to_string(Image.open("captcha.png"))
                reader = easyocr.Reader(["en"])
                image = 'captcha.png'
                result = reader.readtext(image)
                captcha_text = " ".join([text[1] for text in result])
                logging.info(f"Captcha Text: {captcha_text}")

                # Enter captcha text
                captcha_input = driver.find_element(By.XPATH, '//input[@type="text" and @id="userEnteredCaptcha"]')
                captcha_input.clear()
                captcha_input.send_keys(captcha_text)
                logging.info("Entered Captcha")
                time.sleep(5)
                sign_in = driver.find_element(By.XPATH, '//input[@type="submit" and @value="Sign In"]')
                sign_in.click()
                logging.info("Clicked on Sign in")
                time.sleep(5)
                try:
                    incorrect_captcha = driver.find_element(By.XPATH, '//li[text()="Enter valid Letters shown."]')
                    if incorrect_captcha.is_displayed():
                        logging.info("Incorrect Captcha entered")
                        time.sleep(3)
                        close_box_button = driver.find_element(By.XPATH, '//a[@class="boxclose" and @id="msgboxclose"]')
                        time.sleep(2)
                        close_box_button.click()
                        time.sleep(2)
                        continue
                except:
                    break
                try:
                    no_captcha = driver.find_element(By.XPATH,'//li[text()="Please enter letters shown."]')
                    if no_captcha.is_displayed():
                        logging.info("No Captcha Entered")
                        time.sleep(2)
                        no_captcha_close_button = driver.find_element((By.XPATH,'//a[@class="boxclose" and @id="alertboxclose"]'))
                        time.sleep(2)
                        no_captcha_close_button.click()
                        time.sleep(1)
                        continue
                except:
                    break
                

                # Click on the Sign In button

            try:
                upload_forms_button = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, '//input[@type="button" and @value="Upload e-Forms"]'))
                )
                Status = "Pass"

                # Update the LoginStatus to 'yes' in the database
                update_login_status(username, db_config)

                time.sleep(5)
                return Status, driver,options,'Login Failed'
            except:
                logging.info("Login failed. Retrying...")
                driver.close()
            # If login fails after the last attempt, print a message
        if attempt == retry_count:
            print("Login failed after 3 attempts.")
            Status = "Fail"
            return Status, driver,options,'Login Failed'

            # Add a delay before the next attempt
        time.sleep(10)

    except Exception as e:
        logging.warning(f"Error Logging in {e}")
        Status = "Fail"
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # Print the traceback details
        for line in traceback_details:
            logging.error(line.strip())
        return Status,driver,options,e


    # Close the browser window
    """
    if 'driver' in locals():
        driver.delete_all_cookies()
        driver.quit()
    """
