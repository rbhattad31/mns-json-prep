import traceback
import mysql.connector
import mysql.connector
from selenium.webdriver.common.by import By
import os
import datetime
from Config import create_main_config_dictionary
from DownloadFile import insert_Download_Details,download_documents,update_form_extraction_status
from PDFToXML import PDFtoXML,CheckHiddenAttachemnts,fetch_form_extraction_file_data_from_table
from MCAPortalLogin import login_to_website
from XMLToExcel import xml_to_excel


current_date = datetime.date.today()

# Format the date as dd-MM-yyyy
formatted_date = current_date.strftime("%d-%m-%Y")

def connect_to_database(db_config):
    try:
        # Connect to the MySQL server
        connection = mysql.connector.connect(**db_config)
        db_cursor = connection.cursor()
        if connection.is_connected():
            return connection,db_cursor
        else:
            print("Connection is not established.")
            return None,None

    except mysql.connector.Error as error:
        print("Error:", error)
        return None

def fetch_order_data_from_table(connection):
    try:
        if connection:
            cursor = connection.cursor()

            # Construct the SQL query
            query = "SELECT * FROM orders where workflow_status=%s"
            #value1 = ("MCA_Payment_Success")
            cursor.execute(query, ("Download_Pending",))

            # Get the column names from the cursor description
            column_names = [desc[0] for desc in cursor.description]

            # Fetch all the rows
            rows = cursor.fetchall()
            Status="Pass"
            return column_names, rows,Status

    except mysql.connector.Error as error:
        print("Error:", error)
        return None


def update_status(user,Status,db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        query = "UPDATE orders SET workflow_status = %s WHERE payment_by_user = %s"
        print(query)
        cursor.execute(query, (Status,user,))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
    finally:
        cursor.close()
        connection.close()


def update_logout_status(username, db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        query = "UPDATE user_details SET login_status = 'No' WHERE user_name = %s"
        cursor.execute(query, (username,))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
    finally:
        cursor.close()
        connection.close()


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

def fetch_user_credentials_from_db(db_config,input_user):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        query = "SELECT user_name, password, login_status FROM user_details"
        cursor.execute(query)
        records = cursor.fetchall()
        #input_user = 'Shalumns1'
        print("Input recieved from Main" ,input_user)

        for db_username, db_password, login_status in records:
            if db_username == input_user and login_status == 'No':
                print("Ready to Login")
                Status="Pass"
                break
            else:
                Status="Fail"

        if Status=="Pass":
            #user_credentials={'username':db_username,
                          #'password':db_password}
            #print(user_credentials)
            return db_username,db_password,Status
        else:
            return None,None,Status

    except Exception as e:
        print(f"Error fetching user credentials from the database: {str(e)}")
        return []

    finally:
        cursor.close()
        connection.close()


def main():
    # Replace 'config.xlsx' with the path to your Excel file
    excel_file = r"C:\MCA Portal\Config.xlsx"
    Sheet_name="Sheet1"
    #chrome_driver_path = r"C:\Users\BRADSOL123\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
    try:
        #root_path, retry_count, Host, User, Password, Database,Url,config_status = config(excel_file)
        config_dict,config_status = create_main_config_dictionary(excel_file,Sheet_name)
        root_path = config_dict['Root path']
        if not os.path.exists(root_path):
            os.makedirs(root_path)
        retry_count = config_dict['Retry Count']
        Host = config_dict['Host']
        db_User = config_dict['User']
        Password = config_dict['Password']
        Database = config_dict['Database']
        chrome_driver_path=config_dict['chrome_driver_path']
        Url=config_dict['Url']
        map_file_path = config_dict['map_file_path']
        map_file_sheet_name = config_dict['map_file_sheet_name']
        Subsidiary_Config = config_dict['Subsidiary_Config']
        Business_Activity_Config = config_dict['Business_Activity_Config']
        Subsidiary_Config_Sheet_Name = config_dict['Subsidiary_Config_Sheet_Name']
        Business_Activity_Config_Sheet_Name = config_dict['Business_Activity_Config_Sheet_Name']

        if config_status=="Pass":
            print("Going to next")
            db_config = {
                "host": Host,
                "user": db_User,
                "password": Password,
                "database": Database,
            }
            connection,db_cursor = connect_to_database(db_config)
            if connection:
                column_names, rows, Status=fetch_order_data_from_table(connection)
                if column_names and rows and Status == "Pass":
                    # Loop through the rows and print the data
                    last_logged_in_user=None
                    for row in rows:
                        Cin, CompanyName,User = row[2], row[3],row[15]
                        print('Cin:', Cin)
                        print('CompanyName:', CompanyName)
                        print('User' , User)
                        if last_logged_in_user is None or last_logged_in_user!=User:
                            if 'driver' in locals():
                                sign_out(driver)
                                update_logout_status(last_logged_in_user, db_config)
                            username,password,Status=fetch_user_credentials_from_db(db_config,User)
                            if Status == "Pass":
                                Login,driver = login_to_website(Url, chrome_driver_path, username, password, db_config)
                            else:
                                print("Already Logged in")
                                #update_status(User,"Exception",db_config)
                                continue
                            print(Login)
                            if Login == "Pass":
                                print("Successfully Logged in")
                                last_logged_in_user=User
                            else:
                                update_status(User,"Login Failed",db_config)
                                continue
                        else:
                            print("Already Logged in so carrying on with the same credentials")
                        #driver = initialize_driver(chrome_driver_path)
                        category_list = ['Annual Returns and Balance Sheet eForms']
                        for item in category_list:
                            download_status = insert_Download_Details(driver, Cin, CompanyName, db_config, item)
                            if download_status:
                                file_download=download_documents(driver, db_config, Cin, CompanyName, item, root_path)
                            else:
                                continue
                            if file_download:
                                PDF_Form_Extraction = update_form_extraction_status(db_config,Cin,item,CompanyName)
                                if PDF_Form_Extraction:
                                    files_to_be_extracted,Fetch_File_Data_Status = fetch_form_extraction_file_data_from_table(connection,Cin,CompanyName,item)
                                    if Fetch_File_Data_Status == "Pass":
                                        for files in files_to_be_extracted:
                                            pdf_path = files[8]
                                            file_name = files[4]
                                            folder_path = os.path.join(root_path,Cin,CompanyName,item)
                                            xml_file_path,PDF_to_XML = PDFtoXML(folder_path, pdf_path, file_name)
                                            if PDF_to_XML:
                                                hidden_attachments = CheckHiddenAttachemnts(xml_file_path, folder_path,
                                                                                            pdf_path, file_name)
                                                print(hidden_attachments)
                                                all_xml_list = [xml_file_path]
                                                all_xml_list.extend(hidden_attachments)
                                                print(all_xml_list)
                                                if len(hidden_attachments) != 0:
                                                    for xml in all_xml_list:
                                                        if "Subsidiaries" in xml or "Holding" in xml or "Associate" in xml or "Joint Venture" in xml:
                                                            map_file_path = Subsidiary_Config
                                                            map_file_sheet_name = Subsidiary_Config_Sheet_Name
                                                        elif "Business Activity" in xml:
                                                            map_file_path = Business_Activity_Config
                                                            map_file_sheet_name = Business_Activity_Config_Sheet_Name
                                                        else:
                                                            pass
                                                        output_excel_name = str(xml).replace('.xml', '.xlsx')
                                                        output_excel_path = os.path.join(folder_path, output_excel_name)
                                                        xml_to_excel(db_cursor, config_dict, map_file_path,
                                                                     map_file_sheet_name, xml, output_excel_path, Cin,
                                                                     CompanyName)
                                                        print("XMl file Processed", xml)
                                                    cursor = connection.cursor()
                                                    update_data_extraction_query = "update documents set form_data_extraction_status='Success' where document=%s and cin=%s"
                                                    update_data_extraction_values = (file_name, Cin)
                                                    cursor.execute(update_data_extraction_query,
                                                                   update_data_extraction_values)
                                                    connection.commit()
                                                else:
                                                    output_excel_name = str(xml_file_path).replace('.xml', '.xlsx')
                                                    output_excel_path = os.path.join(folder_path, output_excel_name)
                                                    xml_to_excel(db_cursor, config_dict, map_file_path,
                                                                 map_file_sheet_name,
                                                                 xml_file_path, output_excel_path, Cin, CompanyName)
                                                    print("XMl file Processed", xml_file_path)
                                                    update_data_extraction_query = "update documents set form_data_extraction_status='Success' where document=%s and cin=%s"
                                                    update_data_extraction_values = (file_name, Cin)
                                                    cursor.execute(update_data_extraction_query,
                                                                   update_data_extraction_values)
                                                    connection.commit()
                    if 'driver' in locals():
                        sign_out(driver)
                        update_logout_status(last_logged_in_user,db_config)
                else:
                    print("Failed to Fetch Data")
            else:
                print("DB Connection Failed")
        else:
            print("Config Failed")
    except FileNotFoundError:
        print(f"Configuration file '{excel_file}' not found. Please make sure it exists.")
    except Exception as e:
        traceback.print_exc()
        print(f"An unexpected error occurred: {str(e)}")


if __name__ == "__main__":
    main()
