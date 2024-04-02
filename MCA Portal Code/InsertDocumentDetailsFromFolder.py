import mysql.connector
import os
from datetime import datetime
import datetime
import re
import sys
import traceback
from DownloadFile import update_form_extraction_status
from AOC_XBRL_HiddenAttachment_Generation import xbrl_xml_attachment
current_date = datetime.datetime.now()
today_date = current_date.strftime("%d/%m/%Y %H:%M:%S'")
print(today_date)
current_user = os.getlogin()


def convert_date_format(date):
    # Check if the year is in YY format
    if len(date) == 6:
        year = '20' + date[-2:]
    else:
        year = date[-4:]
    return f"{date[:2]}-{date[2:4]}-{year}"


def insert_document_details(db_config,cin,root_path,company_name):
    try:
        cin_folder_path = os.path.join(root_path,cin)
        if os.path.exists(cin_folder_path):
            folders = [folder for folder in os.listdir(cin_folder_path) if os.path.isdir(os.path.join(cin_folder_path, folder))]
            if len(folders) <= 1:
                raise Exception(f"No Files found for {cin}")
            for folder in folders:
                try:
                    folder_path = os.path.join(cin_folder_path,folder)
                    files = [files for files in os.listdir(folder_path) if files.endswith('.pdf')]
                    for file in files:
                        try:
                            file_path = os.path.join(folder_path,file)
                            if '.pdf' not in file_path:
                                new_file_path = file_path + '.pdf'
                                os.rename(file_path,new_file_path)
                                file_path = new_file_path
                            pattern = r'(?<=[-_])(?:0[1-9]|[12][0-9]|3[01])(?:0[1-9]|1[0-2])(?:\d{4}|\d{2})'
                            matches = re.findall(pattern, file)
                            print(matches)
                            if matches:
                                formatted_date = convert_date_format(matches[0])
                            else:
                                formatted_date = None
                            print(formatted_date)
                            connection = mysql.connector.connect(**db_config)
                            cursor = connection.cursor()
                            duplicate_query = "select * from documents where cin=%s and document=%s and company=%s and Category=%s and document_download_path = %s"
                            Value1 = cin
                            Value2 = file
                            Value3 = company_name
                            Value4 = folder
                            Value5 = file_path
                            cursor.execute(duplicate_query, (Value1, Value2, Value3, Value4,Value5))
                            result = cursor.fetchall()
                            print("Result from db", result)
                            if len(result) == 0:
                                query = "Insert into documents(cin,company,Category,document,document_date_year,form_data_extraction_status,created_date,created_by,form_data_extraction_needed,Page_Number,Download_Status,DB_insertion_status,document_download_path) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                values = (
                                cin, company_name, folder, file, formatted_date, 'Pending', current_date, current_user, 'N',
                                1, 'Downloaded', 'Pending',file_path)
                                print(query % values)
                                cursor.execute(query, values)
                                connection.commit()
                            else:
                                print(f"Document already present here in db with {file} at path - {file_path}")
                            cursor.close()
                            connection.close()
                        except Exception as e:
                            print(f"Exception occured in {file} {e}")
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            # Get the formatted traceback as a string
                            traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

                            # logging.info the traceback details
                            for line in traceback_details:
                                print(line.strip())
                except Exception as e:
                    print(f"Exception occurred in folder {folder} {e}")
            update_extraction_status = update_form_extraction_status(db_config, cin, company_name)
            xbrl_hidden_xml_attachment = xbrl_xml_attachment(db_config,cin,company_name)
            if update_extraction_status:
                print("Successfully changed form extraction status")
            if xbrl_hidden_xml_attachment:
                print("Successfully XBRL hidden attachments generated")
        else:
            raise Exception(f"Folder not found for cin {cin}")
    except Exception as e:
        print(f"Exception occurred in inserting document details {e}")
        return False
    else:
        return True
