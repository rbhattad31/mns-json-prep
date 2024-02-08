import fitz
import os
from logging_config import setup_logging
import logging
import xml.etree.ElementTree as ET
import pytesseract
from PIL import Image
import json
import requests
import mysql.connector
from Config import create_main_config_dictionary
import PyPDF2
from PyPDF2 import PdfReader
import re
from CaptureTextUsingOCR import extract_text_from_pdf
import sys
import traceback

def check_name_probability(db_config,cin,input_name):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    director_query = "select name,din,designation from authorized_signatories where cin = %s and extracted_from = 'Master Data'"
    value = (cin,)
    logging.info(director_query % value)
    cursor.execute(director_query,value)
    directors = cursor.fetchall()
    cursor.close()
    connection.close()
    for director in directors:
        dbname = director[0]
        din = director[1]
        designation = director[2]
        # Convert names to lowercase
        input_name = input_name.lower()
        dbname = dbname.lower()

        # Find intersection and union
        intersection = set(input_name) & set(dbname)
        union = set(input_name) | set(dbname)

        # Calculate Jaccard similarity coefficient
        jaccard_similarity = len(intersection) / len(union)

        # Convert to percentage
        percentage_match = jaccard_similarity * 100

        # Output the result as a number
        logging.info(f"Percentage Match of {input_name} is {percentage_match}")
        if percentage_match > 75:
            return dbname,din,designation,director


def update_value_in_db(db_config,name,no_of_shares,cin):
    try:
        setup_logging()
        db_connection = mysql.connector.connect(**db_config)
        db_cursor = db_connection.cursor()
        db_connection.autocommit = True

        check_name_query = "select * from authorized_signatories where cin = %s and LOWER(name) = %s and extracted_from = 'Master Data'"
        values = (cin,str(name).lower())
        logging.info(check_name_query % values)
        db_cursor.execute(check_name_query,values)
        try:
            name_result = db_cursor.fetchone()
        except Exception as e:
            return
        logging.info(name_result)
        if name_result is None or len(name_result) == 0:
            logging.info("Going for name percentage check")
            updated_name,din,designation,updated_director_list = check_name_probability(db_config,cin,name)
            logging.info(updated_name)
            name = updated_name
        else:
            din = name_result[4]
            designation = name_result[6]
            updated_director_list = []
        shareholdings_query = "select * from director_shareholdings where cin = %s and LOWER(full_name) = %s"
        values = (cin, str(name).lower())
        logging.info(shareholdings_query % values)
        db_cursor.execute(shareholdings_query,values)
        shareholdings_result = db_cursor.fetchall()

        paid_up_capital_query = "select paidup_capital from shareholdings_summary where cin = %s"
        paid_up_capital_value = (cin,)
        logging.info(paid_up_capital_query % paid_up_capital_value)
        db_cursor.execute(paid_up_capital_query,paid_up_capital_value)
        paid_up_capital = db_cursor.fetchone()[0]
        try:
            percentage_holding = (float(no_of_shares)/float(paid_up_capital))*100
            percentage_holding = round(float(percentage_holding),2)
            logging.info(percentage_holding)
        except Exception as e:
            logging.info(f"Error in calculating percentage holding {e}")
            percentage_holding = None
        try:
            year_query = "select * from director_shareholdings where cin = %s and (din_pan = '' or din_pan IS NULL)"
            year_values = (cin,)
            logging.info(year_query % year_values)
            db_cursor.execute(year_query,year_values)
            year_result = db_cursor.fetchone()
            year = year_result[6]
            financial_year = year_result[7]
        except Exception as e:
            logging.info(f"Error in capturing year and financial year {e}")
            year = ''
            financial_year = ''

        if name_result is not None or updated_director_list is not None or len(name_result) != 0 or len(updated_director_list) != 0:
            if len(shareholdings_result) != 0:
                update_query = "UPDATE director_shareholdings set no_of_shares = %s where cin = %s and LOWER(full_name) = %s"
                update_values = (no_of_shares,cin,str(name).lower())
                logging.info(update_query % update_values)
                db_cursor.execute(update_query,update_values)

                din_update_query = "UPDATE director_shareholdings set din_pan = %s where cin = %s and LOWER(full_name) = %s"
                din_update_values = (din, cin, str(name).lower())
                logging.info(din_update_query % din_update_values)
                db_cursor.execute(din_update_query, din_update_values)

                percentage_holding_query = "UPDATE director_shareholdings set percentage_holding = %s where cin = %s and LOWER(full_name) = %s"
                percentage_holding_value = (percentage_holding, cin, str(name).lower())
                logging.info(percentage_holding_query % percentage_holding_value)
                db_cursor.execute(percentage_holding_query,percentage_holding_value)

                designation_query = "UPDATE director_shareholdings set designation = %s where cin = %s and LOWER(full_name) = %s"
                designation_value = (designation, cin, str(name).lower())
                logging.info(designation_query % designation_value)
                db_cursor.execute(designation_query, designation_value)

                year_query = "UPDATE director_shareholdings set year = %s where cin = %s and LOWER(full_name) = %s"
                year_values = (year,cin,str(name).lower())
                logging.info(year_query % year_values)
                db_cursor.execute(year_query,year_values)

                financial_year_query = "UPDATE director_shareholdings set financial_year = %s where cin = %s and LOWER(full_name) = %s"
                financial_year_values = (financial_year, cin, str(name).lower())
                logging.info(financial_year_query % financial_year_values)
                db_cursor.execute(financial_year_query, financial_year_values)

            else:
                insert_query = "INSERT INTO director_shareholdings(cin,full_name,no_of_shares,din_pan,percentage_holding,designation,year,financial_year) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                insert_values = (cin,name,no_of_shares,din,percentage_holding,designation,year,financial_year)
                logging.info(insert_query % insert_values)
                db_cursor.execute(insert_query,insert_values)

        db_cursor.close()
        db_connection.close()
    except Exception as e:
        logging.info(f"Exception {e} occured while inserting into db")
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.info(line.strip())


def image_to_text(image_path):
    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    return pytesseract.image_to_string(Image.open(image_path), lang='eng')


def get_hidden_attachment(input_pdf_path, output_path,file_name_hidden_pdf):
    setup_logging()
    os.makedirs(output_path, exist_ok=True)
    doc = fitz.open(input_pdf_path)

    item_name_dict = {}
    for each_item in doc.embfile_names():
        item_name_dict[each_item] = doc.embfile_info(each_item)["filename"]

    for item_name, file_name in item_name_dict.items():
        if 'shareholders' in str(file_name).lower() or 'shareholder' in str(file_name).lower() or 'share holders' in str(file_name).lower() or 'share holder' in str(file_name).lower() or 'los' in str(file_name).lower() or 'shareholding' in str(file_name).lower() or 'share' in str(file_name).lower() or 'shl' in str(file_name).lower():
            out_pdf =  output_path + "\\" + file_name
            logging.info(out_pdf)
            fData = doc.embfile_get(item_name)
            with open(out_pdf, 'wb') as outfile:
                outfile.write(fData)
            return out_pdf
    return None



def fetch_address_din_using_open_ai(text,config_dict):
    try:
        setup_logging()
        url = config_dict['url']
        prompt = text + '\n' + '-------------------' + '\n' + config_dict['MGT_director_shareholdings_prompt']
        logging.info(prompt)
        payload = json.dumps({
            "model": "gpt-3.5-turbo",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.7,
            "max_tokens": 1000,
            "top_p": 1,
            "frequency_penalty": 0,
            "presence_penalty": 0
        })
        print(payload)
        headers = {
            'Authorization': config_dict['api_key'],
            'Content-Type': 'application/json',
            'Cookie': config_dict['cookie_key']
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        json_response = response.json()
        logging.info(json_response)
        content = json_response[config_dict['choices_keyword']][0][config_dict['message_keyword']][
            config_dict['content_keyword']]
        logging.info(content)
        return content
    except Exception as e:
        logging.info(f"Exception occured in fetching address from OpenAI{e}")
        return []


def MGT_director_shareholdings_pdf_to_db(pdf_path,config_dict,db_config,cin):
    try:
        setup_logging()
        pdf_document = fitz.open(pdf_path)
        xml_path = str(pdf_path).replace('.pdf','.xml')

        with open(xml_path, "w", encoding="utf-8") as xml_file:
            xml_file.write("<?xml version='1.0' encoding='utf-8'?>\n")
            xml_file.write("<PDFData>\n")
        text = ''
        bucket_name = config_dict['bucket_name']
        total_text = extract_text_from_pdf(pdf_path, bucket_name, pdf_path, config_dict)
        logging.info(f"OCR Captured text {total_text}")
        if total_text is None:
            pdf_reader = PdfReader(pdf_path)
            for page in pdf_reader.pages:
                logging.info("Taking text using normal approach")
                text += page.extract_text()
            total_text = text
            logging.info(total_text)
        shareholders_details = fetch_address_din_using_open_ai(total_text,config_dict)
        logging.info(shareholders_details)
        shareholders_details = eval(shareholders_details)
        salutation_list = str(config_dict['salutation_list']).split(',')
        salutation_list = [str(x).strip() for x in salutation_list]
        if len(shareholders_details) != 0:
            for shareholder in shareholders_details:
                name = shareholder['name']
                for salutation in salutation_list:
                    logging.info(salutation)
                    if str(salutation).lower() in name:
                        logging.info(f"{salutation} present in {name}")
                        name = re.sub(fr'\b{re.escape(salutation)}(?=\s|\b)', '', name, flags=re.IGNORECASE)
                        name = name.strip()
                        break
                no_of_shares = shareholder['numberofsharesheld']
                no_of_shares = str(no_of_shares).replace(',','')
                if no_of_shares is not None:
                    if no_of_shares == '':
                        no_of_shares = 0
                else:
                    no_of_shares = 0
                no_of_shares = float(no_of_shares)
                logging.info(name)
                logging.info(no_of_shares)
                update_value_in_db(db_config,name,no_of_shares,cin)
    except Exception as e:
        logging.info(f"Exception in finding address in MGT{e}")
        return False
    else:
        return True


def mgt_director_shareholdings_main(db_config,config_dict,output_directory,pdf_path,cin):
    try:
        setup_logging()
        hidden_attachment = get_hidden_attachment(pdf_path,output_directory,None)
        if hidden_attachment is not None:
            address = MGT_director_shareholdings_pdf_to_db(hidden_attachment,config_dict,db_config,cin)
            if address:
                return True
    except Exception as e:
        logging.info(f"Exception in fetching address from MGT {e}")
    else:
        return True
