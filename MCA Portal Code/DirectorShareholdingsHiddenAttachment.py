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

def update_value_in_db(db_config,name,no_of_shares,cin):
    try:
        db_connection = mysql.connector.connect(**db_config)
        db_cursor = db_connection.cursor()
        db_connection.autocommit = True

        check_name_query = "select * from authorized_signatories where cin = %s and LOWER(name) = %s and extracted_from = 'Master Data'"
        values = (cin,str(name).lower())
        print(check_name_query % values)
        db_cursor.execute(check_name_query,values)
        try:
            name_result = db_cursor.fetchone()
        except Exception as e:
            return
        logging.info(name_result)
        din = name_result[4]
        designation = name_result[6]
        shareholdings_query = "select * from director_shareholdings where cin = %s and LOWER(full_name) = %s"
        values = (cin, str(name).lower())
        print(shareholdings_query % values)
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
            year_query = "select * from director_shareholdings where cin = %s and din_pan = ''"
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

        if len(name_result) != 0:
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
                print(insert_query % insert_values)
                db_cursor.execute(insert_query,insert_values)

        db_cursor.close()
        db_connection.close()
    except Exception as e:
        logging.info(f"Exception {e} occured while inserting into db")


def image_to_text(image_path):
    # pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    return pytesseract.image_to_string(Image.open(image_path), lang='eng')

def get_hidden_attachment(input_pdf_path, output_path,file_name_hidden_pdf):
    setup_logging()
    os.makedirs(output_path, exist_ok=True)
    doc = fitz.open(input_pdf_path)

    item_name_dict = {}
    for each_item in doc.embfile_names():
        item_name_dict[each_item] = doc.embfile_info(each_item)["filename"]

    for item_name, file_name in item_name_dict.items():
        if 'shareholders' in str(file_name).lower() or 'shareholder' in str(file_name).lower() or 'share holders' in str(file_name).lower():
            out_pdf =  output_path + "\\" + file_name
            logging.info(out_pdf)
            fData = doc.embfile_get(item_name)
            with open(out_pdf, 'wb') as outfile:
                outfile.write(fData)
            return out_pdf
    return None



def fetch_address_din_using_open_ai(text,config_dict):
    try:
        url = config_dict['url']
        prompt = text + ' ' + config_dict['MGT_director_shareholdings_prompt']
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
        headers = {
            'Authorization': config_dict['api_key'],
            'Content-Type': 'application/json',
            'Cookie': config_dict['cookie_key']
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        json_response = response.json()
        print(json_response)
        content = json_response[config_dict['choices_keyword']][0][config_dict['message_keyword']][
            config_dict['content_keyword']]
        logging.info(content)
        return content
    except Exception as e:
        print(f"Exception occured in fetching address from OpenAI{e}")
        return []


def MGT_director_shareholdings_pdf_to_db(pdf_path,config_dict,db_config,cin):
    try:
        pdf_document = fitz.open(pdf_path)
        xml_path = str(pdf_path).replace('.pdf','.xml')

        with open(xml_path, "w", encoding="utf-8") as xml_file:
            xml_file.write("<?xml version='1.0' encoding='utf-8'?>\n")
            xml_file.write("<PDFData>\n")
        total_text = ''
        text = ''
        pdf_reader = PdfReader(pdf_path)
        for page in pdf_reader.pages:
            text += page.extract_text()
        total_text = text
        if total_text == '':
            for page_num in range(pdf_document.page_count):
                page = pdf_document.load_page(page_num)
                for img_index, image in enumerate(page.get_images(full=True)):
                    try:
                        xref = image[0]
                        base_image = pdf_document.extract_image(xref)
                        image_data = base_image["image"]

                        with open(f"temp_image_{img_index}.png", "wb") as img_file:
                            img_file.write(image_data)
                        text = image_to_text(f"temp_image_{img_index}.png")
                        total_text += text
                    except Exception as e:
                        print(f"Exception Occured while converting image to text{e}")
                    else:
                        os.remove(f"temp_image_{img_index}.png")
        print(total_text)
        shareholders_details = fetch_address_din_using_open_ai(total_text,config_dict)
        print(shareholders_details)
        shareholders_details = eval(shareholders_details)
        if len(shareholders_details) != 0:
            for shareholder in shareholders_details:
                name = shareholder['name']
                no_of_shares = shareholder['numberofsharesheld']
                no_of_shares = str(no_of_shares).replace(',','')
                no_of_shares = float(no_of_shares)
                print(name)
                print(no_of_shares)
                update_value_in_db(db_config,name,no_of_shares,cin)
    except Exception as e:
        print(f"Exception in finding address in MGT{e}")
        return False
    else:
        return True


def mgt_director_shareholdings_main(db_config,config_dict,output_directory,pdf_path,cin):
    try:
        hidden_attachment = get_hidden_attachment(pdf_path,output_directory,None)
        if hidden_attachment is not None:
            address = MGT_director_shareholdings_pdf_to_db(hidden_attachment,config_dict,db_config,cin)
            if address:
                return True
    except Exception as e:
        print(f"Exception in fetching address from MGT {e}")
    else:
        return True

# pdf_path = r"C:\Users\BRADSOL123\Desktop\XBRL\SD brakes\Form MGT-7A-31102022.pdf"
# excel_path = r"C:\Users\BRADSOL123\Documents\Python\Config\Config_Python.xlsx"
# sheet_name = 'OpenAI'
# config_dict,status = create_main_config_dictionary(excel_path,sheet_name)
# db_config = {
# "host": "162.241.123.123",
# "user": "classle3_deal_saas",
# "password": "o2i=hi,64u*I",
# "database": "classle3_mns_credit",
# }
# cin = 'U35203HR2011PTC043528'
# output_directory = r"C:\Users\BRADSOL123\Desktop\XBRL\SD brakes"
# mgt_director_shareholdings_main(db_config,config_dict,output_directory,pdf_path,cin)