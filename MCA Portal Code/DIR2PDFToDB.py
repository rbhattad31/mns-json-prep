import PyPDF2
import fitz
import os
from logging_config import setup_logging
import logging
import xml.etree.ElementTree as ET
import os
from pdf2image import convert_from_path
import pytesseract
from PyPDF2 import PdfReader
import pytesseract
from PIL import Image
import json
import requests
import mysql.connector
from Config import create_main_config_dictionary
import sys
import traceback
from CaptureTextUsingOCR import extract_text_from_pdf


def update_value_in_db(db_config, DIN, PAN, MobileNumber, Email, CIN):
    try:
        setup_logging()
        db_connection = mysql.connector.connect(**db_config)
        db_cursor = db_connection.cursor()
        db_connection.autocommit = True
        # Either DIN or PAN will be available, CIN will be constant.

        if bool(DIN):
            # Authorized_Signatures
            DIN_Query = "select * from authorized_signatories where din = %s and cin = %s"
            values = (DIN, CIN)
            print(DIN_Query % values)
            db_cursor.execute(DIN_Query, values)
            DIN_Result = db_cursor.fetchall()

            if len(DIN_Result) != 0:
                # update Query:
                update_query = "UPDATE authorized_signatories set pan = %s,phone_number = %s,email = %s where din = %s and cin = %s"
                update_values = (PAN, MobileNumber, Email, DIN, CIN)
                print(update_query % update_values)
                db_cursor.execute(update_query, update_values)
            # else:
            #     # Insert Query:
            #     insert_query = "INSERT INTO authorized_signatories(din,pan,phone_number,email,cin) VALUES (%s,%s,%s,%s,%s)"
            #     insert_values = (DIN, PAN, MobileNumber, Email, CIN)
            #     print(insert_query % insert_values)
            #     db_cursor.execute(insert_query, insert_values)

            # director_network
            DirectorNetwork_query = "select * from director_network where din = %s and cin = %s"
            values = (DIN, CIN)
            print(DirectorNetwork_query % values)
            db_cursor.execute(DirectorNetwork_query, values)
            DirectorNetwork_result = db_cursor.fetchall()

            if len(DirectorNetwork_result) != 0:
                # update Query:
                update_query = "UPDATE director_network set pan = %s where din = %s and cin = %s"
                update_values = (PAN, DIN, CIN)
                print(update_query % update_values)
                db_cursor.execute(update_query, update_values)
            # else:
            #     # Insert Query:
            #     insert_query = "INSERT INTO director_network(din,pan,cin) VALUES (%s,%s,%s)"
            #     insert_values = (DIN, PAN, CIN)
            #     print(insert_query % insert_values)
            #     db_cursor.execute(insert_query, insert_values)

        else:
            PAN_Query = "select * from authorized_signatories where pan = %s and cin = %s"
            values = (PAN, CIN)
            print(PAN_Query % values)
            db_cursor.execute(PAN_Query, values)
            PAN_Result = db_cursor.fetchall()

            if len(PAN_Result) != 0:
                # update Query:
                update_query = "UPDATE authorized_signatories set phone_number = %s,email = %s where pan = %s and cin = %s"
                update_values = (MobileNumber, Email, PAN, CIN)
                print(update_query % update_values)
                db_cursor.execute(update_query, update_values)
            # else:
            #     # Insert Query:
            #     insert_query = "INSERT INTO authorized_signatories(cin,din,pan,phone_number,email) VALUES (%s,%s,%s,%s,%s)"
            #     insert_values = (CIN, DIN, PAN, MobileNumber, Email)
            #     print(insert_query % insert_values)
            #     db_cursor.execute(insert_query, insert_values)

        db_cursor.close()
        db_connection.close()
    except Exception as e:
        logging.info(f"Exception ocured while inserting into Db {e}")


def image_to_text(image_path):
    # pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    return pytesseract.image_to_string(Image.open(image_path), lang='eng')


def get_pdf_text(pdf_path):
    pdf_reader = PyPDF2.PdfReader(pdf_path)
    text = ''
    for page in pdf_reader.pages:
        text += page.extract_text()
    result = text
    return result


def get_hidden_attachment(input_pdf_path, output_path, file_name_hidden_pdf):
    setup_logging()
    os.makedirs(output_path, exist_ok=True)
    doc = fitz.open(input_pdf_path)

    item_name_dict = {}
    for each_item in doc.embfile_names():
        item_name_dict[each_item] = doc.embfile_info(each_item)["filename"]

    for item_name, file_name in item_name_dict.items():
        if 'shareholders' in str(file_name).lower():
            out_pdf = output_path + "\\" + file_name
            logging.info(out_pdf)
            fData = doc.embfile_get(item_name)
            with open(out_pdf, 'wb') as outfile:
                outfile.write(fData)
            return out_pdf
    return None


def fetch_address_din_using_open_ai(text, config_dict):
    setup_logging()
    try:
        url = config_dict['url']
        prompt = text + ' ' + config_dict['DIR2_prompt']
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


def MGT_director_shareholdings_pdf_to_db(pdf_path, config_dict, db_config, cin):
    setup_logging()
    try:
        pdf_document = fitz.open(pdf_path)
        text = ''
        pdf_reader = PdfReader(pdf_path)
        bucket_name = config_dict['bucket_name']
        total_text = extract_text_from_pdf(pdf_path,bucket_name,pdf_path,config_dict)
        logging.info(f"OCR Captured text {total_text}")
        if total_text is None:
            for page in pdf_reader.pages:
                text += page.extract_text()
            total_text = text
            logging.info(f"Plain Captured text: {total_text}")
        shareholders_details = fetch_address_din_using_open_ai(total_text, config_dict)
        logging.info(shareholders_details)
        shareholders_details = eval(shareholders_details)
        if len(shareholders_details) != 0:
            for shareholder in shareholders_details:
                try:
                    if bool(shareholder['DIN']) or bool(shareholder['PAN']):
                        DIN = shareholder['DIN']
                        PAN = shareholder['PAN']
                        MobileNumber = shareholder['MobileNumber']
                        Email = shareholder['Email']
                        logging.info('DIN-', DIN)
                        logging.info('PAN-', PAN)
                        logging.info('MobileNumber-', MobileNumber)
                        logging.info('Email-', Email)
                        update_value_in_db(db_config, DIN, PAN, MobileNumber, Email, cin)
                except Exception as e:
                    return True
                    # DIN not available
    except Exception as e:
        logging.info(f"Exception in finding address in MGT{e}")
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.error(line.strip())
        return False
    else:
        return True


def dir2_main(db_config, config_dict, output_directory, pdf_path, cin):
    try:
        setup_logging()
        # hidden_attachment = get_hidden_attachment(pdf_path, output_directory, None)
        # if hidden_attachment is not None:
        address = MGT_director_shareholdings_pdf_to_db(pdf_path, config_dict, db_config, cin)
        if address:
            return True
    except Exception as e:
        logging.info(f"Exception in fetching address from MGT {e}")
    else:
        return True

