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
from CaptureTextUsingOCR import extract_text_from_pdf

def update_value_in_db(db_config,din,address,cin):
    try:
        setup_logging()
        db_connection = mysql.connector.connect(**db_config)
        db_cursor = db_connection.cursor()
        db_connection.autocommit = True

        check_address_query = "select * from authorized_signatories where cin = %s and din = %s"
        values = (cin,din)
        logging.info(check_address_query % values)
        db_cursor.execute(check_address_query,values)
        result = db_cursor.fetchall()

        if len(result) != 0:
            update_query = "UPDATE authorized_signatories set address = %s where cin = %s and din = %s"
            update_values = (address,cin,din)
            logging.info(update_query % update_values)
            db_cursor.execute(update_query,update_values)

        db_cursor.close()
        db_connection.close()
    except Exception as e:
        logging.info(f"Exception occured while inserting into Db {e}")


def image_to_text(image_path):
    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    return pytesseract.image_to_string(Image.open(image_path), lang='eng')


def MGT_Address_pdf_to_db(pdf_path,config_dict,db_config,cin):
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
                text += page.extract_text()
            total_text = text
            logging.info(total_text)
        directors_details = fetch_address_din_using_open_ai(total_text,config_dict)
        logging.info(directors_details)
        directors_details = eval(directors_details)
        if len(directors_details) != 0:
            for director in directors_details:
                din = director['din']
                address = director['address']
                logging.info(din)
                logging.info(address)
                update_value_in_db(db_config,din,address,cin)
    except Exception as e:
        logging.info(f"Exception in finding address in MGT{e}")
        return False
    else:
        return True


def fetch_address_din_using_open_ai(text,config_dict):
    try:
        setup_logging()
        url = config_dict['url']
        prompt = text + ' ' + config_dict['MGT_address_prompt']
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
        logging.info(json_response)
        content = json_response[config_dict['choices_keyword']][0][config_dict['message_keyword']][
            config_dict['content_keyword']]
        logging.info(content)
        return content
    except Exception as e:
        print(f"Exception occured in fetching address from OpenAI{e}")
        return []


def get_hidden_attachment(input_pdf_path, output_path,file_name_hidden_pdf):
    setup_logging()
    os.makedirs(output_path, exist_ok=True)
    doc = fitz.open(input_pdf_path)

    item_name_dict = {}
    for each_item in doc.embfile_names():
        item_name_dict[each_item] = doc.embfile_info(each_item)["filename"]

    for item_name, file_name in item_name_dict.items():
        if 'directors' in str(file_name).lower() or 'lod' in str(file_name).lower() or 'director' in str(file_name).lower():
            out_pdf =  output_path + "\\" + file_name
            logging.info(out_pdf)
            fData = doc.embfile_get(item_name)
            with open(out_pdf, 'wb') as outfile:
                outfile.write(fData)
            return out_pdf
    return None


def mgt_address_main(db_config,config_dict,output_directory,pdf_path,cin):
    try:
        setup_logging()
        hidden_attachment = get_hidden_attachment(pdf_path,output_directory,None)
        if hidden_attachment is not None:
            address = MGT_Address_pdf_to_db(hidden_attachment,config_dict,db_config,cin)
            if address:
                return True
    except Exception as e:
        logging.info(f"Exception in fetching address from MGT {e}")
    else:
        return True
