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


def update_value_in_db(db_config, DIN, PAN, MobileNumber, Email, CIN):
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
            "max_tokens": 200,
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
    try:
        pdf_document = fitz.open(pdf_path)
        # xml_path = str(pdf_path).replace('.pdf', '.xml')
        #
        # with open(xml_path, "w", encoding="utf-8") as xml_file:
        #     xml_file.write("<?xml version='1.0' encoding='utf-8'?>\n")
        #     xml_file.write("<PDFData>\n")
        total_text = ''
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
                    print(f"Exception Occurred while converting image to text{e}")
                else:
                    os.remove(f"temp_image_{img_index}.png")
        print(total_text)
        shareholders_details = fetch_address_din_using_open_ai(total_text, config_dict)
        # print(shareholders_details)
        shareholders_details = eval(shareholders_details)
        if len(shareholders_details) != 0:
            for shareholder in shareholders_details:
                try:
                    if bool(shareholder['DIN']) or bool(shareholder['PAN']):
                        DIN = shareholder['DIN']
                        PAN = shareholder['PAN']
                        MobileNumber = shareholder['MobileNumber']
                        Email = shareholder['Email']
                        print('DIN-', DIN)
                        print('PAN-', PAN)
                        print('MobileNumber-', MobileNumber)
                        print('Email-', Email)
                        update_value_in_db(db_config, DIN, PAN, MobileNumber, Email, cin)
                except Exception as e:
                    return True
                    # DIN not available
    except Exception as e:
        print(f"Exception in finding address in MGT{e}")
        return False
    else:
        return True


def dir2_main(db_config, config_dict, output_directory, pdf_path, cin):
    try:
        # hidden_attachment = get_hidden_attachment(pdf_path, output_directory, None)
        # if hidden_attachment is not None:
        address = MGT_director_shareholdings_pdf_to_db(pdf_path, config_dict, db_config, cin)
        if address:
            return True
    except Exception as e:
        print(f"Exception in fetching address from MGT {e}")
    else:
        return True


# Cin = 'U01210MH1999PTC119449'
# output = r'C:\Users\BRADSOL\Documents\python\MCA_DataExtraction\Output'
# pdf = r'C:\Users\BRADSOL\Documents\python\MCA_DataExtraction\dir2_files\dir2_files\DIR-2- Sally (1).pdf'
# main_dict = create_main_config_dictionary(r'C:\Users\BRADSOL\Documents\python\MCA_DataExtraction\Config_Python.xlsx',
#                                           'OpenAI')
# config_dict = main_dict[0]
# db_config = {
#     "host": "localhost",
#     "user": "root",
#     "password": "",
#     "database": "classle3_mns_credit",
# }
# dir2_main(db_config, config_dict, output, pdf, Cin)
