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


def update_value_in_db(db_config,name,date,cin):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    check_name_query = "select * from name_history where cin = %s and name = %s and date = %s"
    values = (cin,name,date)
    print(check_name_query % values)
    db_cursor.execute(check_name_query,values)
    name_result = db_cursor.fetchall()

    if len(name_result) == 0:
        insert_query = "INSERT INTO name_history(cin,name,date) VALUES (%s,%s,%s)"
        insert_values = (cin,name,date)
        print(insert_query % insert_values)
        db_cursor.execute(insert_query,insert_values)
    db_cursor.close()
    db_connection.close()


def extract_text_from_pdf(pdf_path):
    text = ""
    try:
        with open(pdf_path, "rb") as file:
            pdf_reader = PyPDF2.PdfReader(file)
            num_pages = len(pdf_reader.pages)

            for page_num in range(num_pages):
                page = pdf_reader.pages[page_num]
                text += page.extract_text()

    except Exception as e:
        print(f"Error: {e}")

    return text



def fetch_name_details_using_open_ai(text,config_dict):
    try:
        url = config_dict['url']
        prompt = text + ' ' + config_dict['change_name_prompt']
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


def fresh_name_pdf_to_db(pdf_path,config_dict,db_config,cin):
    try:
        pdf_text = extract_text_from_pdf(pdf_path)
        name = fetch_name_details_using_open_ai(pdf_text,config_dict)
        name = eval(name)
        new_name = name['new_name']
        date = name['date']
        print(new_name,date)
        update_value_in_db(db_config,new_name,date,cin)
    except Exception as e:
        print(f"Exception occured in updating fresh name {e}")
        return False
    else:
        return True

def fresh_name_main(db_config,config_dict,pdf_path,cin):
    try:
        address = fresh_name_pdf_to_db(pdf_path,config_dict,db_config,cin)
        if address:
            return True
    except Exception as e:
        print(f"Exception in fetching address from MGT {e}")
    else:
        return True
