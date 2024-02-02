import requests
import json
import mysql.connector
from Config import create_main_config_dictionary
import logging
from logging_config import setup_logging

def split_openai(config_dict,address):
    setup_logging()
    url = config_dict['url']
    prompt = address + ' ' + config_dict['Prompt']
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
        "max_tokens": 100,
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
    content = json_response[config_dict['choices_keyword']][0][config_dict['message_keyword']][config_dict['content_keyword']]
    logging.info(content)
    return content


def split_address(cin,config_dict,db_config):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    address_query = "select address from authorized_signatories where cin=%s"
    value = (cin,)
    logging.info(address_query % value)
    cursor.execute(address_query,value)
    address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    for address in address_list:
        address_to_split = address[0]
        logging.info(address_to_split)
        if str(address_to_split).lower() != 'null' and address_to_split is not None:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            connection.autocommit = True
            splitted_address = split_openai(config_dict,address_to_split)
            update_query = 'update authorized_signatories set splitted_address = %s where cin = %s and address = %s'
            address_values = (splitted_address,cin,address_to_split)
            logging.info(update_query % address_values)
            cursor.execute(update_query,address_values)
            cursor.close()
            connection.close()
