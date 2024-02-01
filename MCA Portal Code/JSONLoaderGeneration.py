import json
import mysql.connector
from Config import create_main_config_dictionary
import os
import shutil
import sys
import traceback
import logging
from logging_config import setup_logging
import datetime
from datetime import date

def get_json_node_names(data, parent_name=''):
    node_names = []
    if isinstance(data, dict):
        for key, value in data.items():
            node_name = f"{parent_name}.{key}" if parent_name else key
            get_json_node_names(value, node_name)
            node_names.append(node_name)
    elif isinstance(data, list):
        for index, item in enumerate(data):
            node_name = f"{parent_name}[{index}]"
            get_json_node_names(item, node_name)
            node_names.append(node_name)
    return node_names
# Read JSON data from a file

def JSON_loader(db_config,config_json_file_path,cin,root_path,excel_path,sheet_name,receiptno):
    try:
        setup_logging()
        if not os.path.exists(config_json_file_path):
            raise Exception("Config file not exists")
        json_folder_name = 'JSons'
        json_folder_path = os.path.join(root_path,json_folder_name)
        if not os.path.exists(json_folder_path):
            os.makedirs(json_folder_path)
        current_date = datetime.date.today()
        today_date = current_date.strftime("%d-%m-%Y")
        file_name = receiptno
        json_file_path = os.path.join(json_folder_path,file_name)
        json_file_path = json_file_path + '.json'
        if not os.path.exists(json_file_path):
            shutil.copy(config_json_file_path,json_file_path)
        with open(json_file_path) as f:
            json_data = json.load(f)
        try:
            json_data["metatag"]["last_updated"] = date.today().strftime("%Y-%m-%d")
            json_data["metatag"]["MNS_receiptno"] = receiptno
        except Exception as e:
            logging.info(f"Exception occured while updating receipt no and date {e}")
        # Call the function with your JSON data, starting directly with the 'data' child nodes
        json_nodes = get_json_node_names(json_data.get('data', {}), parent_name='')
        config_dict_loader, status = create_main_config_dictionary(excel_path, sheet_name)
        #logging.info(json_nodes)
        for json_node in json_nodes:
            try:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                try:
                    company_query = config_dict_loader[json_node]
                except Exception as e:
                    continue
                if json_node == 'company' or json_node == 'authorized_signatories' or json_node == 'charge_sequence' or json_node == 'director_network' or json_node == 'open_charges' or json_node == 'open_charges_latest_event' or json_node == 'stock_exchange' or json_node == 'directors' or json_node == 'llp' or json_node == 'contribution_details' or json_node == 'financials' or json_node == 'nbfc_financials' or json_node == 'financial_parameters' or json_node == 'credit_ratings' or json_node == 'legal_history':
                    if json_node == 'contribution_details':
                       query = company_query.format(cin,cin,cin,cin)
                    else:
                       query = company_query.format(cin)
                    logging.info(query)
                    cursor.execute(query)
                else:
                    if json_node == 'contact_details':
                        values = (cin,cin)
                    elif json_node == 'subsidiary_entities' or json_node == 'associate_entities' or json_node == 'joint_ventures' or json_node == 'holding_entities':
                        values = (cin,cin,cin)
                    else:
                        values = (cin,)
                    logging.info(company_query % values)
                    cursor.execute(company_query, values)
                result_company = cursor.fetchall()
                json_string = ', '.join(result_company[0])
                #logging.info(json_string)

                # Convert the JSON string to a Python dictionary
                company_data = json.loads(json_string)

                # Replace the entire "company" dictionary with the new company data
                json_data["data"][json_node] = company_data

                # logging.info or use the updated JSON structure
                #logging.info(json.dumps(json_data, indent=2))
                with open(json_file_path, 'w') as json_file:
                    json.dump(json_data, json_file, indent=2)
                cursor.close()
                connection.close()
            except Exception as e:
                logging.error(f"Exception occured or no value for {json_node}{e}")

    except Exception as e:
        logging.error(f"Exception occured while preparing JSON Loader {e}")
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.error(line.strip())
        return False,None,e,[]
    else:
        return True,json_file_path,None,json_nodes
