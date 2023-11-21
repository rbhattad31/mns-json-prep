import json
import mysql.connector
from Config import create_main_config_dictionary
import os
import shutil
import sys
import traceback
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

def JSON_loader(db_config,config_json_file_path,cin,root_path):
    try:
        if not os.path.exists(config_json_file_path):
            raise Exception("Config file not exists")

        json_folder_path = os.path.join(root_path,cin)
        if not os.path.exists(json_folder_path):
            os.makedirs(json_folder_path)
        json_file_path = os.path.join(json_folder_path,cin)
        json_file_path = json_file_path + '.json'
        shutil.copy(config_json_file_path,json_file_path)
        with open(json_file_path) as f:
            json_data = json.load(f)

        # Call the function with your JSON data, starting directly with the 'data' child nodes
        json_nodes = get_json_node_names(json_data.get('data', {}), parent_name='')
        excel_path = r"C:\MCA Portal\Config.xlsx"
        sheet_name = 'JSON Loader Queries'
        config_dict_loader, status = create_main_config_dictionary(excel_path, sheet_name)
        print(json_nodes)
        for json_node in json_nodes:
            try:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                try:
                    company_query = config_dict_loader[json_node]
                except Exception as e:
                    continue
                if json_node == 'contact_details':
                    values = (cin,cin)
                elif json_node == 'subsidiary_entities' or json_node == 'associate_entities' or json_node == 'joint_ventures':
                    values = (cin,cin,cin)
                else:
                    values = (cin,)
                print(company_query % values)
                cursor.execute(company_query, values)
                result_company = cursor.fetchall()
                json_string = ', '.join(result_company[0])
                print(json_string)

                # Convert the JSON string to a Python dictionary
                company_data = json.loads(json_string)

                # Replace the entire "company" dictionary with the new company data
                json_data["data"][json_node] = company_data

                # Print or use the updated JSON structure
                print(json.dumps(json_data, indent=2))
                with open(json_file_path, 'w') as json_file:
                    json.dump(json_data, json_file, indent=2)
                cursor.close()
                connection.close()
            except Exception as e:
                print(f"Exception occured or no value for {json_node}{e}")

    except Exception as e:
        print(f"Exception occured while preparing JSON Loader {e}")
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # Print the traceback details
        for line in traceback_details:
            print(line.strip())
        return False,None,e
    else:
        return True,json_file_path,None



# config_json_file_path = 'Config JSON Non-LLP.json'
# db_config = {
#         "host": "162.241.123.123",
#         "user": "classle3_deal_saas",
#         "password": "o2i=hi,64u*I",
#         "database": "classle3_mns_credit",
#     }
# JSON_loader(db_config,config_json_file_path,'U14100GJ2021PLC121538',r'C:\MCA Portal')