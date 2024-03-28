import time
import requests
import json
import mysql.connector
from Config import create_main_config_dictionary
import os
import pandas as pd
import re
import sys
import traceback
from datetime import datetime
import logging
from logging_config import setup_logging
import xml.etree.ElementTree as Et


def get_single_value_from_xml(xml_root, parent_node, child_node):
    try:
        setup_logging()
        if child_node == 'nan':
            elements = xml_root.findall(f'.//{parent_node}')
        else:
            elements = xml_root.findall(f'.//{parent_node}//{child_node}')

        for element in elements:
            if element.text is None:
                continue
            if element.text is not None:
                if '\r' in str(element.text):
                    return str(element.text).replace('\r', '\n')
                else:
                    return str(element.text)
        return None
    except Exception as e:
        logging.info(f"An error occurred: {e}")
        return None


def get_pan_number_second_file(db_config,cin):
    try:
        setup_logging()
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        mgt_file_query = "select * from documents where cin = %s and form_data_extraction_needed = 'Y' and document like '%%MGT%%' and form_data_extraction_status = 'Success'"
        values = (cin,)
        logging.info(mgt_file_query % values)
        cursor.execute(mgt_file_query,values)
        mgt_result = cursor.fetchall()
        cursor.close()
        connection.close()
        if len(mgt_result) == 2:
            logging.info("Fetching pan number from second file")
            second_file = mgt_result[1]
            logging.info(second_file)
            path = second_file[8]
            xml_file_path = str(path).replace('.pdf','.xml')
            filename = second_file[4]
            if 'MGT-7A'.lower() in str(filename).lower():
                parent_node = 'ZMCA_MGT_7A'
                child_node = 'IT_PAN_OF_COMPNY'
            else:
                parent_node = 'ZMCA_NCA_MGT_7'
                child_node = 'IT_PAN_OF_COMPNY'
            try:
                xml_tree = Et.parse(xml_file_path)
                xml_root = xml_tree.getroot()
                # xml_str = Et.tostring(xml_root, encoding='unicode')
            except Exception as e:
                raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))
            pan_number = get_single_value_from_xml(xml_root,parent_node,child_node)
            return pan_number
        else:
            logging.info(f"Only one Success MGT File found for {cin}")
            return None
    except Exception as e:
        logging.error(f"Error in fetching pan number from second file {e}")
        return None


def update_database_single_value_GST(db_config, table_name, cin_column_name, cin_value,company_name_column_name,company_name, column_name, column_value,gst_number):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    # if column_name == "financials_auditor" and num_elements == 1:
    #     first_key = next(iter(json_dict))
    #     first_value_json_list = json_dict[first_key]
    #     json_string = json.dumps(first_value_json_list)
    #     column_value = json_string
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and {}='{}'".format(table_name, cin_column_name, cin_value,company_name_column_name,company_name,'gstin',gst_number)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {}='{}'".format(table_name, column_name,
                                                                                      column_value, cin_column_name,
                                                                                      cin_value,
                                                                                      company_name_column_name,
                                                                                      company_name,
                                                                                      'gstin',
                                                                                      gst_number)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      column_value)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def insert_gst_number(db_config,config_dict,cin,company,root_path):
    try:
        setup_logging()
        url = config_dict['pan_to_gst_url']
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        if len(cin) == 21:
            pan_number_query = "select pan from Company where cin=%s"
        elif len(cin) == 8:
            pan_number_query = "select pan_number from LLP where llpin=%s"
        else:
            raise Exception("Invalid Cin to fetch GST Details")
        values = (cin,)
        cursor.execute(pan_number_query,values)
        pan_number = cursor.fetchone()[0]
        logging.info(pan_number)
        if pan_number is not None:
            if pan_number == '':
                logging.info(f"Pan Number not found for cin {cin}")
                error_message = 'PAN Number not found'
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                update_query = "update orders set gst_exception_message = %s where cin = %s"
                values_cin = (error_message, cin)
                logging.info(update_query % values_cin)
                cursor.execute(update_query, values_cin)
                connection.commit()
                cursor.close()
                connection.close()
                raise Exception(error_message)
        else:
            logging.info(f"Pan Number not found for cin {cin}")
            error_message = 'PAN Number not found'
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query = "update orders set gst_exception_message = %s where cin = %s"
            values_cin = (error_message, cin)
            logging.info(update_query % values_cin)
            cursor.execute(update_query, values_cin)
            connection.commit()
            cursor.close()
            connection.close()
            raise Exception(error_message)

        payload = json.dumps({
            "panNumber": pan_number
        })
        headers = {
            'Subscriptionkey': config_dict['api_subscription_key'],
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response == '':
            logging.info(f"No response from API")
            raise Exception(f"No Response from API for cin {cin}")
        try:
            json_response = response.json()
            details = json_response['result']
            message = details['message']
            logging.info(message)
            if str(message).lower() == 'gstin not found':
                logging.info("GSTin not found so fetching pan number from second file")
                new_pan_number = get_pan_number_second_file(db_config,cin)
                logging.info(f"New Pan Number {new_pan_number}")
                if new_pan_number is not None:
                    payload = json.dumps({
                        "panNumber": new_pan_number
                    })
                    headers = {
                        'Subscriptionkey': config_dict['api_subscription_key'],
                        'Content-Type': 'application/json'
                    }
                    response = requests.request("POST", url, headers=headers, data=payload)
                    if response == '':
                        logging.info(f"No response from API")
                        raise Exception(f"No Response from API for cin {cin}")
        except Exception as e:
            logging.info(f"Exception in fetching gst number from second pan number {e}")

        if response.status_code == 200:
            try:
                json_response = response.json()
                result = json_response['result']
                logging.info(json_response)
            except Exception as e:
                logging.info(response)
                raise Exception('No Response from API')
            logging.info(result)
            output_df = []
            for gst_details in result:
                gst_number = gst_details[config_dict['gst_key_response']]
                gst_status = gst_details[config_dict['status_key_response']]
                if gst_status == config_dict['status_active_keyword']:
                    logging.info(gst_number, gst_status)
                    df_map = fetch_gst_details(config_dict,gst_number,gst_status)
                    output_df.append(df_map)
            output_folder_path = os.path.join(root_path,cin)
            if not os.path.exists(output_folder_path):
               os.makedirs(output_folder_path)
            output_file_path = os.path.join(output_folder_path,'gst.xlsx')
            #output_file_path = r"C:\Users\mns-admin\Documents\Power Automate\MNS Credit Automation\Output\U27100WB2021PTC246718\gst.xlsx"
            cin_column_name = config_dict['cin_column_name']
            company_column_name = config_dict['company_column_name']
            for df in output_df:
                gstin = df[df['Field_name'] == 'gstin']['Value'].values[0]
                tables_list = df[df.columns[2]].unique()
                for table in tables_list:
                    table_df = df[df[df.columns[2]] == table]
                    columns_list = table_df[table_df.columns[3]].unique()
                    for column in columns_list:
                        column_df = table_df[table_df[table_df.columns[3]] == column]
                        json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                        # Convert the dictionary to a JSON string
                        json_string = json.dumps(json_dict)
                        update_database_single_value_GST(db_config,table,cin_column_name,cin,company_column_name,company,column,json_string,gstin)
            with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
                row_index = 0
                for df in output_df:
                    df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                    row_index += len(df.index) + 2

        else:
            raise Exception("Not able to connect to API")
        cursor.close()
        connection.close()

    except Exception as error:
        logging.error(f"Error in fetching GST number from API {error}")
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.error(line.strip())
        try:
            json_response = response.json()
            error_details = json_response['result']
            error_message = error_details['message']
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query = "update orders set gst_exception_message = %s where cin = %s"
            values_cin = (error_message,cin)
            logging.info(update_query % values_cin)
            cursor.execute(update_query, values_cin)
            connection.commit()
            cursor.close()
            connection.close()
        except Exception as e:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            update_query = "update orders set gst_exception_message = %s where cin = %s"
            values_cin = (error, cin)
            logging.info(update_query % values_cin)
            cursor.execute(update_query, values_cin)
            connection.commit()
            cursor.close()
            connection.close()
            logging.info(f"Exception in updating error status {e}")

        return False
    else:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        update_query = "update orders set gst_status='Y',gst_exception_message = '' where cin = %s"
        values_cin = (cin,)
        logging.info(update_query % values_cin)
        cursor.execute(update_query,values_cin)
        connection.commit()
        cursor.close()
        connection.close()
        return True


def fetch_gst_details(config_dict,gst_number,status):
    try:
        setup_logging()
        map_file_path = config_dict['map_file_path']
        map_file_sheet_name = config_dict['map_file_sheet_name']
        if not os.path.exists(map_file_path):
            raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")
        try:
            df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
            # logging.info(df_map)
        except Exception as e:
            raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))
        df_map['Value'] = None
        url = config_dict['gst_url']
        payload = json.dumps({
            "gstin": gst_number
        })
        headers = {
            'Subscriptionkey': config_dict['api_subscription_key'],
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            gst_json_response = response.json()
            gst_details_result = gst_json_response[config_dict['gst_result_keyword']][config_dict['gst_result_keyword']][config_dict['gstn_detailed_keyword']]
            for index,row in df_map.iterrows():
                field_name = str(row.iloc[0]).strip()
                json_node = str(row.iloc[1]).strip()
                table = str(row.iloc[2]).strip()
                column = str(row.iloc[3]).strip()
                if field_name == config_dict['filings_keyword']:
                    value = gst_json_response[config_dict['gst_result_keyword']][config_dict['gst_result_keyword']][json_node]
                    for entry in value:
                        # Convert the dateOfFiling to yyyy-mm-dd format
                        try:
                            entry["dateOfFiling"] = datetime.strptime(entry["dateOfFiling"], "%d/%m/%Y").strftime(
                                "%Y-%m-%d")
                        except:
                            pass
                        try:
                            entry["return_type"] = entry.pop("gstType", entry["gstType"])
                            entry["date_of_filing"] = entry.pop("dateOfFiling", entry["dateOfFiling"])
                            entry["financial_year"] = entry.pop("filingYear", entry["filingYear"])
                            entry["tax_period"] = entry.pop("monthOfFiling", entry["monthOfFiling"])
                            entry["status"] = entry.pop("gstStatus", entry["gstStatus"])
                        except Exception as e:
                            logging.info(f"Exception in updating key names {e}")
                            continue
                    value = json.dumps(value)
                    value = value.replace("'", '"')
                elif field_name == config_dict['gstin_keyword']:
                    value = gst_number
                elif field_name == config_dict['status_keyword']:
                    value = status
                else:
                    value = gst_details_result[json_node]
                    if field_name == config_dict['state_keyword'] or field_name == config_dict['state_jurisdiction_keyword']:
                        pattern = re.compile(r'STATE - ([\w\s]+)(?:,|$)')
                        # Use the findall method to extract the state information
                        matches = pattern.findall(value)
                        # logging.info the result
                        if matches:
                            value = matches[0]
                    elif field_name == config_dict['centre_jurisdiction_keyword']:
                        pattern = re.compile(r'COMMISSIONERATE - (\w+)')
                        # Use the findall method to extract the state information
                        matches = pattern.findall(value)
                        # logging.info the result
                        if matches:
                            value = matches[0]
                    elif field_name == config_dict['nature_of_business_activities_keyword']:
                        value = '\n'.join(value)
                    elif field_name == 'legal_business_name' or field_name == 'trade_name':
                        try:
                            value = value.replace("'", '')
                        except:
                            pass
                df_map.at[index, 'Value'] = value
            return df_map
    except Exception as e:
        logging.error(f"Error in fetching GST Details {e}")
