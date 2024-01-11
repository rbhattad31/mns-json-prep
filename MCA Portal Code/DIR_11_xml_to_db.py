import json
import os
import xml.etree.ElementTree as Et
from logging_config import setup_logging
import logging
import pandas as pd
import mysql.connector
from Config import create_main_config_dictionary
import sys
import traceback


def get_single_value_from_xml(xml_root, parent_node, child_node):
    try:
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


def update_database_single_value(db_config, table_name, cin_column_name, cin_value, column_name, column_value, din,
                                 designation):
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
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and LOWER({})='{}' and LOWER({})='{}'".format(table_name, cin_column_name,
                                                                                     cin_value, 'din', din,
                                                                                     'designation',
                                                                                     str(designation).lower(),
                                                                                    'event',
                                                                                     'cessation')
    logging.info(query)
    print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) == 0:
        insert_query = "INSERT INTO {} ({},{},{},{},{}) VALUES ('{}', '{}','{}','{}','{}')".format(table_name, cin_column_name,
                                                                                           'din',
                                                                                           'designation',
                                                                                           'event',
                                                                                           column_name,
                                                                                           cin_value,
                                                                                           din,
                                                                                           designation,'cessation',column_value)
        print(insert_query)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    # if cin value doesn't exist
    else:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND LOWER({}) = '{}' AND LOWER({}) = '{}'".format(table_name,
                                                                                                           column_name,
                                                                                                           column_value,
                                                                                                           cin_column_name,
                                                                                                           cin_value,
                                                                                                           'din',
                                                                                                           din,
                                                                                                           'designation',
                                                                                                           str(designation).lower(),
                                                                                                           'event',
                                                                                                           'cessation')
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")
        print(update_query)

    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, cin_column_name_in_db,
              cin_column_value):
    setup_logging()
    field_name_index = config_dict['field_name_index']
    xml_type_index = config_dict['xml_type_index']
    single_group_type_index = config_dict['single_group_type_index']
    parent_node_index = config_dict['parent_node_index']
    child_nodes_index = config_dict['child_nodes_index']
    table_name_index = config_dict['sql_table_name_index']
    column_name_index = config_dict['column_name_index']
    column_json_node_index = config_dict['column_json_node_index']

    config_dict_keys = [
    ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]
    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

    if not os.path.exists(map_file_path):
        raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

    try:
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        # logging.info(df_map)
    except Exception as e:
        raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))

    # adding an empty column to mapping df
    df_map['Value'] = None
    single_df = df_map[df_map[df_map.columns[single_group_type_index]] == config_dict['single_type_indicator']]
    if not os.path.exists(xml_file_path):
        raise FileNotFoundError(f"The XML file '{xml_file_path}' is not found.")

    try:
        with open(xml_file_path, 'r', encoding='utf-8') as file:
            xml_data = file.read()
        xml_root = Et.fromstring(xml_data)
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))

    event_abbreviation_list = [x.strip() for x in config_dict['event_abbreviation_list'].split(',')]
    event_list = [x.strip() for x in config_dict['event_list'].split(',')]
    event_dict = dict(zip(event_abbreviation_list, event_list))
    logging.info(event_dict)
    designation_abbreviation_list = [x.strip() for x in config_dict['designation_abbreviation_list'].split(',')]
    designation_list = [x.strip() for x in config_dict['designation_list'].split(',')]
    designation_dict = dict(zip(designation_abbreviation_list, designation_list))
    for index, row in single_df.iterrows():
        field_name = str(row.iloc[field_name_index]).strip()
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()

        # sql_table_name = str(row.iloc[table_name_index]).strip()
        # column_name = str(row.iloc[column_name_index]).strip()
        # column_json_node = str(row.iloc[column_json_node_index]).strip()
        value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        try:
            if field_name == 'designation':
                if value is not None:
                    value = designation_dict.get(value,value)
            if field_name == 'event':
                if value is not None:
                    value = event_dict.get(value,value)
        except Exception as e:
            return value + 'not found in dictionary'
        single_df.at[index, 'Value'] = value
    logging.info(single_df)
    print(single_df)

    # DB Logics
    sql_tables_list = single_df[single_df.columns[table_name_index]].unique()
    logging.info(sql_tables_list)
    din_value = single_df[single_df['Field_Name'] == 'din']['Value'].values[0]
    designation = single_df[single_df['Field_Name'] == 'designation']['Value'].values[0]
    logging.info(din_value)
    for table_name in sql_tables_list:
        table_df = single_df[single_df[single_df.columns[table_name_index]] == table_name]
        columns_list = table_df[table_df.columns[column_name_index]].unique()
        logging.info(columns_list)
        for column_name in columns_list:
            logging.info(column_name)
            # filter table df with only column value
            column_df = table_df[table_df[table_df.columns[column_name_index]] == column_name]
            logging.info(column_df)
            # create json dict with keys of field name and values for the same column name entries
            json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
            # Convert the dictionary to a JSON string
            json_string = json.dumps(json_dict)
            logging.info(json_string)
            try:
                if column_name.strip() != 'nan' and column_name != '' and column_name is not None:
                    update_database_single_value(db_config, table_name, cin_column_name_in_db, cin_column_value,
                                                 column_name, json_string, din_value, designation)
            except Exception as e:
                logging.info(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                             f"with data {json_string}")


def dir11_main(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, cin_column_name_in_db,
              cin_column_value):
    try:
        setup_logging()
        xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, cin_column_name_in_db,
              cin_column_value)
    except Exception as e:
        logging.info(f"Exception occured while inserting for dir 11")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.info(line.strip())
        return False
    else:
        return True
# main_dict = create_main_config_dictionary(r"C:\Users\BRADSOL123\Documents\Python\Config\Config_Python.xlsx",
#                                           'DIR')
# config_dict = main_dict[0]
# db_config = {
#         "host": "162.241.123.123",
#         "user": "classle3_deal_saas",
#         "password": "o2i=hi,64u*I",
#         "database": "classle3_mns_credit",
#     }
# map_file_path = r"C:\Users\BRADSOL123\Documents\Python\Config\DIR-11config.xlsx"
# map_file_sheet_name = 'Sheet1'
# xml_file_path = r"C:\Users\BRADSOL123\Downloads\Form DIR-11-02092017_signed (1).xml"
# cin_column_name_in_db = 'cin'
# cin = 'U27107CT1999PLC013773'
# print(xml_to_db(db_config,config_dict,map_file_path,map_file_sheet_name,xml_file_path,cin_column_name_in_db,cin))
