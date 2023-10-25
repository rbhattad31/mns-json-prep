import re
from datetime import datetime

import pandas as pd
import xml.etree.ElementTree as Et
import json
import os
import mysql.connector

pd.set_option('display.max_columns', None)


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
        print(f"An error occurred: {e}")
        return None


def extract_table_values_from_xml(xml_root, table_node_name, child_nodes):
    data_list = []
    child_nodes_list = [x.strip() for x in child_nodes.split(',')]
    # print(child_nodes_list)
    # print(table_node_name)
    for data in xml_root.findall(f'.//{table_node_name}'):
        temp_list = []
        for node in child_nodes_list:
            # print(node)
            try:
                node_value = data.find(node).text
            except AttributeError:
                node_value = None
            # print(node_value)
            temp_list.append(node_value)
        # print(temp_list)
        data_list.append(temp_list)
        # print(data_list)
    return data_list


def update_datatable_single_value(db_cursor, table_name, cin_column_name, cin_value,
                                  company_name_column_name,
                                  company_name, column_name, column_value):
    # determine value to be updated
    # if only one key value pair - update value
    # otherwise complete json dictionary
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    print(column_name)
    if column_name == 'total_equity_shares' or column_name == 'total_preference_shares':
        temp_column_value = 0
        for key, value in json_dict.items():
            temp_column_value += float(value)
        print(f'{column_value=}')
        column_value = temp_column_value
    elif num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)
    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}'".format(table_name, cin_column_name, cin_value)
    # print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        print(err)
    result = db_cursor.fetchall()
    # print(result)
    print(column_value)
    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(table_name, column_name,
                                                                                      column_value, cin_column_name,
                                                                                      cin_value,
                                                                                      company_name_column_name,
                                                                                      company_name
                                                                                      )
        print(update_query)
        db_cursor.execute(update_query)
        print("Updated entry")
    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      column_value)
        print(insert_query)
        db_cursor.execute(insert_query)
        print("inserted entry")


def xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
              output_file_path, cin_column_value, company_name, filing_date):
    config_dict_keys = ['single_type_indicator']

    missing_keys = [key for key in config_dict_keys if key not in config_dict]

    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

    # initializing empty list to save all the result dataframes for saving to output excel
    output_dataframes_list = []

    if not os.path.exists(map_file_path):
        raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

    try:
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        print(df_map)
    except Exception as e:
        raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))

    # adding an empty column to mapping df
    df_map['Value'] = None

    # creating new filtered dataframes for single and group values
    single_df = df_map[df_map[df_map.columns[2]] == config_dict['single_type_indicator']]
    print(single_df)

    if not os.path.exists(xml_file_path):
        raise FileNotFoundError(f"The XML file '{xml_file_path}' is not found.")

    try:
        xml_tree = Et.parse(xml_file_path)
        xml_root = xml_tree.getroot()
        # xml_str = Et.tostring(xml_root, encoding='unicode')
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))

    cin_column_name_in_db = config_dict['cin_column_name_in_db']
    company_name_column_name_in_db = config_dict['company_name_column_name_in_db']

    # initializing list to save single values data
    results = []

    # extract single values
    for index, row in single_df.iterrows():
        field_name = str(row.iloc[0]).strip()       # Field name
        parent_node = str(row.iloc[3]).strip()      # Parent Node
        child_nodes = str(row.iloc[4]).strip()      # Child Node
        sql_table_name = str(row.iloc[5]).strip()   # Table Name
        column_name = str(row.iloc[6]).strip()      # column name
        column_json_node = str(row.iloc[7]).strip() # column json
        if field_name == 'filing_date':
            value = filing_date
        else:
            value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        single_df.at[index, 'Value'] = value
        print(field_name)
        print(value)
        results.append([field_name, value, sql_table_name, column_name, column_json_node])
    print(single_df)

    # update single values in datatable
    # get all the tables names for all single values df
    sql_tables_list = single_df[single_df.columns[5]].unique()
    # for each distinct table value, filter the df with table value and find columns
    for sql_table_name in sql_tables_list:

        # filter only table
        table_df = single_df[single_df[single_df.columns[5]] == sql_table_name]
        # print(table_df)

        columns_list = table_df[table_df.columns[6]].unique()
        # print(columns_list)
        for column_name in columns_list:
            # print(column_name)
            # filter table df with only column value
            column_df = table_df[table_df[table_df.columns[6]] == column_name]
            # print(column_df)

            # create json dict with keys of field name and values for the same column name entries
            json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
            # Convert the dictionary to a JSON string
            json_string = json.dumps(json_dict)
            # print(json_string)

            try:
                update_datatable_single_value(db_cursor, sql_table_name,
                                              cin_column_name_in_db,
                                              cin_column_value,
                                              company_name_column_name_in_db,
                                              company_name, column_name,
                                              json_string)
            except Exception as e:
                print(f"Exception {e} occurred while updating data in dataframe for {sql_table_name} "
                      f"with data {json_string}")

    single_output_df = pd.DataFrame(results, columns=['Field Name', 'Value',
                                                      'Table Name', 'Column Name',
                                                      'Column JSON Node'])
    # print(single_output_df)
    output_dataframes_list.append(single_output_df)
    print("Completed processing single rows")

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name=config_dict['output_sheet_name'], index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()


def chg1_xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                   output_file_path, cin_column_value, company_name, filing_date):
    try:
        xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                  output_file_path, cin_column_value, company_name, filing_date)
    except Exception as e:
        print("Below Exception occurred while processing mgt7 file: \n ", e)
        return False
    else:
        return True
