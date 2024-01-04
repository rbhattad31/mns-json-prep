import sys
import traceback
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as Et
import os
import mysql.connector
from Config import create_main_config_dictionary
pd.set_option('display.max_columns', None)
from logging_config import setup_logging
import logging
import json


def get_single_value_from_xml(xml_root, parent_node, child_node):
    try:
        setup_logging()
        namespaces = {'xfa': 'http://www.xfa.org/schema/xfa-data/1.0/',
                      'frm': 'http://www.mit.gov.in/eGov/BackOffice/schema/Form',
                      'cdt':'http://www.mit.gov.in/eGov/BackOffice/schema/ComplexDataTypes'}
        if child_node == 'nan':
            elements = xml_root.findall(f'.//{parent_node}')
        else:
            elements = xml_root.findall(f'.//{parent_node}//{child_node}',namespaces)

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


def update_database_single_value(db_config, config_dict, table_name, cin_column_name, cin_value,
                                 column_name, column_value):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True
    year_column_name = 'financial_year'
    year = config_dict['date']

    json_dict = json.loads(column_value)
    num_elements = len(json_dict)

    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        if first_value is None:
            print(f"Value is None for column '{column_name}' to update in table '{table_name}'")
            db_cursor.close()
            db_connection.close()
            return
        column_value = first_value.replace('"', '\\"').replace("'", "\\'")
        # print(column_value)
    else:
        column_value = json.dumps(json_dict)

    query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}'".format(table_name, cin_column_name, cin_value,
                                                                    year_column_name, year)
    print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as e:
        print(f"Exception {e} occurred while executing db selection query for table {table_name}")
    except Exception as e:
        print(f"Exception {e} occurred while executing db selection query for table {table_name}")
    result = db_cursor.fetchall()

    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} ='{}'".format(table_name,
                                                                                     column_name, column_value,
                                                                                     cin_column_name, cin_value,
                                                                                     year_column_name,
                                                                                     year
                                                                                     )
        print(update_query)
        try:
            db_cursor.execute(update_query)
        except Exception as e:
            raise e
        else:
            print(f"updated form 11 data in table {table_name}")
            db_cursor.close()
            db_connection.close()
            return
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name,
                                                                                      cin_column_name,
                                                                                      year_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      year,
                                                                                      column_value
                                                                                      )
        print(insert_query)
        try:
            db_cursor.execute(insert_query)
        except Exception as e:
            raise e
        else:
            print(f"Inserted into table {table_name}")
            db_cursor.close()
            db_connection.close()
            return


def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
              output_file_path, cin):
    config_dict_keys = ['single_type_indicator', 'group_type_indicator', 'cin_column_name',
                        'field_name_index', 'type_index',
                        'parent_node_index',
                        'child_nodes_index', 'sql_table_name_index',
                        'column_name_index', 'column_json_node_index',
                        'company_table_name', 'summary_designated_partners_table_name',
                        'principal_business_activities_table_name',
                        'individual_partners_table_name',
                        'authorized_signatories_table_name',
                        'body_corporates_table_name',
                        'din_column_name', 'pan_column_name',
                        'financial_year_column_name',
                        'signer_id_column_name', 'id_type_column_name',
                        'nominee_id_column_name', 'nominee_id_type_column_name',
                        'cin_type_value', 'llp_type_value', 'others_type_value',
                        'din_value', 'pan_value', 'others_nominee_type_value'
                        ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]
    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

    field_name_index = 0
    single_group_type_index = 2
    parent_node_index = 3
    child_nodes_index = 4
    sql_table_name_index = 6
    column_name_index = 7
    column_json_node_index = 8

    if not os.path.exists(map_file_path):
        raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

    try:
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        # print(df_map)
    except Exception as e:
        raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))


    # adding an empty column to mapping df
    df_map['Value'] = None

    single_df = df_map[df_map[df_map.columns[single_group_type_index]] == config_dict['single_type_indicator']]
    if not os.path.exists(xml_file_path):
        raise FileNotFoundError(f"The XML file '{xml_file_path}' is not found.")

    try:
        xml_tree = Et.parse(xml_file_path)
        xml_root = xml_tree.getroot()
        # xml_str = Et.tostring(xml_root, encoding='unicode')
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))

    cin_column_name = config_dict['cin_column_name']

    # initializing empty list to save all the result dataframes for saving to output excel
    output_dataframes_list = []

    # initializing list to save single values data
    results = []

    # extract single values
    for index, row in single_df.iterrows():
        field_name = str(row.iloc[field_name_index]).strip()  # Field name
        parent_node = str(row.iloc[parent_node_index]).strip()  # Parent Node
        child_nodes = str(row.iloc[child_nodes_index]).strip()  # Child Node
        sql_table_name = str(row.iloc[sql_table_name_index]).strip()  # Table Name
        column_name = str(row.iloc[column_name_index]).strip()  # column name
        column_json_node = str(row.iloc[column_json_node_index]).strip()  # column json
        value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        single_df.at[index, 'Value'] = value
        # print(field_name)
        # print(value)
        results.append([field_name, value, sql_table_name, column_name, column_json_node])

    if (single_df['Value'] == '').all() or single_df['Value'].isna().all() or single_df['Value'].isnull().all():
        logging.info("All empty so going for other type of form")
        raise Exception("All values in the 'Value' column are empty.")
    date = single_df[single_df['Field_Name'] == 'date_of_address_change']['Value'].values[0]
    all_columns_list = single_df[single_df.columns[column_name_index]].unique()
    for column_name in all_columns_list:
        if column_name == 'financial_year':
            column_df = single_df[single_df[single_df.columns[column_name_index]] == column_name]
            date = column_df['Value'].iloc[0]
            print(date)
            config_dict['date'] = date

        if column_name == config_dict['ba_address_line1_column_name'] or \
                column_name == config_dict['ba_address_line2_column_name'] or \
                column_name == config_dict['ba_city_column_name'] or \
                column_name == config_dict['ba_state_column_name'] or \
                column_name == config_dict['ba_pincode_column_name']:
            column_df = single_df[single_df[single_df.columns[column_name_index]] == column_name]
            config_dict[column_name] = column_df['Value'].iloc[0]  # config_dict['ba_address_line1'] = value
            print(config_dict[column_name])
            # create full address from config dict saved values
    full_address = ', '.join(value for value in [config_dict[config_dict['ba_address_line1_column_name']],
                                                     config_dict[config_dict['ba_address_line2_column_name']],
                                                     config_dict[config_dict['ba_city_column_name']],
                                                     config_dict[config_dict['ba_state_column_name']],
                                                     config_dict[config_dict['ba_pincode_column_name']]]
                                 if value is not None
                                 )
    print(f'{full_address=}')

    full_address_row_index = single_df[single_df[single_df.columns[column_name_index]] ==
                                       'full_address'].index[0]
    print(f'{full_address_row_index=}')
    if full_address_row_index is not None:
        if full_address is not None:
            single_df.loc[full_address_row_index, 'Value'] = full_address
        else:
            single_df.loc[full_address_row_index, 'Value'] = None
    else:
        print(f"full_address details is not in mapping file.")


    purpose_abbreviation_list = [x.strip() for x in config_dict['purpose_abbreviation_list'].split(',')]
    purpose_list = [x.strip() for x in config_dict['purpose_list'].split(',')]
    purpose_dict = dict(zip(purpose_abbreviation_list,purpose_list))
    try:
        purpose_row_index = single_df[single_df['Column_Name'] == 'purpose'].index[0]
    except IndexError as index_error:
        raise (f"Below Exception occurred while getting status row details of charge sequence table - "
               f"\n {index_error}")
    if purpose_row_index is not None:
        purpose_value = single_df.loc[purpose_row_index, 'Value']
        purpose_value = str(purpose_value).lower()
        logging.info(f'{purpose_value=}')
        single_df.loc[purpose_row_index, 'Value'] = purpose_dict.get(purpose_value, "Status Not Found")
    sql_tables_list = single_df[single_df.columns[sql_table_name_index]].unique()
    print(sql_tables_list)
    # for each distinct table value, filter the df with table value and find columns
    for sql_table_name in sql_tables_list:
        print(sql_table_name)
        # filter only table
        table_df = single_df[single_df[single_df.columns[sql_table_name_index]] == sql_table_name]
        print(table_df)

        columns_list = table_df[table_df.columns[column_name_index]].unique()
            # print(columns_list)
        for column_name in columns_list:
            # print(column_name)
            # filter table df with only column value
            column_df = table_df[table_df[table_df.columns[column_name_index]] ==
                                     column_name]
            # print(column_df)

            # create json dict with keys of field name and values for the same column name entries
            json_dict = column_df.set_index(table_df.columns[field_name_index])['Value'].to_dict()
            # Convert the dictionary to a JSON string
            json_string = json.dumps(json_dict)
            print(json_string)
            try:
                update_database_single_value(db_config, config_dict, sql_table_name,
                                                 cin_column_name,
                                                 cin,
                                                 column_name,
                                                 json_string)
            except Exception as e:
                print(f"Exception {e} occurred while updating data in table {sql_table_name} "
                          f"with data {json_string}")
            else:
                print(f'{sql_table_name} Table is updated')

    single_output_df = pd.DataFrame(results, columns=['Field Name', 'Value',
                                                      'Table Name', 'Column Name',
                                                      'Column JSON Node'])
    output_dataframes_list.append(single_output_df)


def form_18_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                      output_file_path, cin):
    try:
        xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path, cin)
    except Exception as e:
        print("Below Exception occurred while processing Form 11 file: \n ", e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # Print the traceback details
        for line in traceback_details:
            print(line.strip())

        return False
    else:
        return True
