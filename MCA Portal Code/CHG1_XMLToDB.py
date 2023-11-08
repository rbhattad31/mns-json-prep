import re
import sys
import traceback
from datetime import datetime

import pandas as pd
import xml.etree.ElementTree as Et
import json
import os
import mysql.connector

# import datetime
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


def update_datatable_single_value(db_cursor, table_name, cin_column_name, cin_value,
                                  company_name_column_name,
                                  company_name, column_name, column_value, charge_id, date, charge_id_column_name,
                                  date_column_name):
    # determine value to be updated
    # if only one key value pair - update value
    # otherwise complete json dictionary
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    print(column_name)

    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)
    # check if there is already entry with cin
    query = 'SELECT * FROM {} WHERE {} = "{}" and {} = "{}" and {} = "{}"'.format(table_name, cin_column_name,
                                                                                  cin_value, charge_id_column_name,
                                                                                  charge_id, date_column_name, date)
    # print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        print(err)
    result = db_cursor.fetchall()
    # print(result)
    print(column_value)
    # if cin value already exists
    print(len(result))
    if len(result) > 0:
        update_query = 'UPDATE {} SET {} = "{}" WHERE {} = "{}" AND {} = "{}" AND {} = "{}" and {} = "{}"'.format(
            table_name, column_name,
            column_value, cin_column_name,
            cin_value,
            company_name_column_name,
            company_name, charge_id_column_name, charge_id, date_column_name, date
            )
        print(update_query)
        db_cursor.execute(update_query)
        print("Updated entry")
    # if cin value doesn't exist
    else:
        insert_query = 'INSERT INTO {} ({}, {}, {},{},{}) VALUES ("{}", "{}", "{}","{}","{}")'.format(table_name,
                                                                                                      cin_column_name,
                                                                                                      company_name_column_name,
                                                                                                      column_name,
                                                                                                      charge_id_column_name,
                                                                                                      date_column_name,
                                                                                                      cin_value,
                                                                                                      company_name,
                                                                                                      column_value,
                                                                                                      charge_id, date)
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
        # print(df_map)
    except Exception as e:
        raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))

    # adding an empty column to mapping df
    df_map['Value'] = None

    # creating new filtered dataframes for single and group values
    single_df = df_map[df_map[df_map.columns[2]] == config_dict['single_type_indicator']]
    # print(single_df)

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
        field_name = str(row.iloc[0]).strip()  # Field name
        parent_node = str(row.iloc[3]).strip()  # Parent Node
        child_nodes = str(row.iloc[4]).strip()  # Child Node
        sql_table_name = str(row.iloc[5]).strip()  # Table Name
        column_name = str(row.iloc[6]).strip()  # column name
        column_json_node = str(row.iloc[7]).strip()  # column json
        if field_name == 'filing_date':
            value = filing_date
        else:
            value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
            if field_name == 'date':
                try:
                    value = datetime.strptime(value, '%Y-%m-%d').strftime('%d/%m/%Y')
                except Exception as e:
                    print(f"Excpetion occured for date conversion to dd/mm/yyyy \n {e}")
        single_df.at[index, 'Value'] = value
        # print(field_name)
        print(value)
        results.append([field_name, value, sql_table_name, column_name, column_json_node])
    # print(single_df)

    # update single values in datatable
    # get all the tables names for all single values df
    sql_tables_list = single_df[single_df.columns[5]].unique()
    # for each distinct table value, filter the df with table value and find columns
    for sql_table_name in sql_tables_list:

        # filter only table
        table_df = single_df[single_df[single_df.columns[5]] == sql_table_name]
        if sql_table_name == config_dict['open_charges_latest_event_table_name']:
            charge_id = table_df.loc[table_df['Column_Name'] == config_dict['charges_latest_id_column_name'], 'Value'].values[0]
        elif sql_table_name == config_dict['charge_sequence_table_name']:
            charge_id = table_df.loc[table_df['Column_Name'] == config_dict['charge_id_column_name'], 'Value'].values[0]
        else:
            charge_id = None
        date = table_df.loc[table_df['Column_Name'] == config_dict['date_column_name'], 'Value'].values[0]
        print(table_df)
        if sql_table_name == config_dict['open_charges_latest_event_table_name']:
            print(f'{sql_table_name=}')
            charges_latest_id = \
                table_df.loc[table_df['Column_Name'] == config_dict['charges_latest_id_column_name'], 'Value'].values[0]
            date = table_df.loc[table_df['Column_Name'] == config_dict['date_column_name'], 'Value'].values[0]
            print(f'{charges_latest_id=}')
            # check if there is already entry with cin
            charge_id_year_check_query = (("SELECT * FROM {} WHERE {} = '{}' AND {} = '{}' AND {}"
                                           " = '{}' AND {} = '{}'").
                                          format(sql_table_name, cin_column_name_in_db, cin_column_value,
                                                 company_name_column_name_in_db,
                                                 company_name,
                                                 config_dict['charges_latest_id_column_name'],
                                                 charges_latest_id,
                                                 config_dict['date_column_name'],
                                                 date
                                                 ))
            print(f'{charge_id_year_check_query=}')
            try:
                db_cursor.execute(charge_id_year_check_query)
            except mysql.connector.Error as err:
                print(err)
            result = db_cursor.fetchall()
            # print(result)
            # if change id value already exists for cin
            if len(result) == 0:
                print("Charge ID details are not exist in 'open_charges_latest_event' table for charge id {} with cin"
                      " {}, hence skipping updating the charge id details.".format(charges_latest_id, cin_column_value))
                continue
            else:
                table_df = table_df[table_df['Column_Name'] != config_dict['charges_latest_id_column_name']]
                table_df = table_df[table_df['Column_Name'] != config_dict['date_column_name']]

        if sql_table_name == config_dict['charge_sequence_table_name']:
            print(table_df)
            print(f'{sql_table_name=}')
            status_abbreviation_list = [x.strip() for x in config_dict['status_abbreviation_list'].split(',')]
            status_list = [x.strip() for x in config_dict['status_list'].split(',')]
            status_dict = dict(zip(status_abbreviation_list, status_list))
            print(f'{status_dict=}')
            try:
                status_row_index = table_df[table_df['Column_Name'] == config_dict['status_column_name']].index[0]
            except IndexError as index_error:
                raise (f"Below Exception occurred while getting status row details of charge sequence table - "
                       f"\n {index_error}")
            print(f'{status_row_index=}')
            if status_row_index is not None:
                status_value = table_df.loc[status_row_index, 'Value']
                print(f'{status_value=}')
                table_df.loc[status_row_index, 'Value'] = status_dict.get(status_value, "Status Not Found")
            print(table_df)
            charge_id = table_df.loc[table_df['Column_Name'] == config_dict['charge_id_column_name'], 'Value'].values[0]
            date = table_df.loc[table_df['Column_Name'] == config_dict['date_column_name'], 'Value'].values[0]
            print(f'{charge_id=}')
            if charge_id is None:
                continue
            # check if there is already entry with cin
            charge_id_year_check_query = (("SELECT * FROM {} WHERE {} = '{}' AND {} = '{}' AND {}"
                                           " = '{}' AND {} = '{}'").
                                          format(sql_table_name, cin_column_name_in_db, cin_column_value,
                                                 company_name_column_name_in_db,
                                                 company_name,
                                                 config_dict['charge_id_column_name'],
                                                 charge_id,
                                                 config_dict['date_column_name'],
                                                 date
                                                 ))
            print(f'{charge_id_year_check_query=}')
            try:
                db_cursor.execute(charge_id_year_check_query)
            except mysql.connector.Error as err:
                print(err)
            result = db_cursor.fetchall()
            if sql_table_name == config_dict['charge_sequence_table_name']:
                charge_id_column_name = config_dict['charge_id_column_name']
            elif sql_table_name == config_dict['open_charges_latest_event_table_name']:
                charge_id_column_name = config_dict['charges_latest_id_column_name']
            else:
                charge_id_column_name = None
            date_column_name = config_dict['date_column_name']
            # if charge details are exist then need not update status column values
            if len(result) > 0:
                print(f"Entry is found for cin {cin_column_value} and company name {company_name} in {sql_table_name} "
                      f"table, \n so removing status info from the data not to update in datatable")
                print(table_df)
                table_df = table_df[table_df['Column_Name'] != config_dict['status_column_name']]
                table_df = table_df[table_df['Column_Name'] != config_dict['charge_id_column_name']]
                table_df = table_df[table_df['Column_Name'] != config_dict['date_column_name']]
                print(table_df)
            else:
                insert_query = 'INSERT INTO {} ({}, {}, {},{}) VALUES ("{}", "{}", "{}","{}")'.format(
                    sql_table_name,
                    cin_column_name_in_db,
                    company_name_column_name_in_db,
                    charge_id_column_name,
                    date_column_name,
                    cin_column_value,
                    company_name,
                    charge_id, date)
                db_cursor.execute(insert_query)
                table_df = table_df[table_df['Column_Name'] != config_dict['charge_id_column_name']]
                table_df = table_df[table_df['Column_Name'] != config_dict['date_column_name']]
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
            date_column_name = config_dict['date_column_name']
            if sql_table_name == config_dict['charge_sequence_table_name']:
                charge_id_column_name = config_dict['charge_id_column_name']
            elif sql_table_name == config_dict['open_charges_latest_event_table_name']:
                charge_id_column_name = config_dict['charges_latest_id_column_name']
            else:
                charge_id_column_name = None
            try:
                update_datatable_single_value(db_cursor, sql_table_name,
                                              cin_column_name_in_db,
                                              cin_column_value,
                                              company_name_column_name_in_db,
                                              company_name, column_name,
                                              json_string,charge_id,date,charge_id_column_name,date_column_name)
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
        # Get the current exception information
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # Print the traceback details
        for line in traceback_details:
            print(line.strip())
        return False
    else:
        return True
