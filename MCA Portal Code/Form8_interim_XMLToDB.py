import sys
import traceback
import pandas as pd
import xml.etree.ElementTree as Et
import json
import os
import mysql.connector
from logging_config import setup_logging
import logging
pd.set_option('display.max_columns', None)
from Config import create_main_config_dictionary

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


def update_form8_interim_datatable_single_value(db_config, table_name,
                                                cin_column_name, cin_value,
                                                charge_id_column_name, charge_id,
                                                status_column_name, status,
                                                date_column_name, date,
                                                column_name, column_value,amount_column_name,amount
                                                ):
    # determine value to be updated
    # if only one key value pair - update value
    # otherwise complete json dictionary
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    print(column_name)

    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict).replace('"', "'")

    update_query = 'UPDATE {} SET {} = "{}" WHERE {} = "{}" AND {} = "{}" AND {} = "{}" AND {} = "{}" AND {} = "{}"' \
        .format(table_name,
                column_name, column_value,
                cin_column_name, cin_value,
                charge_id_column_name, charge_id,
                status_column_name, status,
                date_column_name, date,
                amount_column_name,amount
                )
    print(update_query)
    try:
        db_cursor.execute(update_query)
    except Exception as e:
        raise e
    else:
        print("Updated entry")
    db_cursor.close()
    db_connection.close()
    

def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
              output_file_path, cin_column_value, filing_date):
    config_dict_keys = ['single_type_indicator',
                        'cin_column_name_in_db',
                        'field_name_index', 'type_index',
                        'parent_node_index', 'child_nodes_index',
                        'sql_table_name_index', 'column_name_index',
                        'column_json_node_index',
                        'Type_of_file_field_name', 'interim_keyword_in_xml',
                        'open_charges_table_name',
                        'open_charges_latest_event_table_name', 'charge_sequence_table_name',
                        'status_abbreviation_list', 'status_list',
                        'creation_keyword', 'modification_keyword', 'satisfaction_keyword',
                        'date_creation_field_name', 'date_modification_field_name', 'date_satisfaction_field_name',
                        'open_charges_charge_id_column_name',
                        'open_charges_latest_event_charge_id_column_name',
                        'charge_sequence_charge_id_column_name',
                        'status_column_name', 'date_column_name',
                        'filing_date_column_name'
                        ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]

    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

    field_name_index = int(config_dict['field_name_index'])
    type_index = int(config_dict['type_index'])
    parent_node_index = int(config_dict['parent_node_index'])
    child_nodes_index = int(config_dict['child_nodes_index'])
    sql_table_name_index = int(config_dict['sql_table_name_index'])
    column_name_index = int(config_dict['column_name_index'])
    column_json_node_index = int(config_dict['column_json_node_index'])

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
    # print(df_map)

    # creating new filtered dataframes for single and group values
    single_df = df_map[df_map[df_map.columns[type_index]] == config_dict['single_type_indicator']]
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

    # initializing list to save single values data
    results = []

    status_abbreviation_list = [x.strip() for x in config_dict['status_abbreviation_list'].split(',')]
    status_list = [x.strip() for x in config_dict['status_list'].split(',')]
    status_dict = dict(zip(status_abbreviation_list, status_list))
    print(status_dict)

    property_type_abbreviation_list = [x.strip() for x in config_dict['property_type_abbreviation_list'].split(',')]
    property_type_list = [x.strip() for x in config_dict['property_type_list'].split(',')]
    property_type_dict = dict(zip(property_type_abbreviation_list, property_type_list))
    print(property_type_dict)

    # extract single values
    for index, row in single_df.iterrows():
        field_name = str(row.iloc[field_name_index]).strip()  # Field name
        parent_node = str(row.iloc[parent_node_index]).strip()  # Parent Node
        child_nodes = str(row.iloc[child_nodes_index]).strip()  # Child Node
        sql_table_name = str(row.iloc[sql_table_name_index]).strip()  # Table Name
        column_name = str(row.iloc[column_name_index]).strip()  # column name
        column_json_node = str(row.iloc[column_json_node_index]).strip()  # column json

        if column_name == config_dict['filing_date_column_name']:
            value = filing_date
        else:
            value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        single_df.at[index, 'Value'] = value
        # print(field_name)
        # print(value)
        results.append([field_name, value, sql_table_name, column_name, column_json_node])
    print(single_df)

    type_of_file_row_index = single_df[single_df[single_df.columns[field_name_index]] ==
                                       config_dict['Type_of_file_field_name']].index[0]
    if type_of_file_row_index is not None:
        type_of_file_value = single_df.loc[type_of_file_row_index, 'Value']
        print(f'{type_of_file_value=}')
        if type_of_file_value != config_dict['interim_keyword_in_xml']:
            raise Exception("Type of file is not related to Form 8 interim. So stopping program execution")
    else:
        raise Exception("Type of file field is not found in Form 8 interim mapping file. So stopping program execution")
    # get all the tables names for all single values df
    try:
        charge_id = single_df.loc[single_df['Field_Name'] == 'charge_id', 'Value'].values[0]
        status_abbreviation_list = [x.strip() for x in config_dict['status_abbreviation_list'].split(',')]
        status_list = [x.strip() for x in config_dict['status_list'].split(',')]
        status_dict = dict(zip(status_abbreviation_list, status_list))
        logging.info(f'{status_dict=}')
        try:
            status_row_index = single_df[single_df['Column_Name'] == config_dict['status_column_name']].index[0]
        except IndexError as index_error:
            raise (f"Below Exception occurred while getting status row details of charge sequence table - "
                   f"\n {index_error}")
        logging.info(f'{status_row_index=}')
        if status_row_index is not None:
            status_value = single_df.loc[status_row_index, 'Value']
            logging.info(f'{status_value=}')
            single_df.loc[status_row_index, 'Value'] = status_dict.get(status_value, "Status Not Found")
        status = single_df.loc[single_df['Field_Name'] == 'status', 'Value'].values[0]
        holder_name = single_df.loc[single_df['Field_Name'] == 'holder_name', 'Value'].values[0]
        amount = single_df.loc[single_df['Field_Name'] == 'amount', 'Value'].values[0]
        try:
            try:
                amount = str(amount).replace("\n", "")
            except:
                pass
            amount = str(amount).replace(',','')
            amount = float(amount)
            amount = int(amount)
        except Exception as e:
            print(e)
            pass
        logging.info(status)
        logging.info(holder_name)
        logging.info(amount)
        date = single_df.loc[single_df['Field_Name'] == 'date', 'Value'].values[0]
        if str(status).lower() == 'creation':
            date_column = 'date_of_creation'
        elif str(status).lower() == 'modification':
            date_column = 'date_of_modification'
        elif str(status).lower() == 'satisfaction':
            date_column = 'date_of_satisfaction'
        else:
            date_column = 'date'
        type_column = config_dict['type_column_name']
        holder_column = config_dict['holder_name_column_name']
        amount_column_name = config_dict['amount_column_name']
        if charge_id is not None:
            if charge_id == '' or charge_id == '-' or charge_id == 0 or charge_id == '0' or charge_id == 'None':
                db_connection = mysql.connector.connect(**db_config)
                db_cursor = db_connection.cursor()
                db_connection.autocommit = True
                charge_id_check_query = "select id from open_charges where {} = '{}' and REPLACE({},',','') = '{}' and {}='{}' and {} = '{}'".format(cin_column_name_in_db,cin_column_value,amount_column_name,amount,date_column,date,type_column,status)
                logging.info(charge_id_check_query)
                db_cursor.execute(charge_id_check_query)
                charge_id = db_cursor.fetchone()[0]
                logging.info(charge_id)
                single_df.loc[single_df['Field_Name'] == 'id', 'Value'] = charge_id
                single_df.loc[single_df['Field_Name'] == 'charge_id', 'Value'] = charge_id
                update_query = "update charge_sequence set charge_id = %s where cin = %s and holder_name = %s and amount = %s and date = %s"
                update_values = (charge_id,cin_column_value,holder_name,amount,date)
                logging.info(update_query % update_values)
                db_cursor.execute(update_query,update_values)
            else:
                logging.info("Charge ID Present")
        else:
            db_connection = mysql.connector.connect(**db_config)
            db_cursor = db_connection.cursor()
            db_connection.autocommit = True
            charge_id_check_query = "select id from open_charges where {} = '{}' and REPLACE({},',','') = '{}' and {}='{}' and {} = '{}'".format(
                cin_column_name_in_db, cin_column_value, amount_column_name, amount, date_column, date, type_column,
                status)
            logging.info(charge_id_check_query)
            db_cursor.execute(charge_id_check_query)
            charge_id = db_cursor.fetchone()[0]
            logging.info(charge_id)
            single_df.loc[single_df['Field_Name'] == 'id', 'Value'] = charge_id
            single_df.loc[single_df['Field_Name'] == 'charge_id', 'Value'] = charge_id
            update_query = "update charge_sequence set charge_id = %s where cin = %s and holder_name = %s and amount = %s and date = %s"
            update_values = (charge_id, cin_column_value, holder_name, amount, date)
            logging.info(update_query % update_values)
            db_cursor.execute(update_query, update_values)
    except Exception as e:
        logging.info(f"Exception occured in updating charge id {e}")
    sql_tables_list = single_df[single_df.columns[sql_table_name_index]].unique()
    # for each distinct table value, filter the df with table value and find columns
    for sql_table_name in sql_tables_list:
        table_df = single_df[single_df[single_df.columns[sql_table_name_index]] == sql_table_name]
        logging.info(table_df)
        if sql_table_name == config_dict['open_charges_latest_event_table_name']:
            charge_id = table_df.loc[table_df['Column_Name'] == config_dict['open_charges_latest_event_charge_id_column_name'], 'Value'].values[0]
        elif sql_table_name == config_dict['charge_sequence_table_name']:
            charge_id = table_df.loc[table_df['Column_Name'] == config_dict['charge_sequence_charge_id_column_name'], 'Value'].values[0]
        else:
            charge_id = None
        if sql_table_name == config_dict['open_charges_table_name']:
            print(f'{sql_table_name=}')
            try:
                status_row_index = table_df[table_df[table_df.columns[column_name_index]] ==
                                            config_dict['status_column_name']].index[0]

            except Exception as e:
                raise Exception(f"Below Exception occurred while getting status row details of table {sql_table_name}"
                                f" from mapping file-"
                                f"\n {e}")
            # print(f'{status_row_index=}')
            if status_row_index is not None:
                status_value = table_df.loc[status_row_index, 'Value']
                table_df.loc[status_row_index, 'Value'] = status_dict.get(status_value, status_value)
                status_value = table_df.loc[status_row_index, 'Value']
                if status_value == config_dict['creation_keyword']:
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_modification_field_name']]
                    pass
                elif status_value == config_dict['modification_keyword']:
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_creation_field_name']]
                    pass
            else:
                print(f"Status details is not found for the table {sql_table_name} in mapping file. \n "
                      f"hence continuing with next table.")
        elif sql_table_name == config_dict['open_charges_latest_event_table_name']:
            print(f'{sql_table_name=}')
            try:
                status_row_index = table_df[table_df[table_df.columns[column_name_index]] ==
                                            config_dict['status_column_name']].index[0]
            except Exception as e:
                raise Exception(f"Below Exception occurred while getting status row details of table {sql_table_name} "
                                f"from mapping file"
                                f"\n {e}")
            # print(f'{status_row_index=}')
            date = table_df.loc[table_df['Column_Name'] == config_dict['date_column_name'], 'Value'].values[0]
            amount = table_df.loc[table_df['Column_Name'] == config_dict['amount_column_name'], 'Value'].values[0]
            if status_row_index is not None:
                status_value = table_df.loc[status_row_index, 'Value']
                table_df.loc[status_row_index, 'Value'] = status_dict.get(status_value, status_value)
                status_value = table_df.loc[status_row_index, 'Value']
                if status_value == config_dict['creation_keyword']:
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_modification_field_name']]
                    pass
                elif status_value == config_dict['modification_keyword']:
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_creation_field_name']]
                    pass
            else:
                print(f"Status details is not found for the table {sql_table_name} in mapping file. \n "
                      f"hence continuing with next table.")
        elif sql_table_name == config_dict['charge_sequence_table_name']:
            print(f'{sql_table_name=}')
            try:
                status_row_index = table_df[table_df[table_df.columns[column_name_index]] ==
                                            config_dict['status_column_name']].index[0]
            except Exception as e:
                raise Exception(f"Below Exception occurred while getting status row details of table {sql_table_name} "
                                f"from mapping file"
                                f"\n {e}")
            # print(f'{status_row_index=}')
            if status_row_index is not None:
                status_value = table_df.loc[status_row_index, 'Value']
                table_df.loc[status_row_index, 'Value'] = status_dict.get(status_value, status_value)
                status_value = table_df.loc[status_row_index, 'Value']
                if status_value == config_dict['creation_keyword']:
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_modification_field_name']]
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_satisfaction_field_name']]
                    pass
                elif status_value == config_dict['modification_keyword']:
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_creation_field_name']]
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_satisfaction_field_name']]
                    pass
                elif status_value == config_dict['satisfaction_keyword']:
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_creation_field_name']]
                    table_df = table_df[table_df[table_df.columns[field_name_index]] !=
                                        config_dict['date_modification_field_name']]
            else:
                print(f"Status details is not found for the table {sql_table_name} in mapping file. \n "
                      f"hence continuing with next table.")
        else:
            continue
        print(table_df)
        if sql_table_name == config_dict['open_charges_table_name']:
            charge_id_column_name = config_dict['open_charges_charge_id_column_name']
        elif sql_table_name == config_dict['open_charges_latest_event_table_name']:
            charge_id_column_name = config_dict['open_charges_latest_event_charge_id_column_name']
        elif sql_table_name == config_dict['charge_sequence_table_name']:
            charge_id_column_name = config_dict['charge_sequence_charge_id_column_name']
        else:
            continue
        # print(charge_id_column_name)
        status_column_name = config_dict['status_column_name']
        # print(status_column_name)
        date_column_name = config_dict['date_column_name']
        # print(date_column_name)
        amount_column_name = config_dict['amount_column_name']
        try:
            charge_id_row_index = table_df[table_df[table_df.columns[column_name_index]] == charge_id_column_name]. \
                index[0]
            status_row_index = table_df[table_df[table_df.columns[column_name_index]] == status_column_name].index[0]
            date_row_index = table_df[table_df[table_df.columns[column_name_index]] == date_column_name].index[0]
            charge_id = table_df.loc[charge_id_row_index, 'Value']
            print(f'{charge_id=}')
            status = table_df.loc[status_row_index, 'Value']
            print(f'{status=}')
            date = table_df.loc[date_row_index, 'Value']
            print(f'{date=}')
            date = table_df.loc[table_df['Column_Name'] == config_dict['date_column_name'], 'Value'].values[0]
            amount = table_df.loc[table_df['Column_Name'] == config_dict['amount_column_name'], 'Value'].values[0]
        except Exception as e:
            print(f'Exception {e} occurred while finding charge id, status and date values. please check mapping file '
                  f'and datatable structure. \ncontinuing with next table processing...')
            continue
        db_connection = mysql.connector.connect(**db_config)
        db_cursor = db_connection.cursor()
        db_connection.autocommit = True
        charge_id_year_check_query = (('SELECT * FROM {} WHERE {} = "{}" AND {} = "{}" AND {} = "{}" '
                                       'AND {} = "{}" AND {} = "{}"').
                                      format(sql_table_name,
                                             cin_column_name_in_db, cin_column_value,
                                             charge_id_column_name, charge_id,
                                             status_column_name, status,
                                             date_column_name, date,
                                             amount_column_name,amount
                                             ))
        print(f'{charge_id_year_check_query=}')
        try:
            db_cursor.execute(charge_id_year_check_query)
        except Exception as e:
            print(f"Exception Occurred while verifying charge id details in table {sql_table_name}: \n {e}. "
                  f"Continuing with next table processing")
            continue
        result = db_cursor.fetchall()

        if len(result) == 0:
            if sql_table_name == config_dict['open_charges_table_name']:
                print(f"Charge id details are not exist in table {sql_table_name}, "
                      f"hence continuing with next table processing")
                continue
            elif sql_table_name == config_dict['open_charges_latest_event_table_name']:
                print(f"Charge id details are not exist in table {sql_table_name}, "
                      f"hence continuing with next table processing")
                continue
            elif sql_table_name == config_dict['charge_sequence_table_name']:
                # code for insert charge id, status, date
                print(f"Charge id details are not exist in table {sql_table_name}, "
                      f"hence updating the table with charge id, status and date")
                insert_query = 'INSERT INTO {} ({}, {}, {}, {},{}) VALUES ("{}", "{}", "{}", "{}","{}")'.format(
                    sql_table_name,
                    cin_column_name_in_db,
                    charge_id_column_name,
                    status_column_name,
                    date_column_name,
                    amount_column_name,
                    cin_column_value,
                    charge_id,
                    status,
                    date,
                    amount
                )
                print(f'{insert_query=}')

                try:
                    db_cursor.execute(insert_query)
                except Exception as e:
                    print(f"Exception Occurred while inserting charge id details in table {sql_table_name}: "
                          f"\n{e} \ncontinuing with next table process.")

                else:
                    print(f"Charge id details are inserted into table {sql_table_name}")
                # delete charge id, status, date rows
                table_df = table_df[table_df[table_df.columns[column_name_index]] != charge_id_column_name]
                table_df = table_df[table_df[table_df.columns[column_name_index]] != status_column_name]
                table_df = table_df[table_df[table_df.columns[column_name_index]] != date_column_name]
                table_df = table_df[table_df[table_df.columns[column_name_index]] != amount_column_name]
                # update remaining rows
                pass
            else:
                continue
        if len(result) > 0:
            print(
                f"Entry is found for charge id {charge_id}, status {status} and date {date} in table {sql_table_name}, "
                f"hence continuing with tables remaining rows data into database")
            table_df = table_df[table_df[table_df.columns[column_name_index]] != charge_id_column_name]
            table_df = table_df[table_df[table_df.columns[column_name_index]] != status_column_name]
            table_df = table_df[table_df[table_df.columns[column_name_index]] != date_column_name]
            table_df = table_df[table_df[table_df.columns[column_name_index]] != amount_column_name]
        # update all remaining rows for all tables
        columns_list = table_df[table_df.columns[column_name_index]].unique()
        # print(columns_list)

        for column_name in columns_list:
            # print(column_name)
            # filter table df with only column value
            column_df = table_df[table_df[table_df.columns[column_name_index]] == column_name]
            # print(column_df)

            # create json dict with keys of field name and values for the same column name entries
            json_dict = column_df.set_index(table_df.columns[field_name_index])['Value'].to_dict()
            if column_name == config_dict['property_type_column_name']:
                json_dict = {key: property_type_dict.get(value, value) for key, value in json_dict.items() if value !=
                             'NONE'}
                print(json_dict)
                for key,value in json_dict.items():
                    if key == 'property_type_OTHER':
                        if value is None:
                            json_dict.pop(key)
                            break
            # Convert the dictionary to a JSON string
            json_string = json.dumps(json_dict)
            print(json_string)
            db_cursor.close()
            db_connection.close()

            try:
                update_form8_interim_datatable_single_value(db_config, sql_table_name,
                                                            cin_column_name_in_db, cin_column_value,
                                                            charge_id_column_name, charge_id,
                                                            status_column_name, status,
                                                            date_column_name, date,
                                                            column_name, json_string,amount_column_name,amount)

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


def form8_interim_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                            output_file_path, cin_column_value, filing_date):
    try:
        xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                  output_file_path, cin_column_value, filing_date)
    except Exception as e:
        print("Below Exception occurred while processing form 8 interim file: \n ", e)
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


# db_config = {
#         "host": "162.241.123.123",
#         "user": "classle3_deal_saas",
#         "password": "o2i=hi,64u*I",
#         "database": "classle3_mns_credit"
#     }
# excel_file_path = r"C:\Users\BRADSOL123\Documents\Python\Config\Config_Python.xlsx"
# sheet_name = 'Form_8_Interim'
# config_dict,config_status = create_main_config_dictionary(excel_file_path,sheet_name)
# map_file_path = config_dict['mapping file path']
# map_sheet_name = 'Sheet1'
# xml_file_path = r"C:\Users\BRADSOL123\OneDrive - MNS Credit Management Group P Ltd\MNS-Credit\AAG-8883\Charge Documents\LLP Form8-29122021_signed.xml"
# output_file_path = str(xml_file_path).replace('.xml','.xlsx')
# cin = 'AAG-8883'
# filing_date = '29-12-2021'
# form8_interim_xml_to_db(db_config,config_dict,map_file_path,map_sheet_name,xml_file_path,output_file_path,cin,filing_date)