import re
from datetime import datetime

import pandas as pd
import xml.etree.ElementTree as Et
import json
import os
import mysql.connector
import logging
from logging_config import setup_logging
from Config import create_main_config_dictionary
pd.set_option('display.max_columns', None)


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


def extract_table_values_from_xml(xml_root, table_node_name, child_nodes):
    setup_logging()
    data_list = []
    child_nodes_list = [x.strip() for x in child_nodes.split(',')]
    # logging.info(child_nodes_list)
    # logging.info(table_node_name)
    for data in xml_root.findall(f'.//{table_node_name}'):
        temp_list = []
        for node in child_nodes_list:
            # logging.info(node)
            try:
                node_value = data.find(node).text
            except AttributeError:
                node_value = None
            # logging.info(node_value)
            temp_list.append(node_value)
        # logging.info(temp_list)
        data_list.append(temp_list)
        # logging.info(data_list)
    return data_list


def update_datatable_single_value(config_dict, db_config, table_name, cin_column_name, cin_value,
                                  company_name_column_name,
                                  company_name, column_name, column_value):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    # determine value to be updated
    # if only one key value pair - update value
    # otherwise complete json dictionary
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    logging.info(column_name)

    if column_name == config_dict['total_equity_shares_column_name'] or column_name == \
            config_dict['total_preference_shares_column_name'] or column_name == 'nominal_value_per_share':
        temp_column_value = 0
        for key, value in json_dict.items():
            if value is None:
                continue
            temp_column_value += float(value)
        logging.info(f'{column_value=}')
        column_value = temp_column_value
    elif num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    if column_name == config_dict['registered_full_address_column_name']:
        logging.info(column_value)
        # query to check if address exist in db with cin
        query = 'SELECT {} FROM {} WHERE {} = "{}"'.format(column_name, table_name, cin_column_name, cin_value)

        try:
            db_cursor.execute(query)
        except mysql.connector.Error as err:
            logging.info(err)
        rows = db_cursor.fetchall()
        logging.info(rows)
        logging.info(len(rows))
        for row in rows:
            logging.info(row)
            column_index_to_check = 0
            value_in_column = row[column_index_to_check]
            if value_in_column is None or value_in_column == '' or value_in_column == 'null':
                # if not found update address for cin in Company table
                update_query = 'UPDATE {} SET {} = "{}" WHERE {} = "{}"'.format(table_name,
                                                                                column_name,
                                                                                column_value,
                                                                                cin_column_name,
                                                                                cin_value
                                                                                )
                logging.info(update_query)
                db_cursor.execute(update_query)
                logging.info(f"Updated address for CIN '{cin_value}' in table '{table_name}'")
                return
            else:
                # if cin and address value already exist skip updating
                logging.info(f"Entry for Address field for CIN '{cin_value}' already exist in '{table_name}' Table, hence "
                      f"skipping updating Address")
                return

    # check if there is already entry with cin
    query = 'SELECT * FROM {} WHERE {} = "{}"'.format(table_name, cin_column_name, cin_value)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)
    logging.info(column_value)
    # if cin value already exists
    if len(result) > 0:
        update_query = 'UPDATE {} SET {} = "{}" WHERE {} = "{}"'.format(table_name, column_name,
                                                                                      column_value, cin_column_name,
                                                                                      cin_value
                                                                                      )
        logging.info(update_query)
        db_cursor.execute(update_query)
    # if cin value doesn't exist
    else:
        insert_query = 'INSERT INTO {} ({}, {}, {}) VALUES ("{}", "{}", "{}")'.format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      column_value)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
    db_cursor.close()
    db_connection.close()


def insert_datatable_with_table(db_config, sql_table_name, column_names_list, df_row):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True
    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # logging.info(result_dict)
    cin_column_name = 'cin'
    cin = result_dict[cin_column_name]
    where_clause = f'SELECT * FROM {sql_table_name} WHERE '
    for key, value in result_dict.items():
        if value is not None:
            where_clause += f'`{key}` = "{value}" AND '
        else:
            where_clause += f"(`{key}` is NULL OR `{key}` = '') AND "

    select_query = where_clause[:-4]
    logging.info(select_query)
    db_cursor.execute(select_query)
    result = db_cursor.fetchall()
    logging.info(len(result))
    if len(result) == 0:  # If no matching record found
        # Insert the record
        insert_query = f"""INSERT INTO {sql_table_name} SET """
        for key, value in result_dict.items():
            if value is None:
                insert_query += f"`{key}` = NULL , "
            else:
                insert_query += f'`{key}` = "{value}" , '
        insert_query = insert_query[:-2]
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        # logging.info(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
    else:
        logging.info(f"Entry with values already exists in table {sql_table_name}")
        # update_query = f"""UPDATE {sql_table_name}
        #                                         SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])}
        #                                         WHERE {cin_column_name} = '{cin}'"""
        # logging.info(update_query)
        # db_cursor.execute(update_query)
    db_cursor.close()
    db_connection.close()


def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
              output_file_path, cin_column_value, company_name):
    setup_logging()
    config_dict_keys = ['single_type_indicator',
                        'group_type_indicator',

                        'principal_business_activities_table_name',
                        'year_field_name',

                        'principal_business_activities_field_name',
                        'director_shareholdings_field_name',
                        'Hold_Sub_Assoc_field_name',
                        'director_remuneration_field_name',

                        'Hold_Sub_Assoc_column_name',
                        'cin_column_name_in_db',
                        'company_name_column_name_in_db',

                        'output_sheet_name',

                        'associate_keyword_in_xml',
                        'holding_keyword_in_xml',
                        'joint_venture_keyword_in_xml',
                        'subsidiary_keyword_in_xml'
                        ]
    missing_keys = [key for key in config_dict_keys if key not in config_dict]

    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

    # initializing empty list to save all the result dataframes for saving to output excel
    output_dataframes_list = []

    llp_pattern = r'^[a-zA-Z]{3}-\d{4}$'

    if not os.path.exists(map_file_path):
        raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

    try:
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        logging.info(df_map)
    except Exception as e:
        raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))

    # adding an empty column to mapping df
    df_map['Value'] = None

    # creating new filtered dataframes for single and group values
    single_df = df_map[df_map[df_map.columns[2]] == config_dict['single_type_indicator']]
    group_df = df_map[df_map[df_map.columns[2]] == config_dict['group_type_indicator']]

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
        # field_name = str(row['Field_Name']).strip()
        # parent_node = str(row['Parent_Node']).strip()
        # child_nodes = str(row['Child_Nodes']).strip()
        # sql_table_name = str(row['Table_Name']).strip()
        # column_name = str(row['Column_Name']).strip()
        # column_json_node = str(row['Column_JSON_Node']).strip()

        field_name = str(row.iloc[0]).strip()
        parent_node = str(row.iloc[3]).strip()
        child_nodes = str(row.iloc[4]).strip()
        sql_table_name = str(row.iloc[5]).strip()
        column_name = str(row.iloc[6]).strip()
        column_json_node = str(row.iloc[7]).strip()

        if parent_node == 'Formula':
            continue
        value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        if field_name == 'year' and value is not None:
            logging.info(value)
            try:
                date_obj = datetime.strptime(value, "%Y-%m-%d")
                year = date_obj.year
                single_df.at[index, 'Value'] = year
            except:
                single_df.at[index, 'Value'] = value
        elif field_name == 'phoneNumber' or field_name == 'cin_registrar' or field_name == 'name_registrar' or field_name == 'registrar_address' or field_name == 'pan' or field_name == 'website':
            single_df.at[index, 'Value'] = value
        elif field_name == 'Email':
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            email_check_query = "select email from company where cin = '{}'".format(cin_column_value)
            logging.info(email_check_query)
            cursor.execute(email_check_query)
            email_db = cursor.fetchone()[0]
            logging.info(email_db)
            cursor.close()
            connection.close()
            if email_db is not None:
                if '*' in email_db:
                    logging.info("* is there in email so taking email from MGT")
                    single_df.at[index, 'Value'] = value
                else:
                    logging.info("Email found so skipping")
                    single_df = single_df[single_df['Field_Name'] != 'Email']
            else:
                logging.info("No email found so taking email from MGT")
                single_df.at[index, 'Value'] = value
        else:
            if value is not None:
                if all(char == '0' for char in value):
                    if 'percentage_of_shares' in column_json_node:
                        logging.info("0 in normal format for % of shares")
                        value = float(0.00)
                    else:
                        logging.info("0 in normal format for no of shares")
                        value = int(0)
                elif '.' in value and all(char == '0' for char in value.split('.')[1]) and all(
                        char == '0' for char in value.split('.')[0]):
                    # If yes, convert to the actual value before the decimal point
                    value = value.split('.')[0]
                    if 'percentage_of_shares' in column_json_node:
                        logging.info("0 in decimal format for % of shares")
                        value = float(0.00)
                    else:
                        logging.info("0 in decimal format for no of shares")
                        value = int(0)
                else:
                    logging.info("The string has no zero.")
                    try:
                        value = float(value)
                    except:
                        pass
            else:
                if 'percentage_of_shares' in column_json_node:
                    logging.info("0 in None for % of shares")
                    value = float(0.00)
                else:
                    logging.info("0 in None for no of shares")
                    value = int(0)
            logging.info(value)
            single_df.at[index, 'Value'] = value
        results.append([field_name, value, sql_table_name, column_name, column_json_node])
    logging.info(single_df)
    single_formula_df = single_df[
        single_df[single_df.columns[3]] == 'Formula']
    total_equity_shares = single_df[single_df['Field_Name'] == 'paidup_capital']['Value'].values[0]
    for _, row in single_formula_df.iterrows():
        current_formula = row['Child_Nodes']
        print(current_formula)
        current_formula_field_name = row['Field_Name']
        print(current_formula_field_name)
        for field_name in single_df['Field_Name']:
            pattern = r'\b' + re.escape(field_name) + r'\b'
            print(field_name)
            # current_formula = current_formula.replace(field_name, str(current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]))
            current_formula = re.sub(pattern, str(
                single_df[single_df['Field_Name'] == field_name]['Value'].values[0]), current_formula)
        logging.info(current_formula_field_name + ":" + current_formula)
        try:
            # Calculate the value using the provided formula and insert it
            if 'None' in current_formula:
                current_formula = current_formula.replace('None', '0')
            single_df.at[
                single_df[single_df['Field_Name'] == current_formula_field_name].index[0], 'Value'] = round(
                eval(current_formula), 2)
        except (NameError, SyntaxError):
            # Handle the case where the formula is invalid or contains a missing field name
            logging.info(f"Invalid formula for {current_formula_field_name}: {current_formula}")
    # update single values in datatable
    # get all the tables names for all single values df
    sql_tables_list = single_df[single_df.columns[5]].unique()
    # for each distinct table value, filter the df with table value and find columns
    for sql_table_name in sql_tables_list:
        if sql_table_name == config_dict['principal_business_activities_table_name']:
            # skip if the table is principle business activities table
            # as the year value used to update in other tables along with other values
            continue

        # filter only table
        table_df = single_df[single_df[single_df.columns[5]] == sql_table_name]
        # logging.info(table_df)

        columns_list = table_df[table_df.columns[6]].unique()
        # logging.info(columns_list)
        for column_name in columns_list:
            # logging.info(column_name)
            # filter table df with only column value
            column_df = table_df[table_df[table_df.columns[6]] == column_name]
            # logging.info(column_df)

            # create json dict with keys of field name and values for the same column name entries
            json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
            # Convert the dictionary to a JSON string
            json_string = json.dumps(json_dict)
            # logging.info(json_string)
            if sql_table_name == 'Company':
                company_name_column_name = 'legal_name'
            else:
                company_name_column_name = company_name_column_name_in_db
            try:
                update_datatable_single_value(config_dict, db_config, sql_table_name,
                                              cin_column_name_in_db,
                                              cin_column_value,
                                              company_name_column_name,
                                              company_name, column_name,
                                              json_string)
            except Exception as e:
                logging.info(f"Exception {e} occurred while updating data in dataframe for {sql_table_name} "
                      f"with data {json_string}")

    single_output_df = pd.DataFrame(results, columns=['Field Name', 'Value',
                                                      'Table Name', 'Column Name',
                                                      'Column JSON Node'])
    # logging.info(single_output_df)
    output_dataframes_list.append(single_output_df)
    logging.info("Completed processing single rows")

    # extract group values
    for index, row in group_df.iterrows():
        # field_name = str(row['Field_Name']).strip()
        # type_value = str(row['Type']).strip()
        # parent_node = str(row['Parent_Node']).strip()
        # child_nodes = str(row['Child_Nodes']).strip()
        # sql_table_name = str(row['Table_Name']).strip()
        # column_names = str(row['Column_Name']).strip()
        # column_json_node = str(row['Column_JSON_Node']).strip()
        field_name = str(row.iloc[0]).strip()
        parent_node = str(row.iloc[3]).strip()
        child_nodes = str(row.iloc[4]).strip()
        sql_table_name = str(row.iloc[5]).strip()
        column_names = str(row.iloc[6]).strip()
        column_json_node = str(row.iloc[7]).strip()

        table_node_name = parent_node
        # logging.info(table_node_name)
        try:
            table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
        except Exception as e:
            logging.info(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
            continue
        # logging.info(table_in_list)
        table_df = pd.DataFrame(table_in_list)
        # logging.info(table_df)

        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]
        table_df[cin_column_name_in_db] = cin_column_value
        table_df[company_name_column_name_in_db] = company_name
        column_names_list.append(cin_column_name_in_db)
        column_names_list.append(company_name_column_name_in_db)
        column_names_list = [x.strip() for x in column_names_list]

        if field_name == config_dict['Hold_Sub_Assoc_field_name']:
            column_names_list.insert(2, config_dict['Hold_Sub_Assoc_column_name'])
        # logging.info(column_names_list)
        table_df.columns = column_names_list
        # logging.info(table_df)
        try:
            year_end_date = \
                single_df[single_df[single_df.columns[0]]
                          == config_dict['year_field_name']]['Value'].values[0]
            logging.info(f'{year_end_date=}')
        except Exception as e:
            logging.info(f"Exception {e} occurred while extracting date from xml, setting year to default '01-01-2000'")
            year_end_date = '2000-01-01'

        # update group values in datatable
        if field_name == config_dict['principal_business_activities_field_name']:
            date_obj = datetime.strptime(year_end_date, "%Y-%m-%d")
            year = date_obj.year
            table_df['year'] = year

            # filter out table with nan values
            table_df = table_df[table_df[column_names_list[0]].notna()]
            # logging.info(table_df)

            for _, df_row in table_df.iterrows():
                # logging.info(df_row)
                # logging.info(table_df.columns)
                try:
                    insert_datatable_with_table(db_config, sql_table_name, table_df.columns, df_row)
                except Exception as e:
                    logging.info(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                          df_row)
            logging.info("DB execution is complete for principal business activities")
        elif field_name == config_dict['director_shareholdings_field_name']:
            table_df['financial_year'] = year_end_date
            date_obj = datetime.strptime(year_end_date, "%Y-%m-%d")
            year = date_obj.year
            table_df['year'] = year

            table_df = table_df[table_df[column_names_list[0]].notna()]
            # logging.info(table_df)
            # total_shares = pd.to_numeric(table_df['no_of_shares']).sum()
            # logging.info(total_shares)
            table_df['percentage_holding'] = (pd.to_numeric(table_df['no_of_shares']) / total_equity_shares) * 100
            # logging.info(table_df)

            # Define the mapping of values to be replaced
            designation_mapping = {'DIRT': 'Director',
                                   'MDIR': 'Managing Director',
                                   'WTDR': 'Whole-time director',
                                   'CS': 'Company Secretary'}
            # logging.info(column_names_list)
            table_df[column_names_list[2]] = table_df[column_names_list[2]].replace(designation_mapping)
            # get
            # logging.info(table_df)
            for _, df_row in table_df.iterrows():
                try:
                    insert_datatable_with_table(db_config, sql_table_name, table_df.columns, df_row)
                except Exception as e:
                    logging.info(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                          df_row)
            logging.info("DB execution is complete for director shareholdings table")
        elif field_name == config_dict['Hold_Sub_Assoc_field_name']:
            # logging.info(hold_sub_assoc_table)
            hold_sub_assoc_table = table_df

            # filter table without nan entries
            hold_sub_assoc_table = hold_sub_assoc_table[hold_sub_assoc_table[column_names_list[0]].notna()]
            # logging.info(hold_sub_assoc_table)
            hold_sub_assoc_table_columns = list(hold_sub_assoc_table.columns)
            # logging.info(hold_sub_assoc_table_columns)
            hold_sub_assoc_table_columns.pop(2)
            # logging.info(hold_sub_assoc_table_columns)
            hold_sub_assoc_db_table_columns = hold_sub_assoc_table_columns
            # logging.info(hold_sub_assoc_table)
            # logging.info(sql_table_name)
            sql_tables_list = [x.strip() for x in sql_table_name.split(',')]
            # logging.info(sql_tables_list)

            for _, df_row in hold_sub_assoc_table.iterrows():
                hold_sub_assoc_value = df_row[config_dict['Hold_Sub_Assoc_column_name']]
                cin_value = df_row[column_names_list[1]]
                logging.info(hold_sub_assoc_value)
                try:
                    cin_length = len(cin_value)
                except TypeError:
                    cin_length = 0
                # logging.info(hold_sub_assoc_value)
                # logging.info(cin_value)
                # logging.info(cin_length)
                # logging.info(df_row)

                # removing value of hold sub assoc value to have only the values to append to datatables
                df_row.pop(config_dict['Hold_Sub_Assoc_column_name'])
                # logging.info(df_row)

                # These values are for testing, uncomment all for all tables testing
                # hold_sub_assoc_value = 'ASSOC'
                if ((str(hold_sub_assoc_value).lower() == str(config_dict['associate_keyword_in_xml']).lower()) or
                        (str(hold_sub_assoc_value).lower() in str(config_dict['associate_keyword_in_xml']).lower()) or
                        (str(config_dict['associate_keyword_in_xml']).lower() in str(hold_sub_assoc_value).lower())):
                    associate_tables_list = [item for item in sql_tables_list if
                                             config_dict['associate_table_keyword'] in
                                             item.lower()]
                    logging.info(associate_tables_list)
                    # cin_length = 21  # These values are for testing, uncomment for all the tables testing
                    if cin_length == 21:
                        # companies
                        try:
                            sql_table_companies = \
                                [item for item in associate_tables_list if 'companies' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for company associates {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_companies, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_companies}- \n', df_row)
                    # cin_length = 8
                    elif cin_length == 8 and bool(re.match(llp_pattern, cin_value)):
                        # llp
                        try:
                            sql_table_llp = [item for item in associate_tables_list if 'llp' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for llp associates {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_llp, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_llp}- \n', df_row)
                    # cin_length = 218
                    else:
                        # others
                        try:
                            sql_table_others = [item for item in associate_tables_list if 'others' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for other associates {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_others, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_others}- \n', df_row)

                # hold_sub_assoc_value = 'HOLD'
                if ((str(hold_sub_assoc_value).lower() == str(config_dict['holding_keyword_in_xml']).lower()) or
                        (str(hold_sub_assoc_value).lower() in str(config_dict['holding_keyword_in_xml']).lower()) or
                        (str(config_dict['holding_keyword_in_xml']).lower() in str(hold_sub_assoc_value).lower())):
                    holding_tables_list = [item for item in sql_tables_list if config_dict['holding_table_keyword'] in
                                           item.lower()]
                    # logging.info(holding_tables_list)
                    # cin_length = 21
                    if cin_length == 21:
                        # companies
                        try:
                            sql_table_companies = [item for item in holding_tables_list if 'companies' in item.lower()][
                                0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for company holding {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_companies, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_companies}- \n', df_row)
                    # cin_length = 8
                    elif cin_length == 8 and bool(re.match(llp_pattern, cin_value)):
                        # llp
                        try:
                            sql_table_llp = [item for item in holding_tables_list if 'llp' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for llp holding {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_llp, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_llp}- \n', df_row)
                    # cin_length = 218
                    else:
                        # others
                        try:
                            sql_table_others = [item for item in holding_tables_list if 'others' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for other holding {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_others, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_others}- \n', df_row)
                # hold_sub_assoc_value = 'JOINT'
                joint_keywords = config_dict['joint_venture_keyword_in_xml'].split(',')
                joint_keywords_lower = [keyword.strip().lower() for keyword in joint_keywords]
                if str(hold_sub_assoc_value).lower() in joint_keywords_lower:
                    joint_tables_list = [item for item in sql_tables_list if config_dict['joint_table_keyword'] in
                                         item.lower()]
                    # logging.info(joint_tables_list)
                    # cin_length = 21
                    if cin_length == 21:
                        # companies
                        try:
                            sql_table_companies = [item for item in joint_tables_list if 'companies' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for company joint {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_companies, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_companies}- \n', df_row)
                    # cin_length = 8
                    elif cin_length == 8 and bool(re.match(llp_pattern, cin_value)):
                        # llp
                        try:
                            sql_table_llp = [item for item in joint_tables_list if 'llp' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for llp joint {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_llp, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_llp}- \n', df_row)
                    # cin_length = 218
                    else:
                        # others
                        try:
                            sql_table_others = [item for item in joint_tables_list if 'others' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for other joint {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_others, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_others}- \n', df_row)
                # hold_sub_assoc_value = 'SUBS'
                if ((str(hold_sub_assoc_value).lower() == str(config_dict['subsidiary_keyword_in_xml']).lower()) or
                        (str(hold_sub_assoc_value).lower() in str(config_dict['subsidiary_keyword_in_xml']).lower()) or
                        (str(config_dict['subsidiary_keyword_in_xml']).lower() in str(hold_sub_assoc_value).lower())):
                    subsidiary_tables_list = [item for item in sql_tables_list if
                                              config_dict['subsidiary_table_keyword'] in
                                              item.lower()]
                    # logging.info(subsidiary_tables_list)
                    # cin_length = 21
                    if cin_length == 21:
                        # companies
                        try:
                            sql_table_companies = \
                                [item for item in subsidiary_tables_list if 'companies' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for company holding {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_companies, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_companies}- \n', df_row)
                    # cin_length = 8
                    elif cin_length == 8 and bool(re.match(llp_pattern, cin_value)):
                        # llp
                        try:
                            sql_table_llp = [item for item in subsidiary_tables_list if 'llp' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for llp holding {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_llp, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_llp}- \n', df_row)
                    # cin_length = 218
                    else:
                        # others
                        try:
                            sql_table_others = [item for item in subsidiary_tables_list if 'others' in item.lower()][0]
                        except Exception as e:
                            logging.error(f"Exception occured while getting list of tables for other holding {e}")
                            continue
                        try:
                            insert_datatable_with_table(db_config, sql_table_others, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            logging.info(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_others}- \n', df_row)
                else:
                    pass

            logging.info("DB execution is complete for Holding/Subsidiary/Associate/Joint Venture tables")
            pass
        elif field_name == config_dict['director_remuneration_field_name']:
            date_obj = datetime.strptime(year_end_date, "%Y-%m-%d")
            year = date_obj.year
            table_df['year'] = year
            table_df = table_df[table_df[column_names_list[0]].notna()]
            # logging.info(table_df)
            for _, df_row in table_df.iterrows():
                try:
                    insert_datatable_with_table(db_config, sql_table_name, table_df.columns, df_row)
                except Exception as e:
                    logging.info(f'Exception {e} occurred while inserting below table row in table '
                          f'{sql_table_name}- \n', df_row)
            logging.info("DB execution is complete for director remuneration table")
        else:
            pass
        output_dataframes_list.append(table_df)

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # logging.info(dataframe)
            dataframe.to_excel(writer, sheet_name=config_dict['output_sheet_name'], index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()


def mgt7_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                   output_file_path, cin_column_value, company_name):
    try:
        setup_logging()
        xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                  output_file_path, cin_column_value, company_name)
    except Exception as e:
        logging.info("Below Exception occurred while processing mgt7 file: \n ", e)
        return False
    else:
        return True
