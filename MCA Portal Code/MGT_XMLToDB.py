import re
from datetime import datetime

import pandas as pd
import xml.etree.ElementTree as Et
import json
import os
import mysql.connector
import sys
import traceback
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
        # print(update_query)
        db_cursor.execute(update_query)
    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      column_value)
        # print(insert_query)
        db_cursor.execute(insert_query)


def insert_datatable_with_table(db_cursor, sql_table_name, column_names_list, df_row):
    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # print(result_dict)

    where_clause = f'SELECT * FROM {sql_table_name} WHERE '
    for key, value in result_dict.items():
        if value is not None:
            where_clause += f"`{key}` = '{value}' AND "
        else:
            where_clause += f"(`{key}` is NULL OR `{key}` = '') AND "

    select_query = where_clause[:-4]
    print(select_query)
    db_cursor.execute(select_query)
    result = db_cursor.fetchall()
    print(len(result))
    if len(result) == 0:  # If no matching record found
        # Insert the record
        insert_query = f"""INSERT INTO {sql_table_name} SET """
        for key, value in result_dict.items():
            if value is None:
                insert_query += f"`{key}` = NULL , "
            else:
                insert_query += f"`{key}` = '{value}' , "
        insert_query = insert_query[:-2]
        print(insert_query)
        db_cursor.execute(insert_query)
        # print(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
    else:
        print(f"Entry with values already exists in table {sql_table_name}")


def xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
              output_file_path, cin_column_value, company_name):
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
        print(df_map)
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

        value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        print(value)
        if field_name == 'year':
            date_obj = datetime.strptime(value, "%Y-%m-%d")
            year = date_obj.year
            single_df.at[index, 'Value'] = year
        else:
            if all(char == '0' for char in value):
                # If yes, convert the entire value to '0'
                value = '0'
            elif '.' in value and all(char == '0' for char in value.split('.')[1]):
                # If yes, convert to the actual value before the decimal point
                value = value.split('.')[0]
            if 'no_of_shares' in column_json_node:
                print("The string contains 'no_of_shares'.")
                try:
                    value = int(value)
                    print(value)
                except:
                    pass
            single_df.at[index, 'Value'] = value
        results.append([field_name, value, sql_table_name, column_name, column_json_node])
    # print(single_df)

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
            if sql_table_name == 'Company':
                company_name_column_name = 'legal_name'
            else:
                company_name_column_name = company_name_column_name_in_db
            try:
                update_datatable_single_value(db_cursor, sql_table_name,
                                              cin_column_name_in_db,
                                              cin_column_value,
                                              company_name_column_name,
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
        # print(table_node_name)
        try:
            table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
        except Exception as e:
            print(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
            continue
        # print(table_in_list)
        table_df = pd.DataFrame(table_in_list)
        # print(table_df)

        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]
        table_df[cin_column_name_in_db] = cin_column_value
        table_df[company_name_column_name_in_db] = company_name
        column_names_list.append(cin_column_name_in_db)
        column_names_list.append(company_name_column_name_in_db)
        column_names_list = [x.strip() for x in column_names_list]

        if field_name == config_dict['Hold_Sub_Assoc_field_name']:
            column_names_list.insert(2, config_dict['Hold_Sub_Assoc_column_name'])
        # print(column_names_list)
        table_df.columns = column_names_list
        # print(table_df)
        try:
            year_end_date = \
                single_df[single_df[single_df.columns[0]]
                          == config_dict['year_field_name']]['Value'].values[0]
            # print(f'{year_end_date=}')
        except Exception as e:
            print(f"Exception {e} occurred while extracting date from xml, setting year to default '00-00-0000'")
            year_end_date = '2000-01-01'

        # update group values in datatable
        if field_name == config_dict['principal_business_activities_field_name']:
            table_df['Year'] = year_end_date

            # filter out table with nan values
            table_df = table_df[table_df[column_names_list[0]].notna()]
            # print(table_df)

            for _, df_row in table_df.iterrows():
                # print(df_row)
                # print(table_df.columns)
                try:
                    insert_datatable_with_table(db_cursor, sql_table_name, table_df.columns, df_row)
                except Exception as e:
                    print(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                          df_row)
            print("DB execution is complete for principal business activities")
        elif field_name == config_dict['director_shareholdings_field_name']:
            table_df['financial_year'] = year_end_date
            date_obj = datetime.strptime(year_end_date, "%Y-%m-%d")
            year = date_obj.year
            table_df['year'] = year

            table_df = table_df[table_df[column_names_list[0]].notna()]
            # print(table_df)
            total_shares = pd.to_numeric(table_df['no_of_shares']).sum()
            # print(total_shares)
            table_df['percentage_holding'] = (pd.to_numeric(table_df['no_of_shares'])/total_shares) * 100
            # print(table_df)

             # Define the mapping of values to be replaced
            designation_mapping = {'DIRT': 'Director',
                                   'MDIR': 'Managing Director',
                                   'WTDR': 'Whole-time director',
                                   'CS': 'Company Secretary'}
            # print(column_names_list)
            table_df[column_names_list[2]] = table_df[column_names_list[2]].replace(designation_mapping)
            # get
            # print(table_df)
            for _, df_row in table_df.iterrows():
                try:
                    insert_datatable_with_table(db_cursor, sql_table_name, table_df.columns, df_row)
                except Exception as e:
                    print(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                          df_row)
            print("DB execution is complete for director shareholdings table")
        elif field_name == config_dict['Hold_Sub_Assoc_field_name']:
            # print(hold_sub_assoc_table)
            hold_sub_assoc_table = table_df

            # filter table without nan entries
            hold_sub_assoc_table = hold_sub_assoc_table[hold_sub_assoc_table[column_names_list[0]].notna()]
            # print(hold_sub_assoc_table)
            hold_sub_assoc_table_columns = list(hold_sub_assoc_table.columns)
            # print(hold_sub_assoc_table_columns)
            hold_sub_assoc_table_columns.pop(2)
            # print(hold_sub_assoc_table_columns)
            hold_sub_assoc_db_table_columns = hold_sub_assoc_table_columns
            # print(hold_sub_assoc_table)
            # print(sql_table_name)
            sql_tables_list = [x.strip() for x in sql_table_name.split(',')]
            # print(sql_tables_list)

            for _, df_row in hold_sub_assoc_table.iterrows():
                hold_sub_assoc_value = df_row[config_dict['Hold_Sub_Assoc_column_name']]
                cin_value = df_row[column_names_list[1]]
                try:
                    cin_length = len(cin_value)
                except TypeError:
                    cin_length = 0
                # print(hold_sub_assoc_value)
                # print(cin_value)
                # print(cin_length)
                # print(df_row)

                # removing value of hold sub assoc value to have only the values to append to datatables
                df_row.pop(config_dict['Hold_Sub_Assoc_column_name'])
                # print(df_row)

                # These values are for testing, uncomment all for all tables testing
                # hold_sub_assoc_value = 'ASSOC'
                if ((hold_sub_assoc_value == config_dict['associate_keyword_in_xml']) or
                        (hold_sub_assoc_value in config_dict['associate_keyword_in_xml']) or
                        (config_dict['associate_keyword_in_xml'] in hold_sub_assoc_value)):
                    associate_tables_list = [item for item in sql_tables_list if
                                             str(hold_sub_assoc_value).lower() in
                                             item.lower()]
                    # print(associate_tables_list)
                    # cin_length = 21  # These values are for testing, uncomment for all the tables testing
                    if cin_length == 21:
                        # companies
                        sql_table_companies = \
                            [item for item in associate_tables_list if 'companies' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_companies, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_companies}- \n', df_row)
                    # cin_length = 8
                    elif cin_length == 8 and bool(re.match(llp_pattern, cin_value)):
                        # llp
                        sql_table_llp = [item for item in associate_tables_list if 'llp' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_llp, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_llp}- \n', df_row)
                    # cin_length = 218
                    else:
                        # others
                        sql_table_others = [item for item in associate_tables_list if 'others' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_others, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_others}- \n', df_row)

                # hold_sub_assoc_value = 'HOLD'
                if ((hold_sub_assoc_value == config_dict['holding_keyword_in_xml']) or
                        (hold_sub_assoc_value in config_dict['holding_keyword_in_xml']) or
                        (config_dict['holding_keyword_in_xml'] in hold_sub_assoc_value)):
                    holding_tables_list = [item for item in sql_tables_list if str(hold_sub_assoc_value).lower() in
                                           item.lower()]
                    # print(holding_tables_list)
                    # cin_length = 21
                    if cin_length == 21:
                        # companies
                        sql_table_companies = [item for item in holding_tables_list if 'companies' in item.lower()][
                            0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_companies, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_companies}- \n', df_row)
                    # cin_length = 8
                    elif cin_length == 8 and bool(re.match(llp_pattern, cin_value)):
                        # llp
                        sql_table_llp = [item for item in holding_tables_list if 'llp' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_llp, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_llp}- \n', df_row)
                    # cin_length = 218
                    else:
                        # others
                        sql_table_others = [item for item in holding_tables_list if 'others' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_others, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_others}- \n', df_row)
                # hold_sub_assoc_value = 'JOINT'
                if ((hold_sub_assoc_value == config_dict['joint_venture_keyword_in_xml']) or
                        (hold_sub_assoc_value in config_dict['joint_venture_keyword_in_xml']) or
                        (config_dict['joint_venture_keyword_in_xml'] in hold_sub_assoc_value)):
                    joint_tables_list = [item for item in sql_tables_list if str(hold_sub_assoc_value).lower() in
                                         item.lower()]
                    # print(joint_tables_list)
                    # cin_length = 21
                    if cin_length == 21:
                        # companies
                        sql_table_companies = [item for item in joint_tables_list if 'companies' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_companies, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_companies}- \n', df_row)
                    # cin_length = 8
                    elif cin_length == 8 and bool(re.match(llp_pattern, cin_value)):
                        # llp
                        sql_table_llp = [item for item in joint_tables_list if 'llp' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_llp, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_llp}- \n', df_row)
                    # cin_length = 218
                    else:
                        # others
                        sql_table_others = [item for item in joint_tables_list if 'others' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_others, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_others}- \n', df_row)
                # hold_sub_assoc_value = 'SUBS'
                if ((hold_sub_assoc_value == config_dict['subsidiary_keyword_in_xml']) or
                        (hold_sub_assoc_value in config_dict['subsidiary_keyword_in_xml']) or
                        (config_dict['subsidiary_keyword_in_xml'] in hold_sub_assoc_value)):
                    subsidiary_tables_list = [item for item in sql_tables_list if
                                              str(hold_sub_assoc_value).lower() in
                                              item.lower()]
                    # print(subsidiary_tables_list)
                    # cin_length = 21
                    if cin_length == 21:
                        # companies
                        sql_table_companies = \
                            [item for item in subsidiary_tables_list if 'companies' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_companies, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_companies}- \n', df_row)
                    # cin_length = 8
                    elif cin_length == 8 and bool(re.match(llp_pattern, cin_value)):
                        # llp
                        sql_table_llp = [item for item in subsidiary_tables_list if 'llp' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_llp, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_llp}- \n', df_row)
                    # cin_length = 218
                    else:
                        # others
                        sql_table_others = [item for item in subsidiary_tables_list if 'others' in item.lower()][0]
                        try:
                            insert_datatable_with_table(db_cursor, sql_table_others, hold_sub_assoc_db_table_columns,
                                                        df_row)
                        except Exception as e:
                            print(f'Exception {e} occurred while inserting below table row in table '
                                  f'{sql_table_others}- \n', df_row)
                else:
                    pass

            print("DB execution is complete for Holding/Subsidiary/Associate/Joint Venture tables")
            pass
        elif field_name == config_dict['director_remuneration_field_name']:
            table_df['Year'] = year_end_date
            table_df = table_df[table_df[column_names_list[0]].notna()]
            # print(table_df)
            for _, df_row in table_df.iterrows():
                try:
                    insert_datatable_with_table(db_cursor, sql_table_name, table_df.columns, df_row)
                except Exception as e:
                    print(f'Exception {e} occurred while inserting below table row in table '
                          f'{sql_table_name}- \n', df_row)
            print("DB execution is complete for director remuneration table")
        else:
            pass
        output_dataframes_list.append(table_df)

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name=config_dict['output_sheet_name'], index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()


def mgt7_xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                   output_file_path, cin_column_value, company_name):
    try:
        xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                  output_file_path, cin_column_value, company_name)
    except Exception as e:
        print("Below Exception occurred while processing mgt7 file: \n ", e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # Print the traceback details
        for line in traceback_details:
            print(line.strip())

        return False
    else:
        return True