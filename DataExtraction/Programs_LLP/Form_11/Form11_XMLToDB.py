import json
import re
import sys
import traceback

import pandas as pd
import xml.etree.ElementTree as Et
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
        print(parent_node)
        print(child_node)
        print(f"Below error occurred for processing parent node: {parent_node} and child node: {child_node}"
              f"\n {e}")
        return None


def update_database_single_value(db_config, config_dict, table_name, cin_column_name, cin_value,
                                 column_name, column_value):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        if first_value is None:
            print(f"Value is None for table '{table_name}' column '{column_name}'")
            return
        column_value = first_value.replace('"', '\\"').replace("'", "\\'")
        # print(column_value)
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    if table_name == config_dict['principal_business_activities_table_name']:
        year_column_name = config_dict['year_column_name']
        year = config_dict['year']
        query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}'".format(table_name, cin_column_name, cin_value,
                                                                        year_column_name, year)
    else:
        query = "SELECT * FROM {} WHERE {} = '{}'".format(table_name, cin_column_name, cin_value)
    print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        print(err)
    result = db_cursor.fetchall()
    # print(result)
    print(column_value)
    # if cin value already exists
    if len(result) > 0:
        print(f"cin {cin_value} is exist in table {table_name}")
        if table_name == config_dict['principal_business_activities_table_name']:
            year_column_name = config_dict['year_column_name']
            year = config_dict['year']
            if column_name == year_column_name:
                return
            update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} ='{}'".format(table_name,
                                                                                         column_name, column_value,
                                                                                         cin_column_name, cin_value,
                                                                                         year_column_name,
                                                                                         year
                                                                                         )
        else:
            update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}'".format(table_name,
                                                                            column_name, column_value,
                                                                            cin_column_name, cin_value
                                                                            )
        print(update_query)
        try:
            db_cursor.execute(update_query)
        except Exception as e:
            raise e
        else:
            print("updated")

    # if cin value doesn't exist
    else:
        print(f"cin {cin_value} is not exist in table {table_name}")
        # print(type(cin_value))
        # print(type(column_value))
        if table_name == config_dict['principal_business_activities_table_name']:
            year_column_name = config_dict['year_column_name']
            year = config_dict['year']
            if column_name == year_column_name:
                return
            insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name,
                                                                                          cin_column_name,
                                                                                          year_column_name,
                                                                                          column_name,
                                                                                          cin_value,
                                                                                          year,
                                                                                          column_value
                                                                                          )
        else:
            insert_query = "INSERT INTO {} ({}, {}) VALUES ('{}', '{}')".format(table_name, cin_column_name,
                                                                                column_name,
                                                                                cin_value,
                                                                                column_value)
        print(insert_query)
        try:
            db_cursor.execute(insert_query)
        except Exception as e:
            raise e
        else:
            print("inserted")
    db_cursor.close()
    db_connection.close()


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


def insert_datatable_with_table(db_config, config_dict, sql_table_name, column_names_list, df_row,
                                cin_column_name):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True
    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # print(result_dict)
    cin = result_dict[cin_column_name]
    # print(cin)
    if sql_table_name == config_dict['authorized_signatories_table_name']:
        din_column_name = config_dict['din_column_name']
        din = result_dict[din_column_name]

        pan_column_name = config_dict['pan_column_name']
        pan = result_dict[pan_column_name]

        if din is None and pan is not None:
            select_query = (f"SELECT * FROM {sql_table_name} WHERE {cin_column_name} = '{cin}' AND {pan_column_name}"
                            f" = '{pan}'")
        elif pan is None and din is not None:
            select_query = (f"SELECT * FROM {sql_table_name} WHERE {cin_column_name} = '{cin}' AND {din_column_name}"
                            f" = '{din}'")
        elif pan is not None and din is not None:
            select_query = (f"SELECT * FROM {sql_table_name} WHERE {cin_column_name} = '{cin}' AND {din_column_name}"
                            f" = '{din}' AND {pan_column_name} = '{pan}'")
        else:
            raise Exception(f"Both DIN and PAN values are empty for director's data in table {sql_table_name} "
                            f"with below data \n {list(df_row)} ")

    elif sql_table_name == config_dict['individual_partners_table_name']:
        signer_id_column_name = config_dict['signer_id_column_name']
        signer_id_value = result_dict[signer_id_column_name]

        financial_year_column_name = config_dict['financial_year_column_name']
        financial_year = result_dict[financial_year_column_name]

        if signer_id_value is not None and financial_year is not None:
            select_query = (f"SELECT * FROM {sql_table_name} WHERE {cin_column_name} = '{cin}' AND "
                            f"{signer_id_column_name} ="
                            f" '{signer_id_value}' AND {financial_year_column_name} = '{financial_year}'")
        else:
            raise Exception(f"one of the Signer id and financial year value is not available to save in "
                            f"{sql_table_name} data")

    elif sql_table_name == config_dict['body_corporates_table_name']:
        nominee_id_column_name = config_dict['nominee_id_column_name']
        nominee_id_value = result_dict[nominee_id_column_name]

        if nominee_id_value is not None:
            select_query = (f"SELECT * FROM {sql_table_name} WHERE {cin_column_name} = '{cin}' AND "
                            f"{nominee_id_column_name} = '{nominee_id_value}'")
        else:
            raise Exception(f"Nominee id is not available for sql query for table {sql_table_name}")
    else:
        raise Exception(f"{sql_table_name} is not related to Group datatable")
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
        print(f'{insert_query=}')
        db_cursor.execute(insert_query)
        print(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
    if len(result) > 0:  # If matching record found
        if sql_table_name == config_dict['authorized_signatories_table_name']:
            din_column_name = config_dict['din_column_name']
            din = result_dict[din_column_name]

            pan_column_name = config_dict['pan_column_name']
            pan = result_dict[pan_column_name]

            result_dict.pop(cin_column_name)
            result_dict.pop(din_column_name)
            result_dict.pop(pan_column_name)

            column_names_list = list(column_names_list)
            column_names_list.remove(cin_column_name)
            column_names_list.remove(din_column_name)
            column_names_list.remove(pan_column_name)
            if din is None and pan is not None:
                update_query = f'''UPDATE {sql_table_name}
                                SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])} 
                                WHERE {cin_column_name} = '{cin}' AND {pan_column_name} = '{pan}' '''
            elif pan is None and din is not None:
                update_query = f'''UPDATE {sql_table_name}
                                SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])} 
                                WHERE {cin_column_name} = '{cin}' AND {din_column_name} = '{din}' '''
            elif pan is not None and din is not None:
                update_query = f'''UPDATE {sql_table_name}
                        SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])} 
                        WHERE {cin_column_name} = '{cin}' AND {din_column_name} = '{din}' AND {pan_column_name} = 
                        '{pan}' 
                    '''
            else:
                raise Exception(f"Both DIN and PAN values are empty for director's data in table {sql_table_name} "
                                f"with below data \n {list(df_row)} ")
            print(update_query)
            db_cursor.execute(update_query)

            print(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")

        elif sql_table_name == config_dict['individual_partners_table_name']:
            signer_id_column_name = config_dict['signer_id_column_name']
            signer_id_value = result_dict[signer_id_column_name]

            financial_year_column_name = config_dict['financial_year_column_name']
            financial_year = result_dict[financial_year_column_name]

            result_dict.pop(cin_column_name)
            result_dict.pop(signer_id_column_name)
            result_dict.pop(financial_year_column_name)

            column_names_list = list(column_names_list)
            column_names_list.remove(cin_column_name)
            column_names_list.remove(signer_id_column_name)
            column_names_list.remove(financial_year_column_name)

            if signer_id_value is not None and financial_year is not None:
                update_query = f'''UPDATE {sql_table_name}
                            SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])} 
                            WHERE {cin_column_name} = '{cin}' AND 
                                    {signer_id_column_name} = '{signer_id_value}' AND 
                                    {financial_year_column_name} = '{financial_year}' 
                                    '''
            else:
                raise Exception(f"one of the Signer id and financial year value is not available to save in "
                                f"{sql_table_name} data")
            print(update_query)
            db_cursor.execute(update_query)

            print(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")

        elif sql_table_name == config_dict['body_corporates_table_name']:
            nominee_id_column_name = config_dict['nominee_id_column_name']
            nominee_id_value = result_dict[nominee_id_column_name]

            result_dict.pop(cin_column_name)
            result_dict.pop(nominee_id_column_name)

            column_names_list = list(column_names_list)
            column_names_list.remove(cin_column_name)
            column_names_list.remove(nominee_id_column_name)

            if nominee_id_value is not None:
                update_query = f'''UPDATE {sql_table_name}
                                SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])} 
                                WHERE {cin_column_name} = '{cin}' AND {nominee_id_column_name} = '{nominee_id_value}' '''
            else:
                raise Exception(f"Nominee id is not available for sql query for table {sql_table_name}")
            print(update_query)
            db_cursor.execute(update_query)
            print(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")

        else:
            pass

    db_cursor.close()
    db_connection.close()


def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
              output_file_path, cin):
    config_dict_keys = ['single_type_indicator', 'group_type_indicator', 'cin_column_name',
                        'field_name_index', 'type_index',
                        'parent_node_index',
                        'child_nodes_index', 'sql_table_name_index',
                        'column_name_index', 'column_json_node_index',
                        'Company_table_name', 'summary_designated_partners_table_name',
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

    field_name_index = int(config_dict['field_name_index'])
    single_group_type_index = int(config_dict['type_index'])
    parent_node_index = int(config_dict['parent_node_index'])
    child_nodes_index = int(config_dict['child_nodes_index'])
    sql_table_name_index = int(config_dict['sql_table_name_index'])
    column_name_index = int(config_dict['column_name_index'])
    column_json_node_index = int(config_dict['column_json_node_index'])

    if not os.path.exists(map_file_path):
        raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

    try:
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        # print(df_map)
        selected_indices = [field_name_index, single_group_type_index,
                            parent_node_index, child_nodes_index,
                            sql_table_name_index, column_name_index,
                            column_json_node_index
                            ]

        # Use iloc to select columns by indices
        df_map = df_map.iloc[:, selected_indices]
        # print(df_map)

        # resetting indices after rearranging of the dataframe
        field_name_index = selected_indices.index(field_name_index)
        single_group_type_index = selected_indices.index(single_group_type_index)
        parent_node_index = selected_indices.index(parent_node_index)
        child_nodes_index = selected_indices.index(child_nodes_index)
        sql_table_name_index = selected_indices.index(sql_table_name_index)
        column_name_index = selected_indices.index(column_name_index)
        column_json_node_index = selected_indices.index(column_json_node_index)

    except Exception as e:
        raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))

    # adding an empty column to mapping df
    df_map['Value'] = None

    single_df = df_map[df_map[df_map.columns[single_group_type_index]] == config_dict['single_type_indicator']]
    # print("Single df")
    # print(single_df)
    group_df = df_map[df_map[df_map.columns[single_group_type_index]] == config_dict['group_type_indicator']]
    # print("group df")
    # print(group_df)
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
    # print(single_df)

    # update single values in datatable
    # get all the tables names for all single values df
    sql_tables_list = single_df[single_df.columns[sql_table_name_index]].unique()
    print(sql_tables_list)
    # for each distinct table value, filter the df with table value and find columns
    for sql_table_name in sql_tables_list:
        print(sql_table_name)
        # filter only table

        if (sql_table_name == config_dict["Company_table_name"] or sql_table_name ==
                config_dict["principal_business_activities_table_name"]):
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
                if column_name == config_dict['year_column_name']:
                    year = column_df['Value'].iloc[0]
                    print(year)
                    config_dict['year'] = year
                try:
                    update_database_single_value(db_config, config_dict, sql_table_name,
                                                 cin_column_name,
                                                 cin,
                                                 column_name,
                                                 json_string)
                except Exception as e:
                    print(f"Exception {e} occurred while updating data in dataframe for {sql_table_name} "
                          f"with data {json_string}")
                else:
                    print(f'{sql_table_name} Table is updated')

        elif sql_table_name == config_dict["individual_partners_table_name"]:
            continue
        elif sql_table_name == config_dict["summary_designated_partners_table_name"]:
            continue
        else:
            continue

    financial_year_row_index = single_df[single_df[single_df.columns[column_name_index]] ==
                                         config_dict['financial_year_column_name']].index[0]
    print(f'{financial_year_row_index=}')
    if financial_year_row_index is not None:
        financial_year = single_df.loc[financial_year_row_index, 'Value']
    else:
        print(f"financial year details is not  in mapping file.")
        financial_year = None
    print(f'{financial_year=}')

    single_output_df = pd.DataFrame(results, columns=['Field Name', 'Value',
                                                      'Table Name', 'Column Name',
                                                      'Column JSON Node'])
    # print(single_output_df)
    output_dataframes_list.append(single_output_df)
    print("Completed processing single rows")

    # extract group values
    for index, row in group_df.iterrows():
        field_name = str(row.iloc[field_name_index]).strip()
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[sql_table_name_index]).strip()
        column_names = str(row.iloc[column_name_index]).strip()
        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]
        print(f'{column_names_list=}')
        table_node_name = parent_node
        # print(table_node_name)
        try:
            print(f'{table_node_name=}')
            print(f'{child_nodes=}')
            table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
            table_df = pd.DataFrame(table_in_list)
            table_df.columns = column_names_list
            # print(table_df)

        except Exception as e:
            print(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
            continue

        # print(table_df)

        table_df[cin_column_name] = cin
        column_names_list.append(cin_column_name)
        # print(table_df)
        if sql_table_name == config_dict['individual_partners_table_name']:
            table_df[config_dict['financial_year_column_name']] = financial_year
            column_names_list.append(config_dict['financial_year_column_name'])
        table_df.columns = column_names_list
        table_df = table_df[table_df[column_names_list[field_name_index]].notna()]
        for _, df_row in table_df.iterrows():
            try:
                if sql_table_name == config_dict["authorized_signatories_table_name"]:
                    if str(df_row[config_dict['din_column_name']]).isnumeric():
                        df_row[config_dict['pan_column_name']] = None
                    elif str(df_row[config_dict['pan_column_name']]).isalnum():
                        df_row[config_dict['din_column_name']] = None
                elif sql_table_name == config_dict["individual_partners_table_name"]:
                    if str(df_row[config_dict['signer_id_column_name']]).isnumeric():
                        df_row[config_dict['id_type_column_name']] = config_dict['din_value']
                    elif str(df_row[config_dict['signer_id_column_name']]).isalnum():
                        df_row[config_dict['id_type_column_name']] = config_dict['pan_value']
                    else:
                        df_row[config_dict['id_type_column_name']] = ''

                elif sql_table_name == config_dict["body_corporates_table_name"]:
                    # find id type
                    # signer_id = df_row[config_dict['signer_id_column_name']]
                    # id_length = len(signer_id)
                    # llp_pattern = r'^[a-zA-Z]{3}-\d{4}$'
                    # if id_length == 21:
                    #      id_type = config_dict['cin_type_value']
                    # elif id_length == 8 and bool(re.match(llp_pattern, signer_id)):
                    #     id_type = config_dict['llp_type_value']
                    # else:
                    #     id_type = config_dict['others_type_value']
                    # df_row[config_dict['id_type_column_name']] = id_type

                    # find nominee id type
                    nominee_id = df_row[config_dict['nominee_id_column_name']]
                    din_pattern = re.compile(r'^\d{7}$')
                    pan_pattern = re.compile(r'^[A-Za-z0-9]{10}$')
                    if bool(din_pattern.match(nominee_id)):
                        nominee_id_type = config_dict['din_value']
                    elif bool(pan_pattern.match(nominee_id)):
                        nominee_id_type = config_dict['pan_value']
                    else:
                        nominee_id_type = config_dict['others_nominee_type_value']
                    print(nominee_id_type)
                    df_row[config_dict['nominee_id_type_column_name']] = nominee_id_type
                print(df_row)
                insert_datatable_with_table(db_config, config_dict, sql_table_name, table_df.columns, df_row,
                                            cin_column_name)

            except Exception as e:
                print(f"Exception '{e}' occurred while inserting below table row in table {sql_table_name}- \n",
                      df_row)
        print(f"DB execution is complete for {sql_table_name}")
        output_dataframes_list.append(table_df)

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name=config_dict['output_sheet_name'], index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()


def form_11_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
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
