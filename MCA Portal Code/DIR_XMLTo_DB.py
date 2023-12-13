import sys
import traceback
from datetime import datetime

import pandas as pd
import xml.etree.ElementTree as Et
import os
import mysql.connector
from Config import create_main_config_dictionary
from HiddenFields import dir_hidden_fields
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
        print(f"Below error occurred for processing parent node: {parent_node} and child node: {child_node}"
              f"\n {e}")
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


def extract_table_values_from_hidden_xml(xml_root, table_node_name, child_nodes):
    print(xml_root)
    data_list = []
    child_nodes_list = [x.strip() for x in child_nodes.split(',')]
    print(table_node_name)
    print(child_nodes_list)
    field_values_list = []
    ns = {'ns0': 'http://example.com/ns/form'}
    director1_subform = xml_root.findall(".//ns0:subform[@name='TextField1_C']", namespaces=ns)
    print(director1_subform)
    if director1_subform is not None:
        print("Found")
    # Found subform with name 'Director1'
    # Process it here
    else:
        print("Not found")
    # No subform with name 'Director1' found

    # for director_node in xml_root.findall(f".//subform[@name={table_node_name}]"):
    # for director_node in xml_root.findall(f".//subform[@name='{table_node_name}']"):
    #     print(director_node)
    field_values = {}
    temp_list = []
    field_value = xml_root.find(".//field[@name='Full1_C']")
    print(field_value)
    for field_name in child_nodes_list:
        field_value = xml_root.find(f".//field name='{field_name}'").find('value').find('text').text
        field_values[field_name] = field_value
        temp_list.append(field_value)
    field_values_list.append(field_values)
    data_list.append(temp_list)
    print(field_values_list)
    print(data_list)
    return data_list


def insert_datatable_with_table(config_dict, db_config, sql_table_name, column_names_list, df_row):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # print(result_dict)
    cin_column_name = config_dict['cin_column_name_in_db']
    cin = result_dict[cin_column_name]
    print(f'{cin=}')
    din_column_name = config_dict['din_column_name_in_db']
    din = result_dict[din_column_name]
    print(f'{din=}')

    designation_after_event_column_name = config_dict['designation_after_event_column_name_in_db']
    designation_after_event = result_dict[designation_after_event_column_name]
    print(f'{designation_after_event=}')

    if cin is None or din is None or designation_after_event is None:
        raise Exception(f" One of the Value of CIN, DIN and 'Designation after event' values are empty for director"
                        f" in table {sql_table_name} "
                        f"with below data \n {list(df_row)} ")
    else:
        select_query = (f"SELECT * FROM {sql_table_name} WHERE {cin_column_name} = '{cin}' AND {din_column_name}"
                        f" = '{din}' AND {designation_after_event_column_name} = '{designation_after_event}'")

    print(select_query)
    db_cursor.execute(select_query)
    result = db_cursor.fetchall()
    print(len(result))
    if len(result) == 0:  # If no matching record found
        # Insert the record
        insert_query = f"""
        INSERT INTO {sql_table_name}
        SET {', '.join([f'{col} = %s' for col in column_names_list])};
        """
        print(insert_query)
        print(tuple(df_row.values))
        db_cursor.execute(insert_query, tuple(df_row.values))
        # print(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
    else:
        result_dict.pop(cin_column_name)
        result_dict.pop(din_column_name)
        result_dict.pop(designation_after_event_column_name)

        column_names_list = list(column_names_list)
        column_names_list.remove(cin_column_name)
        column_names_list.remove(din_column_name)
        column_names_list.remove(designation_after_event_column_name)

        update_query = f'''UPDATE {sql_table_name}
                        SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])} 
                        WHERE {cin_column_name} = '{cin}' AND {din_column_name} = '{din}' AND
                        {designation_after_event_column_name} = '{designation_after_event}' '''

        print(update_query)
        db_cursor.execute(update_query)
        print(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")

    db_cursor.close()
    db_connection.close()


def insert_datatable_with_other_dir_table(config_dict, db_config, sql_table_name, column_names_list, df_row):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # print(result_dict)
    cin_column_name = config_dict['cin_column_name_in_db']
    cin = result_dict[cin_column_name]
    print(f'{cin=}')
    pan_column_name = config_dict['pan_column_name_in_db']
    pan = result_dict[pan_column_name]
    print(f'{pan=}')

    event_date_column_name = config_dict['event_date_column_name']
    event_date = result_dict[event_date_column_name]
    print(f'{event_date=}')

    if cin is None or pan is None or event_date is None:
        raise Exception(f"One of the Value of CIN, PAN and 'Event Date' values are empty for record"
                        f"to update in table {sql_table_name} "
                        f"with below data \n {list(df_row)} ")
    else:
        select_query = (f"SELECT * FROM {sql_table_name} WHERE {cin_column_name} = '{cin}' AND {pan_column_name}"
                        f" = '{pan}' AND {event_date_column_name} = '{event_date}'")

    print(select_query)
    db_cursor.execute(select_query)
    result = db_cursor.fetchall()
    print(len(result))
    if len(result) == 0:  # If no matching record found
        # Insert the record
        insert_query = f"""
        INSERT INTO {sql_table_name}
        SET {', '.join([f'{col} = %s' for col in column_names_list])};
        """
        print(insert_query)
        print(tuple(df_row.values))
        db_cursor.execute(insert_query, tuple(df_row.values))
        # print(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
    else:
        result_dict.pop(cin_column_name)
        result_dict.pop(pan_column_name)
        result_dict.pop(event_date_column_name)

        column_names_list = list(column_names_list)
        column_names_list.remove(cin_column_name)
        column_names_list.remove(pan_column_name)
        column_names_list.remove(event_date_column_name)

        update_query = f'''UPDATE {sql_table_name}
                        SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])} 
                        WHERE {cin_column_name} = '{cin}' AND {pan_column_name} = '{pan}' AND
                        {event_date_column_name} = '{event_date}' '''

        print(update_query)
        db_cursor.execute(update_query)
        print(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")

    db_cursor.close()
    db_connection.close()


def update_attachment_table(db_config, config_dict, sql_table_name, column_names_list, df_row):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # print(result_dict)

    din_column_name = config_dict['din_column_name_in_db']
    din = result_dict[din_column_name]

    pan_column_name = config_dict['pan_column_name_in_db']
    pan = result_dict[pan_column_name]

    cin_column_name = config_dict['cin_column_name_in_db']
    cin = result_dict[cin_column_name]

    if din is None or cin is None:
        raise f"din '{din}' or cin '{cin}' value extracted from xml file is None"
    else:
        query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}' ".format(sql_table_name, din_column_name, din,
                                                                         cin_column_name, cin)
    print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        print(err)
    result = db_cursor.fetchall()
    print(len(result))
    if sql_table_name == 'authorized_signatories':
        phone_number_column_name = config_dict['phone_number_column_name_in_db']
        phone_number = result_dict[phone_number_column_name]

        email_column_name = config_dict['email_column_name_in_db']
        email = result_dict[email_column_name]
        if len(result) > 0:
            if pan is not None:
                try:
                    update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' ".format(sql_table_name,
                                                                                                   pan_column_name,
                                                                                                   pan, din_column_name,
                                                                                                   din, cin_column_name,
                                                                                                   cin
                                                                                                   )

                    print(update_query)
                    db_cursor.execute(update_query)
                except Exception as e:
                    print(e)
            else:
                print(f"Pan number is none for din {din} with {cin} to update in table {sql_table_name}")
            if phone_number is not None:
                try:
                    update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(
                        sql_table_name,
                        phone_number_column_name,
                        phone_number,
                        din_column_name,
                        din, cin_column_name,
                        cin)
                    print(update_query)
                    db_cursor.execute(update_query)
                except Exception as e:
                    print(e)
            else:
                print(f"Phone number is none for din {din} with {cin} to update in table {sql_table_name}")
            if email is not None:
                try:
                    update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(sql_table_name,
                                                                                                  email_column_name,
                                                                                                  email,
                                                                                                  din_column_name,
                                                                                                  din, cin_column_name,
                                                                                                  cin)
                    print(update_query)
                    db_cursor.execute(update_query)
                except Exception as e:
                    print(e)
            else:
                print(f"Email is none for din {din} with {cin} to update in table {sql_table_name}")
            print(f"Updated entry in table {sql_table_name}")
        else:
            print(f"Entry for din '{din}' with cin '{cin}' not exists in table {sql_table_name}, hence not updating")
    if sql_table_name == 'director_network':
        if len(result) > 0:
            if pan is not None:
                try:
                    update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(sql_table_name,
                                                                                                  pan_column_name,
                                                                                                  pan,
                                                                                                  din_column_name, din,
                                                                                                  cin_column_name,
                                                                                                  cin
                                                                                                  )

                    print(update_query)
                    db_cursor.execute(update_query)
                    print(f"Updated entry in table {sql_table_name}")
                except Exception as e:
                    print(e)
            else:
                print(f"pan value is none for din {din} to update in table {sql_table_name}")
        else:
            print(f"Entry for din '{din}' with cin '{cin}' not exists in table {sql_table_name}, hence not updating")
    db_cursor.close()
    db_connection.close()


def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, hidden_xml_file_path,
              output_file_path, cin_column_value, filing_date):
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
        # print(df_map)
    except Exception as e:
        raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))

    # adding an empty column to mapping df
    df_map['Value'] = None
    single_df = df_map[df_map[df_map.columns[single_group_type_index]] == config_dict['single_type_indicator']]
    group_df = df_map[df_map[df_map.columns[single_group_type_index]] == config_dict['group_type_indicator']]

    if not os.path.exists(xml_file_path):
        raise FileNotFoundError(f"The XML file '{xml_file_path}' is not found.")

    try:
        xml_tree = Et.parse(xml_file_path)
        xml_root = xml_tree.getroot()
        # xml_str = Et.tostring(xml_root, encoding='unicode')
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))

    try:
        hidden_xml_tree = Et.parse(hidden_xml_file_path)
        hidden_xml_root = hidden_xml_tree.getroot()
        # xml_str = Et.tostring(xml_root, encoding='unicode')
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))

    cin_column_name_in_db = config_dict['cin_column_name_in_db']

    # initializing empty list to save all the result dataframes for saving to output excel
    output_dataframes_list = []

    event_abbreviation_list = [x.strip() for x in config_dict['event_abbreviation_list'].split(',')]
    event_list = [x.strip() for x in config_dict['event_list'].split(',')]
    event_dict = dict(zip(event_abbreviation_list, event_list))
    print(event_dict)
    designation_abbreviation_list = [x.strip() for x in config_dict['designation_abbreviation_list'].split(',')]
    designation_list = [x.strip() for x in config_dict['designation_list'].split(',')]
    designation_dict = dict(zip(designation_abbreviation_list, designation_list))
    print(designation_dict)

    for index, row in single_df.iterrows():
        # field_name = str(row.iloc[field_name_index]).strip()
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        # sql_table_name = str(row.iloc[table_name_index]).strip()
        # column_name = str(row.iloc[column_name_index]).strip()
        # column_json_node = str(row.iloc[column_json_node_index]).strip()
        xml_type = str(row.iloc[xml_type_index]).strip()
        if xml_type == 'Hidden':
            continue
        value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        single_df.at[index, 'Value'] = value
    # print(single_df)
    no_of_directors_row_index = single_df[single_df[single_df.columns[field_name_index]] ==
                                          config_dict['no_of_directors_field_name']].index[0]
    if no_of_directors_row_index is not None:
        no_of_directors_value = single_df.loc[no_of_directors_row_index, 'Value']
        print(f'{no_of_directors_value=}')
        if int(no_of_directors_value) > 0:
            pass
        else:
            raise Exception(f"Number of Directors = '{no_of_directors_value}' found in xml is not greater than zero."
                            f"Hence skipping processing directors program")
    else:
        raise Exception("Number of Directors field is not found in director mapping file.")

    # extract group values
    din_list = []
    for index, row in group_df.iterrows():
        xml_type = str(row.iloc[xml_type_index])
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[table_name_index]).strip()
        column_names = str(row.iloc[column_name_index]).strip()
        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]

        table_node_name = parent_node
        # print(table_node_name)
        if xml_type == config_dict['direct_xml_type_indicator']:
            try:
                print(table_node_name)
                print(child_nodes)
                table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
                table_df = pd.DataFrame(table_in_list)
                table_df.columns = column_names_list
                print(table_df)
            except Exception as e:
                print(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
                continue

        elif xml_type == config_dict['hidden_xml_type_indicator']:
            continue
            # try:
            #     print(table_node_name)
            #     print(child_nodes)
            #     table_in_list = extract_table_values_from_hidden_xml(hidden_xml_root, table_node_name, child_nodes)
            #     table_df_hidden = pd.DataFrame(table_in_list)
            #     table_df_hidden.columns = column_names_list
            #     print(table_df_hidden)
            # except Exception as e:
            #     print(f'Exception {e} occurred while extracting data from xml for table {table_node_name} at line'
            #           f' number ')
            #     traceback.print_exc()
            #     continue
        else:
            continue

        table_df[config_dict['filing_date_column_name']] = filing_date
        # print(table_df)

        table_df[cin_column_name_in_db] = cin_column_value

        column_names_list.append(cin_column_name_in_db)
        print(column_names_list)
        table_df.columns = column_names_list
        table_df = table_df[table_df[column_names_list[0]].notna()]
        # print(table_df)

        # current year
        # current_year = datetime.now().year
        # table_df['date_of_birth'] = pd.to_datetime(table_df['date_of_birth'])
        # table_df['Birth_Year'] = table_df['Birth_Date'].dt.year
        # table_df['age'] = current_year - table_df['Birth_Year']

        for i, df_row in table_df.iterrows():
            event_value = table_df.loc[i, config_dict['event_column_name']]
            table_df.loc[i, config_dict['event_column_name']] = event_dict.get(event_value, event_value)

            designation_value = table_df.loc[i, config_dict['designation_column_name']]
            table_df.loc[i, config_dict['designation_column_name']] = designation_dict.get(designation_value,
                                                                                           designation_value)
        # print(table_df)
        table_df.loc[table_df[config_dict['event_column_name']] ==
                     config_dict['appointment_keyword'], config_dict['date_of_appointment_column_name']] = \
            table_df[config_dict['event_date_column_name']]

        table_df.loc[table_df[config_dict['event_column_name']] ==
                     config_dict['resignation_keyword'], config_dict['date_of_cessation_column_name']] = \
            table_df[config_dict['event_date_column_name']]
        print(table_df)

        for _, df_row in table_df.iterrows():
            try:
                combined = list(zip(column_names_list, df_row))
                result_dict = dict(combined)
                din_column_name = config_dict['din_column_name_in_db']
                din = result_dict[din_column_name]
                print(f'{din=}')
                din_list.append(din)
                insert_datatable_with_table(config_dict, db_config, sql_table_name, table_df.columns, df_row)
            except Exception as e:
                print(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                      df_row)
        print(f"DB execution is complete for {sql_table_name}")
        print(din_list)
        output_dataframes_list.append(table_df)

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name=config_dict['output_sheet_name'], index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()
    return din_list

def other_than_dir_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                             output_file_path, cin_column_value, filing_date):
    field_name_index = config_dict['field_name_index']
    # xml_type_index = config_dict['xml_type_index']
    single_group_type_index = config_dict['single_group_type_index']
    parent_node_index = config_dict['parent_node_index']
    child_nodes_index = config_dict['child_nodes_index']
    table_name_index = config_dict['sql_table_name_index']
    column_name_index = config_dict['column_name_index']
    # column_json_node_index = config_dict['column_json_node_index']

    config_dict_keys = [
    ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]
    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

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
    group_df = df_map[df_map[df_map.columns[single_group_type_index]] == config_dict['group_type_indicator']]

    if not os.path.exists(xml_file_path):
        raise FileNotFoundError(f"The XML file '{xml_file_path}' is not found.")

    try:
        xml_tree = Et.parse(xml_file_path)
        xml_root = xml_tree.getroot()
        # xml_str = Et.tostring(xml_root, encoding='unicode')
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))

    cin_column_name_in_db = config_dict['cin_column_name_in_db']

    # initializing empty list to save all the result dataframes for saving to output excel
    output_dataframes_list = []

    event_abbreviation_list = [x.strip() for x in config_dict['event_abbreviation_list'].split(',')]
    event_list = [x.strip() for x in config_dict['event_list'].split(',')]
    event_dict = dict(zip(event_abbreviation_list, event_list))
    print(event_dict)

    for index, row in single_df.iterrows():
        # field_name = str(row.iloc[field_name_index]).strip()
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        # sql_table_name = str(row.iloc[table_name_index]).strip()
        # column_name = str(row.iloc[column_name_index]).strip()
        # column_json_node = str(row.iloc[column_json_node_index]).strip()

        value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        single_df.at[index, 'Value'] = value
    print(single_df)
    no_of_directors_row_index = single_df[single_df[single_df.columns[field_name_index]] ==
                                          config_dict['no_of_directors_field_name']].index[0]
    if no_of_directors_row_index is not None:
        no_of_directors_value = single_df.loc[no_of_directors_row_index, 'Value']
        print(f'{no_of_directors_value=}')
        if no_of_directors_value is None or int(no_of_directors_value) <= 0:
            pass
        else:
            raise Exception(f"Number of Directors = '{no_of_directors_value}' is found in xml."
                            f"Hence skipping processing program for 'other than directors'")
    else:
        raise Exception("Number of Directors field is not found in director mapping file for 'other than"
                        " directors program'")

    # extract group values
    for index, row in group_df.iterrows():
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[table_name_index]).strip()
        column_names = str(row.iloc[column_name_index]).strip()
        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]

        table_node_name = parent_node
        # print(table_node_name)
        try:
            print(table_node_name)
            print(child_nodes)
            table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
            table_df = pd.DataFrame(table_in_list)
            table_df.columns = column_names_list
            # print(table_df)
        except Exception as e:
            print(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
            continue
        else:
            pass
        table_df[config_dict['filing_date_column_name']] = filing_date
        table_df[cin_column_name_in_db] = cin_column_value

        column_names_list.append(cin_column_name_in_db)
        table_df.columns = column_names_list
        table_df = table_df[table_df[column_names_list[0]].notna()]
        # print(table_df)

        # current year
        current_year = datetime.now().year
        date_of_birth_column_name = config_dict['date_of_birth_column_name']
        age_column_name = config_dict['age_column_name']
        table_df[date_of_birth_column_name] = pd.to_datetime(table_df[date_of_birth_column_name])
        table_df['Birth_Year'] = table_df[date_of_birth_column_name].dt.year
        table_df[age_column_name] = current_year - table_df['Birth_Year']

        for i, df_row in table_df.iterrows():
            event_value = table_df.loc[i, config_dict['event_column_name']]
            table_df.loc[i, config_dict['event_column_name']] = event_dict.get(event_value, event_value)

        table_df.loc[table_df[config_dict['event_column_name']] ==
                     config_dict['appointment_keyword'], config_dict['date_of_appointment_column_name']] = \
            table_df[config_dict['event_date_column_name']]

        table_df.loc[table_df[config_dict['event_column_name']] ==
                     config_dict['resignation_keyword'], config_dict['date_of_cessation_column_name']] = \
            table_df[config_dict['event_date_column_name']]

        table_df[config_dict['name_column_name']] = \
            table_df[config_dict['name_first_name_node_name']].fillna('')+ ' ' + \
            table_df[config_dict['name_second_name_node_name']].fillna('') + ' ' + \
            table_df[config_dict['name_last_name_node_name']].fillna('')

        table_df[config_dict['father_name_column_name']] = \
            table_df[config_dict['father_name_first_name_node_name']].fillna('') + ' ' + \
            table_df[config_dict['father_name_second_name_node_name']].fillna('') + ' ' + \
            table_df[config_dict['father_name_last_name_node_name']].fillna('')

        columns_to_remove = [config_dict['name_first_name_node_name'],
                             config_dict['name_second_name_node_name'],
                             config_dict['name_last_name_node_name'],
                             config_dict['father_name_first_name_node_name'],
                             config_dict['father_name_second_name_node_name'],
                             config_dict['father_name_last_name_node_name'],
                             'Birth_Year'
                             ]
        table_df = table_df.drop(columns=columns_to_remove)
        table_df[date_of_birth_column_name] = table_df[date_of_birth_column_name].astype(str)
        print("Table Before updating in database")
        print(table_df)

        for _, df_row in table_df.iterrows():
            try:
                insert_datatable_with_other_dir_table(config_dict, db_config, sql_table_name, table_df.columns, df_row)
            except Exception as e:
                print(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
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


def attachment_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                         output_file_path, cin):
    # field_name_index = 0
    single_group_type_index = 2
    parent_node_index = 3
    child_nodes_index = 4
    table_name_index = 5
    column_name_index = 6
    # column_json_node_index = 7

    cin_column_name_in_db = config_dict['cin_column_name_in_db']

    config_dict_keys = [
    ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]
    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

    if not os.path.exists(map_file_path):
        raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

    try:
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        # print(df_map)
    except Exception as e:
        raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))

    # adding an empty column to mapping df
    df_map['Value'] = None

    group_df = df_map[df_map[df_map.columns[single_group_type_index]] == config_dict['group_type_indicator']]

    if not os.path.exists(xml_file_path):
        raise FileNotFoundError(f"The XML file '{xml_file_path}' is not found.")

    try:
        xml_tree = Et.parse(xml_file_path)
        xml_root = xml_tree.getroot()
        # xml_str = Et.tostring(xml_root, encoding='unicode')
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))

    # initializing empty list to save all the result dataframes for saving to output excel
    output_dataframes_list = []

    # extract group values
    for index, row in group_df.iterrows():
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[table_name_index]).strip()
        column_names = str(row.iloc[column_name_index]).strip()
        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]

        table_node_name = parent_node
        # print(table_node_name)

        try:
            print(table_node_name)
            print(child_nodes)
            table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
            table_df = pd.DataFrame(table_in_list)
            table_df[cin_column_name_in_db] = cin
            print(table_df)
            column_names_list.append(cin_column_name_in_db)
        except Exception as e:
            print(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
            continue

        table_df.columns = column_names_list
        table_df = table_df[table_df[column_names_list[0]].notna()]
        print(table_df)

        # current year
        # current_year = datetime.now().year
        # table_df['date_of_birth'] = pd.to_datetime(table_df['date_of_birth'])
        # table_df['Birth_Year'] = table_df['Birth_Date'].dt.year
        # table_df['age'] = current_year - table_df['Birth_Year']

        for _, df_row in table_df.iterrows():
            print(df_row)
            try:
                update_attachment_table(db_config, config_dict, sql_table_name, table_df.columns, df_row)
            except Exception as e:
                print(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
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


def dir_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, hidden_xml_file_path,
                  output_file_path, cin_column_value, filing_date):
    try:
        din_list = xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, hidden_xml_file_path,
                  output_file_path, cin_column_value, filing_date)
        print(din_list)
        dir_hidden_fields(db_config,hidden_xml_file_path,map_file_path,config_dict,din_list,cin_column_value)
    except Exception as e:
        print("Below Exception occurred while processing DIR file: \n ", e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # Print the traceback details
        for line in traceback_details:
            print(line.strip())

        return False
    else:
        return True


def other_than_director_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                                  output_file_path, cin_column_value, filing_date):
    try:
        other_than_dir_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                                 output_file_path, cin_column_value, filing_date)
    except Exception as e:
        print("Below Exception occurred while processing DIR file: \n ", e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # Print the traceback details
        for line in traceback_details:
            print(line.strip())

        return False
    else:
        return True


def dir_attachment_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                             output_file_path, cin):
    try:
        attachment_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                             output_file_path, cin)
    except Exception as e:
        print("Below Exception occurred while processing DIR file: \n ", e)
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
# excel_file_path = r"C:\MCA Portal\Config.xlsx"
# sheet_name = 'DIR'
# config_dict,config_status = create_main_config_dictionary(excel_file_path,sheet_name)
# map_file_path = r"C:\MCA Portal\DIR-12_nodes_config.xlsx"
# map_sheet_name = 'Sheet1'
# xml_file_path = r"C:\Users\BRADSOL123\Downloads\8_Form DIR-12-multiplt directors_06092021_signed_06-09-2021.xml"
# hidden_xml_file_path = r"C:\Users\BRADSOL123\Downloads\8_Form DIR-12-multiplt directors_06092021_signed_06-09-2021_hidden.xml"
# output_file_path = str(xml_file_path).replace('.xml','.xlsx')
# cin = 'L25111MH1988PLC048928'
# filing_date = '31/10/2023'
# dir_xml_to_db(db_config,config_dict,map_file_path,map_sheet_name,xml_file_path,hidden_xml_file_path,output_file_path,cin,filing_date)