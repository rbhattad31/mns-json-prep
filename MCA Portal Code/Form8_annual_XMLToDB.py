import sys
import traceback
from datetime import datetime

import pandas as pd
import xml.etree.ElementTree as Et
import os
import json
import mysql.connector
import re

from dateutil.relativedelta import relativedelta


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


def update_database_single_value(db_config, table_name, cin_column_name, cin_value,
                                 column_name, column_value, year_column_name, year):
    if column_name == year_column_name:
        return
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value.replace('"', '\\"').replace("'", "\\'")
        # print(column_value)
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}'".format(table_name, cin_column_name, cin_value,
                                                                    year_column_name, year)
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
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(table_name,
                                                                                      column_name, column_value,
                                                                                      cin_column_name, cin_value,
                                                                                      year_column_name, year
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
        print(f"cin {cin_value} and year {year} is not exist in table {table_name}")
        print(type(year))
        print(type(cin_value))
        print(type(column_value))
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, cin_column_name,
                                                                                      year_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      year,
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


def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path,
              cin_column_value):
    config_dict_keys = ['single_type_indicator', 'cin_column_name_in_db',
                        'Type_of_file_field_name', 'annual_keyword_in_xml',
                        'field_name_index', 'type_index',
                        'year_index', 'parent_node_index',
                        'child_nodes_index', 'sql_table_name_index',
                        'column_name_index', 'column_json_node_index',
                        'Previous_year_keyword', 'Current_year_keyword',
                        'Common_Keyword', 'Formula_Keyword',
                        'year_column_name', 'date_column_name',
                        'auditor_type_field_name', 'auditor_type_column_name',
                        'financials_auditor_column_name',
                        'auditor_type_value_to_check',
                        'auditor_name_field_name', 'auditor_name_column_name',
                        'membership_number_field_name', 'membership_number_column_name',
                        'line_1_field_name', 'line_2_field_name',
                        'city_field_name', 'district_field_name',
                        'state_field_name', 'pincode_field_name',
                        'address_column_name'
                        ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]
    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")
    cin_column_name = config_dict['cin_column_name_in_db']

    years = []

    # index
    field_name_index = int(config_dict['field_name_index'])
    type_index = int(config_dict['type_index'])
    year_index = int(config_dict['year_index'])
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

    # creating new filtered dataframes for single and group values
    single_df = df_map[df_map[df_map.columns[type_index]] == config_dict['single_type_indicator']]

    if not os.path.exists(xml_file_path):
        raise FileNotFoundError(f"The XML file '{xml_file_path}' is not found.")

    try:
        xml_tree = Et.parse(xml_file_path)
        xml_root = xml_tree.getroot()
        # xml_str = Et.tostring(xml_root, encoding='unicode')
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))

    # initializing list to save single values data
    results_previous_year = []
    results_current_year = []
    results_common = []
    # print(single_df)
    # extract single values
    previous_year_df = single_df[single_df[single_df.columns[year_index]] == config_dict['Previous_year_keyword']]
    current_year_df = single_df[single_df[single_df.columns[year_index]] == config_dict['Current_year_keyword']]
    common_df = single_df[single_df[single_df.columns[year_index]] == config_dict['Common_Keyword']]
    single_df_list = []

    print("Processing common data")
    for index, row in common_df.iterrows():
        field_name = str(row.iloc[field_name_index]).strip()
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[sql_table_name_index]).strip()
        column_name = str(row.iloc[column_name_index]).strip()
        column_json_node = str(row.iloc[column_json_node_index]).strip()

        value_common = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        common_df.at[index, 'Value'] = value_common
        results_common.append([field_name, value_common, sql_table_name, column_name, column_json_node])
    print(common_df)
    # find type of file and check condition
    type_of_file_row_index = common_df[common_df[common_df.columns[field_name_index]] ==
                                       config_dict['Type_of_file_field_name']].index[0]
    if type_of_file_row_index is not None:
        type_of_file_value = common_df.loc[type_of_file_row_index, 'Value']
        print(f'{type_of_file_value=}')
        if type_of_file_value != config_dict['annual_keyword_in_xml']:
            print("Type of file is not related to Form 8 annual . So stopping program execution")
            raise Exception("Type of file is not related to Form 8 annual . So stopping program execution")
    else:
        raise Exception("Type of file field is not found in Form 8 annual mapping file. So stopping program execution")

    print("Processing previous year data")
    for index, row in previous_year_df.iterrows():
        field_name = str(row.iloc[field_name_index]).strip()
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[sql_table_name_index]).strip()
        column_name = str(row.iloc[column_name_index]).strip()
        column_json_node = str(row.iloc[column_json_node_index]).strip()
        if parent_node == config_dict['Formula_Keyword']:
            continue
        else:
            value_previous_year = get_single_value_from_xml(xml_root, parent_node, child_nodes)
            if column_name == config_dict['year_column_name'] or column_name == config_dict['date_column_name']:
                try:
                    print(value_previous_year)
                    datetime_object = datetime.fromisoformat(value_previous_year)
                except Exception as e:
                    try:
                        datetime_object = datetime.strptime(value_previous_year, "%Y-%m-%d")
                    except Exception as e:
                        print(f'{value_previous_year=}')
                        raise Exception(f"'{e}' occurred while extracting date from xml")
                value_previous_year = datetime_object.strftime("%Y-%m-%d")
                if column_name == config_dict['year_column_name']:
                    value_previous_year = datetime.strptime(value_previous_year, "%Y-%m-%d")
                    value_previous_year = value_previous_year - relativedelta(years=1)
                    value_previous_year = value_previous_year.strftime("%Y-%m-%d")
                print(value_previous_year)
        # print(value)
        try:
            value_previous_year = float(value_previous_year)
        except Exception as e:
            print(f"Exception occured in converting to float {e}")
            pass
        previous_year_df.at[index, 'Value'] = value_previous_year
        results_previous_year.append([field_name, value_previous_year, sql_table_name, column_name, column_json_node])
    # print("previous year df:\n", previous_year_df)

    previous_year_formula_df = previous_year_df[
        previous_year_df[previous_year_df.columns[parent_node_index]] == config_dict['Formula_Keyword']]
    print("Processing previous year formulas")
    # print(previous_year_formula_df)
    for _, row in previous_year_formula_df.iterrows():
        previous_formula = row['Child_Nodes']
        previous_formula_field_name = row['Field_Name']
        for previous_field_name in previous_year_df['Field_Name']:
            previous_pattern = r'\b' + re.escape(previous_field_name) + r'\b'
            # current_formula = current_formula.replace(field_name, str(current_year_df[current_year_df['Field_Name']
            # == field_name]['Value'].values[0]))
            # print(f'{previous_pattern=}')
            previous_formula = re.sub(previous_pattern, str(
                previous_year_df[previous_year_df['Field_Name'] == previous_field_name]['Value'].values[0]),
                                      previous_formula)
            # print(f'{previous_formula=}')
        # print(previous_formula_field_name + ":" + previous_formula)
        try:
            # print(f'{previous_formula=}')
            if 'None' in previous_formula:
                previous_formula = previous_formula.replace('None', '0')
                # print(f'{previous_formula=}')
            # Calculate the value using the provided formula and insert it
            previous_year_df.at[previous_year_df[previous_year_df['Field_Name'] == previous_formula_field_name].index[
                0], 'Value'] = eval(previous_formula)
        except (NameError, SyntaxError):
            # Handle the case where the formula is invalid or contains a missing field name
            print(f"Exception occurred while processing previous year formulas data - \n"
                  f" Invalid formula for {previous_formula_field_name}: {previous_formula}")
    print("Completed processing previous year data")
    print("Processing present year data")
    for index, row in current_year_df.iterrows():
        field_name = str(row.iloc[field_name_index]).strip()
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[sql_table_name_index]).strip()
        column_name = str(row.iloc[column_name_index]).strip()
        column_json_node = str(row.iloc[column_json_node_index]).strip()
        if parent_node == config_dict['Formula_Keyword']:
            continue
        else:
            value_current_year = get_single_value_from_xml(xml_root, parent_node, child_nodes)
            if column_name == config_dict['year_column_name'] or column_name == config_dict['date_column_name']:
                try:
                    print(value_current_year)
                    datetime_object = datetime.fromisoformat(value_current_year)
                except Exception as e:
                    try:
                        datetime_object = datetime.strptime(value_current_year, "%Y-%m-%d")
                    except Exception as e:
                        print(value_current_year)
                        raise Exception(f"'{e}' occurred while extracting date from xml")
                value_current_year = datetime_object.strftime("%Y-%m-%d")
                print(value_current_year)
        # print(child_nodes)
        # print(value_current_year)
        # print(value)
        try:
            value_current_year = float(value_current_year)
        except Exception as e:
            print(f"Exception occured in converting to float {e}")
            pass
        current_year_df.at[index, 'Value'] = value_current_year
        results_current_year.append([field_name, value_current_year, sql_table_name, column_name, column_json_node])

    current_year_formula_df = current_year_df[
        current_year_df[current_year_df.columns[parent_node_index]] == config_dict['Formula_Keyword']]
    print("processing present year formulae data ")
    # print(current_year_formula_df)
    for _, row in current_year_formula_df.iterrows():
        current_formula = row['Child_Nodes']
        current_formula_field_name = row['Field_Name']
        for field_name in current_year_df['Field_Name']:
            pattern = r'\b' + re.escape(field_name) + r'\b'
            # print(f'{pattern=}')
            # current_formula = current_formula.replace(field_name, str(current_year_df[current_year_df['Field_Name']
            # == field_name]['Value'].values[0]))
            current_formula = re.sub(pattern, str(
                current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]), current_formula)
            # print(f'{current_formula=}')
        # print(current_formula_field_name + ":" + current_formula)
        try:
            # print(f'{current_formula=}')
            if 'None' in current_formula:
                current_formula = current_formula.replace('None', '0')
                # print(f'{current_formula=}')
            # Calculate the value using the provided formula and insert it
            current_year_df.at[
                current_year_df[current_year_df['Field_Name'] == current_formula_field_name].index[0], 'Value'] = eval(
                current_formula)
        except (NameError, SyntaxError):
            # Handle the case where the formula is invalid or contains a missing field name
            print(f"Invalid formula for {current_formula_field_name}: {current_formula}")
    print("Completed processing present year data")
    # print(current_year_df)

    current_year = current_year_df[current_year_df['Field_Name'] == config_dict['year_column_name']]['Value'].values[0]
    if current_year is None:
        raise Exception(f"Exception occurred while extracting year value {current_year} from current year data")
    years.append(current_year)
    previous_year = previous_year_df[previous_year_df['Field_Name'] == config_dict['year_column_name']]['Value'].values[
        0]
    if previous_year is None:
        raise Exception(f"Exception occurred while extracting year value {previous_year} from previous year data")
    years.append(previous_year)
    # print(years)
    print("Saving Single Values to database")

    single_df_list.append(current_year_df)
    single_df_list.append(previous_year_df)
    current_year_output_df = pd.DataFrame(current_year_df, columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name',
                                                                    'Column_JSON_Node'])
    previous_year_output_df = pd.DataFrame(previous_year_df,
                                           columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name',
                                                    'Column_JSON_Node'])
    common_output_df = pd.DataFrame(common_df,
                                    columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name', 'Column_JSON_Node'])
    output_dataframes_list.append(current_year_output_df)
    output_dataframes_list.append(previous_year_output_df)
    output_dataframes_list.append(common_output_df)

    for df in single_df_list:
        # print(df)
        sql_tables_list = df[df.columns[sql_table_name_index]].unique()
        # print(sql_tables_list)
        year_value = df[df[df.columns[column_name_index]] == config_dict['year_column_name']]['Value'].values[0]
        # print(year_value)
        for table_name in sql_tables_list:
            table_df = df[df[df.columns[sql_table_name_index]] == table_name]
            columns_list = table_df[table_df.columns[column_name_index]].unique()
            # print(columns_list)
            for column_name in columns_list:
                # print(column_name)
                # filter table df with only column value
                column_df = table_df[table_df[table_df.columns[column_name_index]] == column_name]
                # print(column_df)
                # create json dict with keys of field name and values for the same column name entries
                json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                # Convert the dictionary to a JSON string
                json_string = json.dumps(json_dict)
                # print(json_string)
                try:
                    update_database_single_value(db_config, table_name, cin_column_name, cin_column_value, column_name,
                                                 json_string, config_dict['year_column_name'], year_value)
                except Exception as e:
                    print(f"{e} occurred while updating data in dataframe for {table_name} "
                          f"with data {json_string}")
    common_sql_tables_list = common_df[common_df.columns[sql_table_name_index]].unique()
    # print(common_sql_tables_list)

    for common_table_name in common_sql_tables_list:
        common_table_df = common_df[common_df[common_df.columns[sql_table_name_index]] == common_table_name]
        common_columns_list = common_table_df[common_table_df.columns[column_name_index]].unique()
        # print(common_columns_list)
        for common_column_name in common_columns_list:

            # print(common_column_name)
            # filter table df with only column value
            common_column_df = common_table_df[common_table_df[common_table_df.columns[column_name_index]]
                                               == common_column_name]
            if common_column_name == config_dict['financials_auditor_column_name']:
                print(common_column_df)
                print(config_dict['auditor_type_column_name'])
                try:
                    auditor_type_row_index = (
                        common_column_df[common_column_df[common_column_df.columns[column_json_node_index]] ==
                                         config_dict['auditor_type_column_name']].index)[0]
                except Exception as e:
                    raise (f"Below Exception occurred while getting auditor type row details of table - "
                           f"{common_table_name} "
                           f"\n {e}")
                print(f'{auditor_type_row_index=}')
                if auditor_type_row_index is not None:
                    auditor_type_value = common_column_df.loc[auditor_type_row_index, 'Value']
                    print(f'{auditor_type_value=}')
                    auditor_type_value_to_check = config_dict['auditor_type_value_to_check']
                    if auditor_type_value != auditor_type_value_to_check:
                        print(f"Auditor type is not equal to {auditor_type_value_to_check}, hence skipping updating"
                              f" auditor details into datatable {common_table_name}")
                        continue
                    if auditor_type_value == 'AUDR':
                        auditor_type_value = 'auditor'
                    elif auditor_type_value == 'D':
                        auditor_type_value = 'designated partner'
                    elif auditor_type_value == 'A':
                        auditor_type_value = 'authorized representative'
                    common_df.loc[auditor_type_row_index, 'Value'] = auditor_type_value
            # print(common_column_df)
            # create json dict with keys of field name and values for the same column name entries
            common_json_dict = common_column_df.set_index(common_table_df.columns[0])['Value'].to_dict()
            # print(common_json_dict)

            common_json_dict[config_dict['address_column_name']] = str(common_json_dict.pop(config_dict["line_1_field_name"]))+","+str(common_json_dict.pop(config_dict["line_2_field_name"]))+","+str(common_json_dict.pop(config_dict["city_field_name"]))+","+str(common_json_dict.pop(config_dict["district_field_name"]))+","+str(common_json_dict.pop(config_dict["state_field_name"]))+","+str(common_json_dict.pop(config_dict["pincode_field_name"]))
            # print(common_json_dict)
            #common_json_dict.pop(config_dict["auditor_type_field_name"])
            # print(common_json_dict)
            # Convert the dictionary to a JSON string
            common_json_string = json.dumps(common_json_dict)
            # print(common_json_string)

            for year in years:
                if year is None or year == '':
                    continue
                try:
                    update_database_single_value(db_config, common_table_name, cin_column_name, cin_column_value,
                                                 common_column_name, common_json_string,
                                                 config_dict['year_column_name'], year)
                except Exception as e:
                    print(f"{e} occurred while updating data in dataframe for {common_table_name} "
                          f"with data {common_json_string}")
    print("Saving Single Values to database is complete")
    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()


def form8_annual_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path,
                           cin_column_value):
    try:
        print("Started Executing Form 8 annual Program")
        xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path,
                  cin_column_value)
    except Exception as e:
        print(f"Exception '{e}' occurred while processing Form 8 Annual program \n ")
        # Get the current exception information
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # Print the traceback details
        for line in traceback_details:
            print(line.strip())
        return False
    else:
        print("Completed Executing Form 8 Annual Program")
        return True
