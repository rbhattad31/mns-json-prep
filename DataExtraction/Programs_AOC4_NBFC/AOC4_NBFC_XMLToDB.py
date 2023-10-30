import pandas as pd
import xml.etree.ElementTree as Et
import os
import json
import mysql.connector
import re


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


def update_database_single_value_aoc(db_config, table_name, cin_column_name, cin_value, company_name_column_name,
                                     company_name, column_name, column_value, year):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    # print(num_elements)
    if column_name == "nbfc_financials_auditor" and num_elements == 1:
        first_key = next(iter(json_dict))
        first_value_json_list = json_dict[first_key]
        json_string = json.dumps(first_value_json_list)
        column_value = json_string
    elif num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)
    # print(column_value)
    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and {}='{}'".format(table_name, cin_column_name, cin_value,
                                                                              company_name_column_name, company_name,
                                                                              'year', year)
    # print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        print(err)
    result = db_cursor.fetchall()
    # print(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = ("UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' "
                        "AND {}='{}'").format(table_name,
                                              column_name,
                                              column_value,
                                              cin_column_name,
                                              cin_value,
                                              company_name_column_name,
                                              company_name,
                                              'Year',
                                              year)
        # print(update_query)
        db_cursor.execute(update_query)
        # print("Updated")
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
        # print("Inserted")
    db_connection.commit()
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


def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path,
              cin_column_value, company_name):
    config_dict_keys = ['cin_column_name_in_db', 'company_name_column_name_in_db',
                        'single_type_indicator', 'group_type_indicator',
                        'Previous_year_keyword', 'Current_year_keyword',
                        'Financial_Parameter_Keyword', 'Common_Keyword',
                        'Formula_Keyword'
                        ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]
    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")
    cin_column_name = config_dict['cin_column_name_in_db']
    company_column_name = config_dict['company_name_column_name_in_db']

    years = []

    # index
    field_name_index = 0
    type_index = 2
    parent_node_index = 3
    child_nodes_index = 4
    year_index = 5
    sql_table_name_index = 6
    column_name_index = 7
    column_json_node_index = 8

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
    group_df = df_map[df_map[df_map.columns[type_index]] == config_dict['group_type_indicator']]

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
    results_financial_parameter = []
    results_common = []
    # print(single_df)
    # extract single values
    previous_year_df = single_df[single_df[single_df.columns[year_index]] == config_dict['Previous_year_keyword']]
    current_year_df = single_df[single_df[single_df.columns[year_index]] == config_dict['Current_year_keyword']]
    financial_parameter_df = single_df[single_df[single_df.columns[year_index]] == config_dict['Financial_Parameter_Keyword']]
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

        if parent_node == config_dict['Constant_Keyword']:
            value_common = child_nodes
        else:
            value_common = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        common_df.at[index, 'Value'] = value_common
        results_common.append([field_name, value_common, sql_table_name, column_name, column_json_node])
    print("Processing financial parameter data")
    for index, row in financial_parameter_df.iterrows():
        field_name = str(row.iloc[field_name_index]).strip()
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[sql_table_name_index]).strip()
        column_name = str(row.iloc[column_name_index]).strip()
        column_json_node = str(row.iloc[column_json_node_index]).strip()

        if parent_node == config_dict['Constant_Keyword']:
            value_financial_parameter = child_nodes
        else:
            value_financial_parameter = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        financial_parameter_df.at[index, 'Value'] = value_financial_parameter
        results_financial_parameter.append(
            [field_name, value_financial_parameter, sql_table_name, column_name, column_json_node])
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
        if parent_node == config_dict['Constant_Keyword']:
            value_previous_year = child_nodes
        else:
            value_previous_year = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        # print(value)
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
            # Calculate the value using the provided formula and insert it
            previous_year_df.at[previous_year_df[previous_year_df['Field_Name'] == previous_formula_field_name].index[
                0], 'Value'] = eval(previous_formula)
        except (NameError, SyntaxError):
            # Handle the case where the formula is invalid or contains a missing field name
            print(f"Invalid formula for {previous_formula_field_name}: {previous_formula}")
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
        if parent_node == config_dict['Constant_Keyword']:
            value_current_year = child_nodes
        else:
            value_current_year = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        # print(child_nodes)
        # print(value_current_year)
        # print(value)
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
            # Calculate the value using the provided formula and insert it
            current_year_df.at[
                current_year_df[current_year_df['Field_Name'] == current_formula_field_name].index[0], 'Value'] = eval(
                current_formula)
        except (NameError, SyntaxError):
            # Handle the case where the formula is invalid or contains a missing field name
            print(f"Invalid formula for {current_formula_field_name}: {current_formula}")
    print("Completed processing present year formulas")
    # print(current_year_df)

    current_year = current_year_df[current_year_df['Field_Name'] == 'year']['Value'].values[0]
    years.append(current_year)
    previous_year = previous_year_df[previous_year_df['Field_Name'] == 'year']['Value'].values[0]
    years.append(previous_year)
    print("Saving Single Values to database")
    single_df_list.append(current_year_df)
    single_df_list.append(previous_year_df)
    single_df_list.append(financial_parameter_df)
    current_year_output_df = pd.DataFrame(current_year_df, columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name',
                                                                    'Column_JSON_Node'])
    previous_year_output_df = pd.DataFrame(previous_year_df,
                                           columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name',
                                                    'Column_JSON_Node'])
    common_output_df = pd.DataFrame(common_df,
                                    columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name', 'Column_JSON_Node'])
    financial_output_df = pd.DataFrame(financial_parameter_df,
                                       columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name', 'Column_JSON_Node'])
    output_dataframes_list.append(current_year_output_df)
    output_dataframes_list.append(previous_year_output_df)
    output_dataframes_list.append(common_output_df)
    output_dataframes_list.append(financial_output_df)
    for df in single_df_list:
        # print(df)
        sql_tables_list = df[df.columns[sql_table_name_index]].unique()
        # print(sql_tables_list)
        year_value = df[df['Field_Name'] == 'year']['Value'].values[0]
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
                    update_database_single_value_aoc(db_config, table_name, cin_column_name, cin_column_value,
                                                     company_column_name, company_name, column_name, json_string,
                                                     year_value)
                except Exception as e:
                    print(f"Exception {e} occurred while updating data in dataframe for {table_name} "
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
            common_column_df = common_table_df[common_table_df[common_table_df.columns[column_name_index]] == common_column_name]
            # print(common_column_df)
            # create json dict with keys of field name and values for the same column name entries
            common_json_dict = common_column_df.set_index(common_table_df.columns[0])['Value'].to_dict()
            # Convert the dictionary to a JSON string
            common_json_string = json.dumps(common_json_dict)
            # print(common_json_string)
            for year in years:
                try:
                    update_database_single_value_aoc(db_config, common_table_name, cin_column_name, cin_column_value,
                                                     company_column_name, company_name, common_column_name,
                                                     common_json_string, year)
                except Exception as e:
                    print(f"Exception {e} occurred while updating data in dataframe for {common_table_name} "
                          f"with data {common_json_string}")
    print("Saving Single Values to database is complete")
    print("Saving group values to database")
    for index, row in group_df.iterrows():
        # print(row)
        field_name = str(row.iloc[field_name_index]).strip()
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[sql_table_name_index]).strip()
        column_names = str(row.iloc[column_name_index]).strip()
        column_json_node = str(row.iloc[column_json_node_index]).strip()

        table_node_name = parent_node
        # print(table_node_name)
        try:
            table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
            # print(table_in_list)
        except Exception as e:
            print(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
            continue
        table_df = pd.DataFrame(table_in_list)
        table_df.dropna(inplace=True)
        # print(table_df)
        if field_name == 'nbfc_financials_auditor':
            child_nodes_list = [x.strip() for x in child_nodes.split(',')]
            table_df.columns = child_nodes_list
            row_dicts = table_df.to_dict(orient='records')

            # Convert each dictionary to a JSON string
            auditor_json_list = []

            for row_dict in row_dicts:
                row_dict["ADDRESS"] = {
                    "ADDRESS_LINE_I": row_dict.pop("ADDRESS_LINE_I"),
                    "ADDRESS_LINE_II": row_dict.pop("ADDRESS_LINE_II"),
                    "CITY": row_dict.pop("CITY"),
                    "STATE": row_dict.pop("STATE"),
                    "COUNTRY": row_dict.pop("COUNTRY"),
                    "PIN_CODE": row_dict.pop("PIN_CODE")
                }
                auditor_json = json.dumps(row_dict)
                auditor_json_list.append(auditor_json)
            group_df.at[index, 'Value'] = auditor_json_list

    # print(group_df)
    output_dataframes_list.append(group_df)
    group_sql_tables = group_df[group_df.columns[sql_table_name_index]].unique()
    for group_table in group_sql_tables:
        group_table_df = group_df[group_df[group_df.columns[sql_table_name_index]] == group_table]
        group_columns_list = group_table_df[group_table_df.columns[column_name_index]].unique()
        for group_column_name in group_columns_list:
            # print(group_column_name)
            # filter table df with only column value
            group_column_df = group_table_df[group_table_df[group_table_df.columns[column_name_index]] == group_column_name]
            # print(group_column_df)
            # create json dict with keys of field name and values for the same column name entries
            group_json_dict = group_column_df.set_index(group_table_df.columns[0])['Value'].to_dict()
            # Convert the dictionary to a JSON string
            group_json_string = json.dumps(group_json_dict)
            # print(group_json_string)
            # print(years)
            for year in years:
                update_database_single_value_aoc(db_config, group_table, cin_column_name, cin_column_value,
                                                 company_column_name, company_name, group_column_name,
                                                 group_json_string, year)
    print("Saving group values to database is complete")
    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()


def aoc_nbfc_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path,
                       cin_column_value, company_name):
    try:
        xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path,
                  cin_column_value, company_name)
    except Exception as e:
        print("Below Exception occurred while processing AOC NBFC file: \n ", e)
        return False
    else:
        return True
