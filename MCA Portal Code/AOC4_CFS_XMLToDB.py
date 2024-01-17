import pandas as pd
import xml.etree.ElementTree as Et
import os
import json
import mysql.connector
from Config import create_main_config_dictionary
import re
from datetime import datetime
import sys
import traceback
import logging
from logging_config import setup_logging


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


def update_database_single_value_AOC(db_config, table_name, cin_column_name, cin_value, company_name_column_name,
                                     company_name, column_name, column_value, year,nature):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    # if column_name == "financials_auditor" and num_elements == 1:
    #     first_key = next(iter(json_dict))
    #     first_value_json_list = json_dict[first_key]
    #     json_string = json.dumps(first_value_json_list)
    #     column_value = json_string
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and {}='{}' and {} = '{}'".format(table_name, cin_column_name, cin_value,
                                                                              company_name_column_name, company_name,
                                                                              'year', year,'nature',nature)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {}='{}' AND {} = '{}'".format(table_name,
                                                                                                  column_name,
                                                                                                  column_value,
                                                                                                  cin_column_name,
                                                                                                  cin_value,
                                                                                                  company_name_column_name,
                                                                                                  company_name,
                                                                                                  'Year',
                                                                                                  year,
                                                                                                  'nature',
                                                                                                   nature)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {},{}) VALUES ('{}', '{}', '{}','{}')".format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      column_name,
                                                                                      'nature',
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      column_value,
                                                                                      nature)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def insert_datatable_with_table(db_config, sql_table_name, column_names_list, df_row, cin_column_name, cin_value,
                                company_column_name, compnay_value, year):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True
    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # logging.info(result_dict)
    result_dict[cin_column_name] = cin_value
    result_dict[company_column_name] = compnay_value
    result_dict['year'] = year

    where_clause = f'SELECT * FROM {sql_table_name} WHERE '
    for key, value in result_dict.items():
        if value is not None:
            where_clause += f"`{key}` = '{value}' AND "
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
                insert_query += f"`{key}` = '{value}' , "
        insert_query = insert_query[:-2]
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        # logging.info(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
    else:
        logging.info(f"Entry with values already exists in table {sql_table_name}")
    db_cursor.close()
    db_connection.close()


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


def output_File(data,excel_file_path):
    # excel_file_path = r'C:\Users\BRADSOL\Documents\python\MCA_AOC4CFS\Ouput\Output.xlsx'
    # Save the DataFrame to Excel
    data.to_excel(excel_file_path, index=False)


def AOC_cfs_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path,
                  cin_column_value, company_name):
    try:
        setup_logging()
        config_dict_keys = []
        missing_keys = [key for key in config_dict_keys if key not in config_dict]
        Cin_Column_Name = config_dict['cin_column_name_in_db']
        Company_column_name = config_dict['company_name_column_name_in_db']
        years = []
        natures = []
        if missing_keys:
            raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")
        # initializing empty list to save all the result dataframes for saving to output excel
        output_dataframes_list = []

        if not os.path.exists(map_file_path):
            raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

        try:
            df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
            # logging.info(df_map)
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

        # initializing list to save single values data
        results_previous_year = []
        results_current_year = []
        results_financial_parameter = []
        results_common = []
        # logging.info(single_df)
        # extract single values
        previous_year_df = single_df[single_df[single_df.columns[5]] == config_dict['Previous_year_keyword']]
        current_year_df = single_df[single_df[single_df.columns[5]] == config_dict['Current_year_keyword']]
        Financial_Parameter_df = single_df[
            single_df[single_df.columns[5]] == config_dict['Financial_Parameter_Keyword']]
        common_df = single_df[single_df[single_df.columns[5]] == config_dict['Common_Keyword']]
        single_df_list = []
        for index, row in common_df.iterrows():
            field_name = str(row.iloc[0]).strip()
            parent_node = str(row.iloc[3]).strip()
            child_nodes = str(row.iloc[4]).strip()
            sql_table_name = str(row.iloc[7]).strip()
            column_name = str(row.iloc[8]).strip()
            column_json_node = str(row.iloc[9]).strip()
            if field_name == 'filing_type':
                common_df.at[index, 'Value'] = 'PDF'
                continue
            if field_name == 'filing_standard':
                common_df.at[index, 'Value'] = 'Normal'
                continue
            if parent_node == config_dict['constant_keyword']:
                continue
            value_common = get_single_value_from_xml(xml_root, parent_node, child_nodes)
            print(value_common)
            # logging.info(value)
            common_df.at[index, 'Value'] = value_common
            results_common.append([field_name, value_common, sql_table_name, column_name, column_json_node])
        output_File(Financial_Parameter_df,output_file_path)
        for index, row in Financial_Parameter_df.iterrows():
            field_name = str(row.iloc[0]).strip()
            parent_node = str(row.iloc[3]).strip()
            child_nodes = str(row.iloc[4]).strip()
            sql_table_name = str(row.iloc[7]).strip()
            column_name = str(row.iloc[8]).strip()
            column_json_node = str(row.iloc[9]).strip()
            value_financial_parameter = get_single_value_from_xml(xml_root, parent_node, child_nodes)
            # logging.info(value)
            if field_name == 'year':
                if value_financial_parameter is not None:
                    try:
                        datetime_object = datetime.fromisoformat(value_financial_parameter)
                    except ValueError:
                        datetime_object = datetime.strptime(value_financial_parameter, "%Y-%m-%d")
                    value_financial_parameter = str(datetime_object.year)
                    # logging.info(value_financial_parameter)
                    Financial_Parameter_df.at[index, 'Value'] = value_financial_parameter
            elif field_name == 'proposed_dividend':
                if value_financial_parameter == 0 or value_financial_parameter == 0.00 or value_financial_parameter == '0' or value_financial_parameter == '0.00':
                    dividend_value = 'No'
                else:
                    dividend_value = 'Yes'
                Financial_Parameter_df.at[index, 'Value'] = dividend_value
            elif field_name == 'nature':
                value_financial_parameter = 'Standalone'
                Financial_Parameter_df.at[index, 'Value'] = value_financial_parameter
            else:
                Financial_Parameter_df.at[index, 'Value'] = value_financial_parameter
            results_financial_parameter.append(
                [field_name, value_financial_parameter, sql_table_name, column_name, column_json_node])
        for index, row in previous_year_df.iterrows():
            field_name = str(row.iloc[0]).strip()
            parent_node = str(row.iloc[3]).strip()
            child_nodes = str(row.iloc[4]).strip()
            sql_table_name = str(row.iloc[7]).strip()
            column_name = str(row.iloc[8]).strip()
            column_json_node = str(row.iloc[9]).strip()
            if parent_node == 'Formula':
                continue
            value_previous_year = get_single_value_from_xml(xml_root, parent_node, child_nodes)
            # logging.info(value)
            if field_name == 'year':
                try:
                    datetime_object = datetime.fromisoformat(value_previous_year)
                    value_previous_year = str(datetime_object.date())
                except ValueError:
                    try:
                        datetime_object = datetime.strptime(value_previous_year, "%Y-%m-%d")
                        value_previous_year = str(datetime_object.date())
                    except Exception as e:
                        pass
                except Exception as e:
                    try:
                        datetime_object = datetime.strptime(value_previous_year, "%Y-%m-%d")
                        value_previous_year = str(datetime_object.date())
                    except Exception as e:
                        pass
                logging.info(value_previous_year)
            elif field_name == 'nature':
                value_previous_year = 'Standalone'
            try:
                value_previous_year = float(value_previous_year)
            except Exception as e:
                logging.info(f"Exception occured in converting{e}")
            previous_year_df.at[index, 'Value'] = value_previous_year
            results_previous_year.append(
                [field_name, value_previous_year, sql_table_name, column_name, column_json_node])
        previous_year_formula_df = previous_year_df[previous_year_df[previous_year_df.columns[3]] == config_dict['Formula_Keyword']]
        #previous_year_formula_df = previous_year_df[previous_year_df['Parent_Node'] == config_dict['Formula_Keyword']]
        for _, row in previous_year_formula_df.iterrows():
            previous_formula = row['Child_Nodes']
            previous_formula_field_name = row['Field_Name']
            for previous_field_name in previous_year_df['Field_Name']:
                previous_pattern = r'\b' + re.escape(previous_field_name) + r'\b'
                # current_formula = current_formula.replace(field_name, str(current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]))
                previous_formula = re.sub(previous_pattern, str(
                    previous_year_df[previous_year_df['Field_Name'] == previous_field_name]['Value'].values[0]),
                                          previous_formula)
            logging.info(previous_formula_field_name + ":" + previous_formula)
            try:
                if 'None' in previous_formula:
                    previous_formula = previous_formula.replace('None', '0')
                # Calculate the value using the provided formula and insert it
                previous_year_df.at[
                    previous_year_df[previous_year_df['Field_Name'] == previous_formula_field_name].index[
                        0], 'Value'] = round(eval(previous_formula), 2)
            except (NameError, SyntaxError):
                # Handle the case where the formula is invalid or contains a missing field name
                logging.info(f"Invalid formula for {previous_formula_field_name}: {previous_formula}")

        for index, row in current_year_df.iterrows():
            field_name = str(row.iloc[0]).strip()
            parent_node = str(row.iloc[3]).strip()
            child_nodes = str(row.iloc[4]).strip()
            sql_table_name = str(row.iloc[7]).strip()
            column_name = str(row.iloc[8]).strip()
            column_json_node = str(row.iloc[9]).strip()
            if parent_node == 'Formula':
                continue
            value_current_year = get_single_value_from_xml(xml_root, parent_node, child_nodes)
            # logging.info(value)
            if field_name == 'year':
                if value_current_year is not None:
                    try:
                        datetime_object = datetime.fromisoformat(value_current_year)
                    except ValueError:
                        datetime_object = datetime.strptime(value_current_year, "%Y-%m-%d")
                    value_current_year = str(datetime_object.date())
                # logging.info(value_current_year)
            elif field_name == 'nature':
                value_current_year = 'Standalone'
            try:
                value_current_year = float(value_current_year)
            except Exception as e:
                logging.info(f"Exception occured in converting{e}")
            current_year_df.at[index, 'Value'] = value_current_year
            results_current_year.append([field_name, value_current_year, sql_table_name, column_name, column_json_node])

        current_year_formula_df = current_year_df[
            current_year_df[current_year_df.columns[3]] == config_dict['Formula_Keyword']]

        for _, row in current_year_formula_df.iterrows():
            current_formula = row['Child_Nodes']
            current_formula_field_name = row['Field_Name']
            for field_name in current_year_df['Field_Name']:
                pattern = r'\b' + re.escape(field_name) + r'\b'
                # current_formula = current_formula.replace(field_name, str(current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]))
                current_formula = re.sub(pattern, str(
                    current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]), current_formula)
            logging.info(current_formula_field_name + ":" + current_formula)
            try:
                # Calculate the value using the provided formula and insert it
                if 'None' in current_formula:
                    current_formula = current_formula.replace('None', '0')
                current_year_df.at[current_year_df[current_year_df['Field_Name'] == current_formula_field_name].index[
                                       0], 'Value'] = round(eval(current_formula), 2)
            except (NameError, SyntaxError):
                # Handle the case where the formula is invalid or contains a missing field name
                logging.info(f"Invalid formula for {current_formula_field_name}: {current_formula}")
        current_year = current_year_df[current_year_df['Field_Name'] == 'year']['Value'].values[0]

        if current_year is None:
            pass
            # raise Exception(f"Exception occurred while extracting year value {current_year} from current year data")
        previous_year = previous_year_df[previous_year_df['Field_Name'] == 'year']['Value'].values[0]

        # if previous_year is None:
        #     raise Exception(f"Exception occurred while extracting year value {previous_year} from previous year data")
        # if not AOC_4_first_file_found:
        #     single_df_list.append(current_year_df)
        previous_year_nature = previous_year_df[previous_year_df['Field_Name'] == 'nature']['Value'].values[0]
        current_year_nature = current_year_df[current_year_df['Field_Name'] == 'nature']['Value'].values[0]
        db_connection = mysql.connector.connect(**db_config)
        db_cursor = db_connection.cursor()
        previous_year_check_query = "select * from financials where year = %s and cin =%s and nature = %s"
        previous_year_values = (previous_year, cin_column_value,previous_year_nature)
        logging.info(previous_year_check_query % previous_year_values)
        db_cursor.execute(previous_year_check_query, previous_year_values)
        previous_year_result = db_cursor.fetchall()
        current_year_check_query = "select * from financials where year = %s and cin =%s and nature = %s"
        current_year_values = (current_year, cin_column_value,current_year_nature)
        logging.info(current_year_check_query, current_year_values)
        db_cursor.execute(current_year_check_query, current_year_values)
        current_year_result = db_cursor.fetchall()
        if len(current_year_result) == 0:
            single_df_list.append(current_year_df)
            years.append(current_year)
            natures.append(current_year_nature)
            # logging.info("Current year not found so inserting")
        if len(previous_year_result) == 0:
            single_df_list.append(previous_year_df)
            years.append(previous_year)
            natures.append(previous_year_nature)
            # logging.info("Previous year not found so inserting")
        db_cursor.close()
        db_connection.close()
        single_df_list.append(Financial_Parameter_df)
        Current_Year_output_df = pd.DataFrame(current_year_df,
                                              columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name',
                                                       'Column_JSON_Node'])
        Previous_Year_output_df = pd.DataFrame(previous_year_df,
                                               columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name',
                                                        'Column_JSON_Node'])
        common_output_df = pd.DataFrame(common_df, columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name',
                                                            'Column_JSON_Node'])
        Financial_output_df = pd.DataFrame(Financial_Parameter_df,
                                           columns=['Field_Name', 'Value', 'Table_Name', 'Column_Name',
                                                    'Column_JSON_Node'])
        output_dataframes_list.append(Current_Year_output_df)
        output_dataframes_list.append(Previous_Year_output_df)
        output_dataframes_list.append(common_output_df)
        output_dataframes_list.append(Financial_output_df)

        for df in single_df_list:
            logging.info(df)
            sql_tables_list = df[df.columns[7]].unique()
            logging.info(sql_tables_list)
            year_value = df[df['Field_Name'] == 'year']['Value'].values[0]
            nature_value = df[df['Field_Name'] == 'nature']['Value'].values[0]
            logging.info(year_value)
            for table_name in sql_tables_list:
                table_df = df[df[df.columns[7]] == table_name]
                columns_list = table_df[table_df.columns[8]].unique()
                logging.info(columns_list)
                for column_name in columns_list:
                    logging.info(column_name)
                    # filter table df with only column value
                    column_df = table_df[table_df[table_df.columns[8]] == column_name]
                    logging.info(column_df)
                    # create json dict with keys of field name and values for the same column name entries
                    json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                    # Convert the dictionary to a JSON string
                    json_string = json.dumps(json_dict)
                    logging.info(json_string)
                    try:
                        update_database_single_value_AOC(db_config, table_name, Cin_Column_Name, cin_column_value,
                                                         Company_column_name, company_name, column_name, json_string,
                                                         year_value,nature_value)
                    except Exception as e:
                        logging.info(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                                     f"with data {json_string}")
        common_sql_tables_list = common_df[common_df.columns[7]].unique()
        logging.info(common_sql_tables_list)
        # if AOC_4_first_file_found:
        #     years = years[1:]
        report_value = None
        for common_table_name in common_sql_tables_list:
            logging.info(common_table_name)
            if common_table_name != config_dict['financials_table_name']:
                logging.info("Continuing table")
                continue
            common_table_df = common_df[common_df[common_df.columns[7]] == common_table_name]
            logging.info(common_table_df)
            common_columns_list = common_table_df[common_table_df.columns[8]].unique()
            logging.info(common_columns_list)
            for common_column_name in common_columns_list:
                logging.info(common_column_name)
                if common_column_name != config_dict['auditor_comments_column_name']:
                    logging.info("continuing column")
                    continue
                logging.info(common_column_name)
                # filter table df with only column value
                common_column_df = common_table_df[common_table_df[common_table_df.columns[8]] == common_column_name]
                logging.info(common_column_df)
                if common_column_name == config_dict['auditor_comments_column_name']:
                    auditor_comments_row_index = common_column_df[common_column_df[common_column_df.columns[8]] ==
                                                                  config_dict['auditor_comments_column_name']].index[0]
                    if auditor_comments_row_index is not None:
                        comment_value = common_column_df.loc[auditor_comments_row_index, 'Value']
                        logging.info(f'{comment_value=}')
                        if comment_value == 'NO':
                            report_value = '''As per Auditors Report, the accounts give a true and fair view, as per the accounting principles generally accepted, of the
                                              state of affairs in the case of Balance sheet and, Profit or Loss in the case of Profit & Loss Accounts. Auditors Report is
                                              Unqualified i.e. Clean'''
                            logging.info(report_value)
                            auditor_report_row_index = common_table_df[common_table_df[common_table_df.columns[8]] ==
                                                                       config_dict[
                                                                           'disclosures_auditor_report_column_name']].index[
                                0]
                            director_report_row_index = common_table_df[common_table_df[common_table_df.columns[8]] ==
                                                                        config_dict[
                                                                            'disclosures_director_report_column_name']].index[
                                0]
                            if auditor_report_row_index is not None:
                                common_df.loc[auditor_report_row_index, 'Value'] = report_value
                            if director_report_row_index is not None:
                                common_df.loc[director_report_row_index, 'Value'] = report_value
                            break
                        else:
                            report_value = None
                            logging.info(report_value)
                            break
                logging.info(common_table_df)
            # auditor_report_row_index = common_table_df[common_table_df[common_table_df.columns[8]] ==
            #                                             config_dict['disclosures_auditor_report_column_name']].index[0]
            # director_report_row_index = common_table_df[common_table_df[common_table_df.columns[8]] ==
            #                                                config_dict['disclosures_director_report_column_name']].index[0]
            # if auditor_report_row_index is not None:
            #     common_table_df.loc[auditor_report_row_index, 'Value'] = report_value
            # if director_report_row_index is not None:
            #     common_table_df.loc[director_report_row_index, 'Value'] = report_value

        for common_table_name in common_sql_tables_list:
            common_table_df = common_df[common_df[common_df.columns[7]] == common_table_name]
            common_columns_list = common_table_df[common_table_df.columns[8]].unique()
            logging.info(common_columns_list)
            for common_column_name in common_columns_list:
                logging.info(common_column_name)
                # filter table df with only column value
                common_column_df = common_table_df[common_table_df[common_table_df.columns[8]] == common_column_name]
                logging.info(common_column_df)
                common_json_dict = common_column_df.set_index(common_table_df.columns[0])['Value'].to_dict()
                # Convert the dictionary to a JSON string
                common_json_string = json.dumps(common_json_dict)
                logging.info(common_json_string)
                logging.info(years)
                for year,nature in zip(years,natures):
                    if year is None or year == '':
                        continue
                    try:
                        update_database_single_value_AOC(db_config, common_table_name, Cin_Column_Name,
                                                         cin_column_value,
                                                         Company_column_name, company_name, common_column_name,
                                                         common_json_string,
                                                         year,nature)
                    except Exception as e:
                        logging.info(f"Exception {e} occurred while updating data in dataframe for {common_table_name} "
                                     f"with data {common_json_string}")
        for index, row in group_df.iterrows():
            field_name = str(row.iloc[0]).strip()
            parent_node = str(row.iloc[3]).strip()
            child_nodes = str(row.iloc[4]).strip()
            sql_table_name = str(row.iloc[7]).strip()
            column_names = str(row.iloc[8]).strip()
            column_json_node = str(row.iloc[9]).strip()

            table_node_name = parent_node
            # logging.info(table_node_name)
            try:
                table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
            except Exception as e:
                logging.info(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
                continue
            table_df = pd.DataFrame(table_in_list)
            # table_df.dropna(inplace=True)
            logging.info(table_df)
            if field_name == 'financials_auditor':
                column_json_node_list = [x.strip() for x in column_json_node.split(',')]
                column_child_node_list = [x.strip() for x in child_nodes.split(',')]
                table_df.columns = column_child_node_list
                try:
                    first_row_df = table_df.iloc[[0]]
                except Exception as e:
                    continue
                row_dicts = first_row_df.to_dict(orient='records')

                # Convert each dictionary to a JSON string
                auditor_json = None
                for row_dict in row_dicts:
                    row_dict["address"] = (
                            str(row_dict.pop("ADDRESS_LINE_I", "")) +
                            ", " +
                            str(row_dict.pop("ADDRESS_LINE_II", "")) +
                            ", " +
                            str(row_dict.pop("CITY", "")) +
                            ", " +
                            str(row_dict.pop("STATE", "")) +
                            ", " +
                            str(row_dict.pop("COUNTRY", "")) +
                            ", " +
                            str(row_dict.pop("PIN_CODE", ""))
                    )
                    # row_dict["address"] = row_dict.pop("ADDRESS_LINE_I") + ", " + row_dict.pop(
                    #     "ADDRESS_LINE_II") + ", " + row_dict.pop("CITY") + ", " + row_dict.pop(
                    #     "STATE") + ", " + row_dict.pop("COUNTRY") + ", " + row_dict.pop("PIN_CODE")
                    # row_dict["ADDRESS"] = {
                    #     "ADDRESS_LINE_I": row_dict.pop("ADDRESS_LINE_I"),
                    #     "ADDRESS_LINE_II": row_dict.pop("ADDRESS_LINE_II"),
                    #     "CITY": row_dict.pop("CITY"),
                    #     "STATE": row_dict.pop("STATE"),
                    #     "COUNTRY": row_dict.pop("COUNTRY"),
                    #     "PIN_CODE": row_dict.pop("PIN_CODE")
                    # }

                    row_dict["auditor_name"] = row_dict.pop("NAME_OF_MEMBER")
                    row_dict["auditor_firm_name"] = row_dict.pop("NAME_AUDT_AUDTRF")
                    row_dict["pan"] = row_dict.pop("IT_PAN")
                    row_dict["membership_number"] = row_dict.pop("MEMBERSHIP_NUMBR")
                    row_dict["firm_registration_number"] = row_dict.pop("MEMBERSHIP_NUM_A")

                    auditor_json = json.dumps(row_dict)
                group_df.at[index, 'Value'] = auditor_json
                for year,nature in zip(years,natures):
                    update_database_single_value_AOC(db_config, sql_table_name, Cin_Column_Name, cin_column_value,
                                                     Company_column_name, company_name, column_names, auditor_json,
                                                     year,nature)
                if len(table_df.index) > 1:
                    try:
                        remaining_row_df = table_df.iloc[1:]
                        rows_dicts_remaining = remaining_row_df.to_dict(orient='records')
                        for row_dict_remaining in rows_dicts_remaining:
                            # row_dict_remaining["ADDRESS"] = {
                            #     "ADDRESS_LINE_I": row_dict_remaining.pop("ADDRESS_LINE_I"),
                            #     "ADDRESS_LINE_II": row_dict_remaining.pop("ADDRESS_LINE_II"),
                            #     "CITY": row_dict_remaining.pop("CITY"),
                            #     "STATE": row_dict_remaining.pop("STATE"),
                            #     "COUNTRY": row_dict_remaining.pop("COUNTRY"),
                            #     "PIN_CODE": row_dict_remaining.pop("PIN_CODE")
                            # }

                            row_dict_remaining["auditor_name"] = row_dict_remaining.pop("NAME_OF_MEMBER")
                            row_dict_remaining["auditor_firm_name"] = row_dict_remaining.pop("NAME_AUDT_AUDTRF")
                            row_dict_remaining["pan"] = row_dict_remaining.pop("IT_PAN")
                            row_dict_remaining["membership_number"] = row_dict_remaining.pop("MEMBERSHIP_NUMBR")
                            row_dict_remaining["firm_registration_number"] = row_dict_remaining.pop("MEMBERSHIP_NUM_A")
                            # row_dict_remaining["address"] = row_dict_remaining.pop(
                            #     "ADDRESS_LINE_I") + ", " + row_dict_remaining.pop(
                            #     "ADDRESS_LINE_II") + ", " + row_dict_remaining.pop(
                            #     "CITY") + ", " + row_dict_remaining.pop(
                            #     "STATE") + ", " + row_dict_remaining.pop("COUNTRY") + ", " + row_dict_remaining.pop(
                            #     "PIN_CODE")
                            row_dict_remaining["address"] = (
                                    str(row_dict_remaining.pop("ADDRESS_LINE_I", "")) +
                                    ", " +
                                    str(row_dict_remaining.pop("ADDRESS_LINE_II", "")) +
                                    ", " +
                                    str(row_dict_remaining.pop("CITY", "")) +
                                    ", " +
                                    str(row_dict_remaining.pop("STATE", "")) +
                                    ", " +
                                    str(row_dict_remaining.pop("COUNTRY", "")) +
                                    ", " +
                                    str(row_dict_remaining.pop("PIN_CODE", ""))
                            )
                        remaining_row_df_new = pd.DataFrame(rows_dicts_remaining)
                        remaining_row_df_new.columns = column_json_node_list
                        remaining_row_df_new["address"] = remaining_row_df_new["address"].apply(lambda x: json.dumps(x))
                        for year in years:
                            for _, df_row in remaining_row_df_new.iterrows():
                                # logging.info(df_row)
                                # logging.info(table_df.columns)
                                try:
                                    insert_datatable_with_table(db_config, config_dict['Additional_Auditor_Table_Name'],
                                                                remaining_row_df_new.columns, df_row, Cin_Column_Name,
                                                                cin_column_value, Company_column_name, company_name,
                                                                year)
                                except Exception as e:
                                    logging.info(
                                        f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                                        df_row)
                    except Exception as e:
                        pass

        logging.info(group_df)
        output_dataframes_list.append(group_df)

        # group_sql_tables = group_df[group_df.columns[7]].unique()
        # for group_table in group_sql_tables:
        #     group_table_df = group_df[group_df[group_df.columns[7]] == group_table]
        #     group_columns_list = group_table_df[group_table_df.columns[8]].unique()
        #     for group_column_name in group_columns_list:
        #         logging.info(group_column_name)
        #         # filter table df with only column value
        #         group_column_df = group_table_df[group_table_df[group_table_df.columns[8]] == group_column_name]
        #         logging.info(group_column_df)
        #         # create json dict with keys of field name and values for the same column name entries
        #         group_json_dict = group_column_df.set_index(group_table_df.columns[0])['Value'].to_dict()
        #         # Convert the dictionary to a JSON string
        #         group_json_string = json.dumps(group_json_dict)
        #         logging.info(group_json_string)
        #         logging.info(years)
        #         for year in years:
        #             update_database_single_value_AOC(db_config, group_table, Cin_Column_Name, cin_column_value,
        #                                              Company_column_name, company_name, group_column_name,
        #                                              group_json_string, year)
        # if group_table == 'financial_parameters':
        #     break
        try:
            with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
                row_index = 0
                for dataframe in output_dataframes_list:
                    # logging.info(dataframe)
                    dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                    row_index += len(dataframe.index) + 2
        except Exception as e:
            print(e)

        output_dataframes_list.clear()

    except Exception as e:
        logging.info(f"Excpetion occured while inserting into db for AOC {e}")
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.info(line.strip())
        return False
    else:
        return True


# Cin = 'U01210MH1999PTC119449'
# Company = 'Tata Capital'
# output = r'C:\Users\BRADSOL\Documents\python\MCA_AOC4CFS\Ouput\Form AOC - 4 CFS-23112021_signed_values.xlsx'
# xml = r'C:\Users\BRADSOL\Documents\python\MCA_AOC4CFS\XML_Files\Form AOC - 4 CFS-23112021_signed.xml'
# main_dict = create_main_config_dictionary(r'C:\Users\BRADSOL\Documents\python\MCA_AOC4CFS\Config_Python.xlsx',
#                                           'AOC CFS')
# config_dict = main_dict[0]
# aoc_config = r'C:\Users\BRADSOL\Documents\python\MCA_AOC4CFS\AOC-4_CFS_nodes_seperated_config 1 1.xlsx'
# db_config = {
#     "host": "localhost",
#     "user": "root",
#     "password": "",
#     "database": "classle3_mns_credit",
# }
# print(AOC_xml_to_db(db_config, config_dict, aoc_config, "Sheet1", xml, output, Cin, Company))
