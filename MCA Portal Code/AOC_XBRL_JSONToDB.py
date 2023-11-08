import json
import re
# Sample JSON data
import os
import pandas as pd
import json
import mysql.connector
import sys
import traceback
from datetime import datetime
import math
from Config import create_main_config_dictionary
import xml.etree.ElementTree as Et

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

def JSONtoDB_AOC_XBRL_straight(Cin,CompanyName,json_file_path,target_header):
    years = []

    # Define the regular expression pattern for matching the date range

    # Read the JSON data from the file
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)

    # Define the header you want to search for

    # Initialize variables to store the current and previous year values
    current_year_value = None
    previous_year_value = None
    values = []
    header = None
    value_found = False
    header_found = False
    # Search for the target header in the JSON data
    for item in data:
        for table_name, table_data in item.items():
            for row in table_data:
                try:
                    row_values = [value for i, value in enumerate(row) if (
                            (isinstance(row[i], (float, int)) and not math.isnan(row[i])) or (
                            isinstance(row[i], str) and row[i] != "NaN"))]
                    if row[0] == target_header and len(row_values) > 1:
                        #print("Going Straight")
                        #current_year_value = row[1]
                        #print(current_year_value)
                        row_values = [value for i, value in enumerate(row) if (
                                (isinstance(row[i], (float, int)) and not math.isnan(row[i])) or (
                                isinstance(row[i], str) and row[i] != "NaN"))]
                        current_year_value = row_values[1]
                        try:
                            previous_year_value = row_values[2]
                        except Exception as e:
                            print(f"Exception in straight values {e} for {target_header}")
                            previous_year_value = 0
                        # if len(row) > 2 and ((isinstance(row[2], (float, int)) and not math.isnan(row[2])) or (isinstance(row[2], str) and row[2] != "NaN")):
                        #     previous_year_value = row[2]
                        #     print(previous_year_value)
                        # elif len(row) > 3 and ((isinstance(row[3], (float, int)) and not math.isnan(row[3])) or (isinstance(row[3], str) and row[3] != "NaN")):
                        #     previous_year_value = row[3]
                        # elif len(row) > 4 and ((isinstance(row[4], (float, int)) and not math.isnan(row[4])) or (isinstance(row[4], str) and row[4] != "NaN")):
                        #     previous_year_value = row[4]
                        # else:
                        #     previous_year_value = None
                        current_year_value = re.sub(r'\([^)]*\)', '', str(current_year_value))
                        previous_year_value = re.sub(r'\([^)]*\)', '', str(previous_year_value))
                        values.append(current_year_value)
                        values.append(previous_year_value)
                    # elif len(row) >= 3 and row[0] == target_header and ((isinstance(row[2], (float, int)) and math.isnan(row[2])) or (isinstance(row[2], str) and row[2] == "NaN")) and ((isinstance(row[1], (float, int)) and not math.isnan(row[1])) or (isinstance(row[1], str) and row[1] != "NaN")):
                    #     current_year_value = row[1]
                    #     print(current_year_value)
                    #     if len(row) > 3 and row[3] is not None:
                    #         previous_year_value = row[3]
                    #         print(previous_year_value)
                    #     current_year_value = re.sub(r'\([^)]*\)', '', current_year_value)
                    #     previous_year_value = re.sub(r'\([^)]*\)', '', previous_year_value)
                    #     values.append(current_year_value)
                    #     values.append(previous_year_value)
                    # elif len(row) >= 4 and row[0] == target_header and ((isinstance(row[1], (float, int)) and math.isnan(row[1])) or (isinstance(row[2], str) and row[1] == "NaN")) and ((isinstance(row[2], (float, int)) and not math.isnan(row[2])) or (isinstance(row[2], str) and row[2] != "NaN")):
                    #     current_year_value = row[2]
                    #     print(current_year_value)
                    #     if len(row) > 4 and row[4] is not None:
                    #         previous_year_value = row[4]
                    #         print(previous_year_value)
                    #     current_year_value= re.sub(r'\([^)]*\)', '', current_year_value)
                    #     previous_year_value = re.sub(r'\([^)]*\)', '', previous_year_value)
                    #     values.append(current_year_value)
                    #     values.append(previous_year_value)
                    else:
                        value_previous_year = None
                        value_current_year = None
                        #print("Going for breaks")
                        if not header_found:
                            if row[0] == target_header:
                                header_found = True
                            else:
                                continue
                        if header is None and any(math.isnan(x) for x in row[1:] if isinstance(x, (float, int))):
                            header = row[0]
                            #print(header)
                        elif header is not None and isinstance(row[0], (float, int)) and math.isnan(row[0]):
                            row_values = [value for i, value in enumerate(row) if (
                                        (isinstance(row[i], (float, int)) and not math.isnan(row[i])) or (
                                            isinstance(row[i], str) and row[i] != "NaN"))]
                            #print(row_values)
                            value_current_year = row_values[0]
                            value_previous_year = row_values[1]
                            value_current_year = re.sub(r'\([^)]*\)', '', str(value_current_year))
                            value_previous_year = re.sub(r'\([^)]*\)', '', str(value_previous_year))
                            # if row[1] is not None:
                            #     value_current_year = row[1]
                            #     print(value_current_year)
                            #     if len(row) > 2 and row[2] is not None:
                            #         value_previous_year = row[2]
                            #         print(previous_year_value)
                            #     value_current_year = re.sub(r'\([^)]*\)', '', value_current_year)
                            #     value_previous_year = re.sub(r'\([^)]*\)', '', value_previous_year)
                            # elif row[2] is not None:
                            #     value_current_year = row[2]
                            #     print(value_current_year)
                            #     if len(row) > 3 and row[3] is not None:
                            #         value_previous_year = row[3]
                            #         print(previous_year_value)
                            #     value_current_year = re.sub(r'\([^)]*\)', '', value_current_year)
                            #     value_previous_year = re.sub(r'\([^)]*\)', '', value_previous_year)
                            # elif row[3] is not None:
                            #     value_current_year = row[3]
                            #     print(value_current_year)
                            #     if len(row) > 4 and row[4] is not None:
                            #         value_previous_year = row[4]
                            #         print(previous_year_value)
                            #     value_current_year = re.sub(r'\([^)]*\)', '', value_current_year)
                            #     value_previous_year = re.sub(r'\([^)]*\)', '', value_previous_year)
                            # else:

                            value_found = True
                            values.append(value_current_year)
                            values.append(value_previous_year)
                            header_found = False
                        else:
                            print("Breaks header not found")
                            continue
                except Exception as e:
                    print(f"Main Exception in straight values {e} for {target_header}")
                    exc_type, exc_value, exc_traceback = sys.exc_info()

                    # Get the formatted traceback as a string
                    traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

                    # Print the traceback details
                    for line in traceback_details:
                        print(line.strip())
                    continue
                if value_found:
                    break
            if value_found:
                break

    # Print the results
    # if current_year_value is not None:
    #     print(f"Current Year Value for '{target_header}': {current_year_value}")
    #     if previous_year_value is not None:
    #         print(f"Previous Year Value for '{target_header}': {previous_year_value}")
    #     else:
    #         print(f"Previous Year Value for '{target_header}': Not available")
    # else:
    #     print(f"'{target_header}' not found in any table.")

    for item in data:
        plain_text_by_page = item.get("plain_text_by_page", [])
        if len(plain_text_by_page) != 0:
            for date in plain_text_by_page:
                date_range = re.search(r'(\d{2}/\d{2}/\d{4}) to (\d{2}/\d{2}/\d{4})', date)
                if date_range:
                    start_date = date_range.group(1)
                    end_date = date_range.group(2)
                    years.append(end_date)
                    years.append(start_date)
                    break  # Exit loop after the first match
    # Print the extracted years
    # print(years[0])
    # print(values[0])
    return values
    # for insert_data in zip(years,values):
    #     year = insert_data[0]
    #     value = insert_data[1]
    #     print(year,value)

# json_file_path = "all_data.json"
# target_header = "Turnover of highest contributing product or service"
#  JSONtoDB_AOC_XBRL_straight(None,None,json_file_path,target_header)

def JSON_Fields_for_breaks(json_file_path,input_header):
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)

    values = []
    # Initialize variables to store the header and value
    header = None
    value_previous_year = None
    value_current_year = None
    header_found = False
    value_found = False
    for item in data:
        for table_name,table_data in item.items():
            for row in table_data:
                #print(row)
                if not header_found:
                    if row[0] == input_header:
                        header_found = True
                    else:
                        continue
                if header is None and any(math.isnan(x) for x in row[1:] if isinstance(x, (float, int))):
                    header = row[0]
                elif header is not None and isinstance(row[0], (float, int)) and math.isnan(row[0]):
                    value_current_year = row[1]
                    value_previous_year = row[2]
                    value_current_year = re.sub(r'\([^)]*\)', '', str(value_current_year))
                    value_previous_year = re.sub(r'\([^)]*\)', '', str(value_previous_year))
                    value_found = True
                    values.append(value_current_year)
                    values.append(value_previous_year)
                    break
                else:
                    continue
            if value_found:
                break
        if value_found:
            break
    if header_found:
        if header and (value_previous_year or value_current_year):
            # print("Header:", header)
            # print("Value:", value)
            return values
        else:
            print("No NaN value found below the header.")
            return []
    else:
        print("Header not found in the JSON structure.")
        return []
    
def capture_values_from_text(json_file_path,input_sentence,field_name):
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)
    values = []
    # Regular expression to capture crore values
    crore_pattern = r'(\d+(?:\.\d+)?\s*crore)'

    # Search for the input keyword in the JSON data
    complete_sentence = ""
    plain_text_data = data[-1]
    sentence_data = plain_text_data['plain_text_by_page']
    for sentence in sentence_data:
        complete_sentence += sentence
    if input_sentence is not None:
        if input_sentence in complete_sentence:
            if field_name == 'earning_fc' or field_name == 'expenditure_fc':
                match = re.search(fr'{input_sentence}.*?({crore_pattern})', complete_sentence)
                if match:
                    value = match.group(1)
                    return value
            elif field_name == 'companies_caro_applicable':
                keyword_position = complete_sentence.find(input_sentence)
                sentence_part = complete_sentence[keyword_position + len(input_sentence):]
                # Split the sentence part into words and capture the next two words
                words = re.findall(r'\S+', sentence_part)
                if len(words) >= 2:
                    next_two_words = " ".join(words[:2])
                    print(f"Next Two Words: {next_two_words}")
                    if 'not' in next_two_words.lower():
                        next_two_words = 'Not Applicable'
                    elif 'not' not in next_two_words.lower():
                        next_two_words = 'Applicable'
                    else:
                        pass
                    return next_two_words
        else:
            print("Word not found")
    else:
        return complete_sentence


def Auditor_information(sentence, keyword):
    keyword_position = sentence.find(keyword)
    if keyword == 'contains adverse remarks':
        print("Contains adverse remarks")
        pattern = re.escape(keyword) + r'(\s*[^ ]+(?: [^ ]+){0})'
    elif keyword == 'under clause (viii) of CARO 2016 is':
        pattern = re.escape(keyword) + r'(\s*[^ ]+(?: [^ ]+){0,1})'
    else:
        pattern = re.escape(keyword) + r'(\s*[^ ]+(?: [^ ]+){0,10})'
    match = re.search(pattern, sentence)
    if match:
        value = match.group(1)
        return value.strip()  # Remove leading/trailing whitespace
    else:
        return None

def update_database_single_value_AOC(db_config, table_name, cin_column_name, cin_value,company_name_column_name,company_name, column_name, column_value,year):
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
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and {}='{}'".format(table_name, cin_column_name, cin_value,company_name_column_name,company_name,'year',year)
    print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        print(err)
    result = db_cursor.fetchall()
    # print(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {}='{}'".format(table_name, column_name,
                                                                                      column_value, cin_column_name,
                                                                                      cin_value,
                                                                                      company_name_column_name,
                                                                                      company_name,
                                                                                      'Year',
                                                                                      year)
        print(update_query)
        db_cursor.execute(update_query)
        print("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      column_value)
        print(insert_query)
        db_cursor.execute(insert_query)
        print("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()
def AOC_XBRL_JSON_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, json_file_path, output_file_path,cin_column_value, company_name):
    output_dataframes_list = []
    cin_column_name = config_dict['cin_column_name_in_db']
    company_column_name = config_dict['company_name_column_name_in_db']
    if not os.path.exists(map_file_path):
        raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

    try:
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        # print(df_map)
    except Exception as e:
        raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))

    # adding an empty column to mapping df
    df_map['Value'] = None
    filing_standard_check = capture_values_from_text(json_file_path, None, None)
    filing_standard = None
    if str(config_dict['IND_Taxonomy_Condition']) in filing_standard_check:
        filing_standard = config_dict['IND_Taxonomy_Keyword']
    elif str(config_dict['AS_Taxonomy_Condition']) in filing_standard_check:
        filing_standard = config_dict['Taxonomy_Keyword']
    else:
        filing_standard = None
    print(filing_standard)
    # creating new filtered dataframes for single and group values
    single_df = df_map[(df_map[df_map.columns[2]] == config_dict['single_type_indicator']) & ((df_map[df_map.columns[7]] == filing_standard) | (df_map[df_map.columns[7]] == 'Common'))]

    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"The XML file '{json_file_path}' is not found.")

    for index,row in single_df.iterrows():
        field_name = str(row.iloc[0]).strip()
        parent_node = str(row.iloc[3]).strip()
        child_nodes = str(row.iloc[4]).strip()
        sql_table_name = str(row.iloc[7]).strip()
        column_name = str(row.iloc[8]).strip()
        column_json_node = str(row.iloc[9]).strip()
        year_category = str(row.iloc[5]).strip()

        if parent_node == config_dict['Formula_Keyword']:
            continue

        if field_name == config_dict['Nature_field_name']:
            if config_dict['Standalone_nature_keyword'] in filing_standard_check:
                nature = 'Standalone'
            elif config_dict['Consolidated_nature_keyword'] in filing_standard_check:
                nature = 'Consolidated'
            else:
                nature = None
            single_df.at[index, 'Value'] = nature
            continue
        # if parent_node == config_dict['Breaks_Keyword']:
        #     values = JSON_Fields_for_breaks(json_file_path,child_nodes)
        #     if year_category == 'Previous':
        #         single_df.at[index, 'Value'] = values[1]
        #     elif year_category == 'Current':
        #         single_df.at[index,'Value'] = values[0]
        #     else:
        #         single_df.at[index, 'Value'] = None
        if parent_node == config_dict['Straight_Keyword']:
            values = JSONtoDB_AOC_XBRL_straight(cin_column_value,company_name,json_file_path,child_nodes)
            print(f"{child_nodes}:{values}")
            if len(values) != 0:
                if year_category == 'Previous':
                    single_df.at[index, 'Value'] = values[1]
                elif year_category == 'Current':
                    single_df.at[index,'Value'] = values[0]
                elif year_category == config_dict['Financial_Parameter_Keyword']:
                    single_df.at[index, 'Value'] = values[0]
                else:
                    single_df.at[index, 'Value'] = None
        elif parent_node == config_dict['Constant_Keyword']:
            if field_name == 'filing_standard':
                value = filing_standard
            else:
                value = child_nodes
            single_df.at[index, 'Value'] = value
        elif parent_node == config_dict['Description_Keyword']:
            words_value = capture_values_from_text(json_file_path,child_nodes,field_name)
            single_df.at[index, 'Value'] = words_value
        elif parent_node == config_dict['Auditor_Keyword']:
            auditor_complete_sentence = capture_values_from_text(json_file_path, None, None)
            auditor_value = Auditor_information(auditor_complete_sentence,child_nodes)
            print(f"{field_name}:{auditor_value}")
            single_df.at[index, 'Value'] = auditor_value
    print(single_df)
    current_year_df = single_df[(single_df[single_df.columns[5]] == config_dict['Current_year_keyword'])]
    previous_year_df = single_df[(single_df[single_df.columns[5]] == config_dict['Previous_year_keyword'])]
    common_df = single_df[(single_df[single_df.columns[5]] == config_dict['Common_Keyword'])]
    financial_parameter_df = single_df[(single_df[single_df.columns[5]] == config_dict['Financial_Parameter_Keyword'])]
    current_year_formula_df = current_year_df[(current_year_df[current_year_df.columns[3]] == config_dict['Formula_Keyword'])]
    previous_year_formula_df = previous_year_df[(previous_year_df[previous_year_df.columns[3]] == config_dict['Formula_Keyword'])]
    for _, row in current_year_formula_df.iterrows():
        current_formula = row['Child_Nodes']
        current_formula_field_name = row['Field_Name']
        for field_name in current_year_df['Field_Name']:
            pattern = r'\b' + re.escape(field_name) + r'\b'
            current_formula = re.sub(pattern, str(
                current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]).replace(',',''), current_formula)
        current_formula = re.sub(r'\([^)]*\)', '', current_formula)
        print(current_formula)
        try:
            # print(f'{current_formula=}')
            if 'None' in current_formula:
                current_formula = current_formula.replace('None', '0')
                # print(f'{current_formula=}')
            if 'nan' in current_formula:
                current_formula = current_formula.replace('nan', '0')
            # Calculate the value using the provided formula and insert it
            current_year_df.at[
                current_year_df[current_year_df['Field_Name'] == current_formula_field_name].index[
                    0], 'Value'] = eval(current_formula)
        except Exception as e:
            print(f"Exception occured for current year formula {current_formula_field_name}: {current_formula} {e}")
        except (NameError, SyntaxError):
            # Handle the case where the formula is invalid or contains a missing field name
            print(f"Invalid formula for {current_formula_field_name}: {current_formula}")
    for _, row in previous_year_formula_df.iterrows():
        previous_formula = row['Child_Nodes']
        previous_formula_field_name = row['Field_Name']
        for previous_field_name in previous_year_df['Field_Name']:
            previous_pattern = r'\b' + re.escape(previous_field_name) + r'\b'
            previous_formula = re.sub(previous_pattern, str(
                previous_year_df[previous_year_df['Field_Name'] == previous_field_name]['Value'].values[0]).replace(',',''),
                                      previous_formula)
        previous_formula = re.sub(r'\([^)]*\)', '', previous_formula)
        print(previous_formula)
        try:
            # print(f'{previous_formula=}')
            if 'None' in previous_formula:
                previous_formula = previous_formula.replace('None', '0')
                # print(f'{previous_formula=}')
            if 'nan' in previous_formula:
                previous_formula = previous_formula.replace('nan', '0')
            # Calculate the value using the provided formula and insert it
            previous_year_df.at[
                previous_year_df[previous_year_df['Field_Name'] == previous_formula_field_name].index[
                    0], 'Value'] = eval(previous_formula)
        except Exception as e:
            print(f"Exception occured for Previous year formula {previous_formula_field_name}: {previous_formula} {e}")
        except (NameError, SyntaxError):
            # Handle the case where the formula is invalid or contains a missing field name
            print(f"Exception occurred while processing previous year data - \n"
                  f" Invalid formula for {previous_formula_field_name}: {previous_formula}")
    years = []
    current_year = current_year_df[current_year_df['Field_Name'] == 'year']['Value'].values[0]
    previous_year = previous_year_df[previous_year_df['Field_Name'] == 'year']['Value'].values[0]
    years.append(current_year)
    years.append(previous_year)
    if current_year is None:
        raise Exception(f"Exception occurred while extracting year value {current_year} from current year data")
    if previous_year is None:
        raise Exception(f"Exception occurred while extracting year value {previous_year} from previous year data")
    single_df_list = []
    single_df_list.append(current_year_df)
    single_df_list.append(previous_year_df)
    single_df_list.append(financial_parameter_df)
    for df in single_df_list:
        print(df)
        sql_tables_list = df[df.columns[8]].unique()
        print(sql_tables_list)
        year_value = df[df['Field_Name'] == 'year']['Value'].values[0]
        print(year_value)
        for table_name in sql_tables_list:
            table_df = df[df[df.columns[8]] == table_name]
            columns_list = table_df[table_df.columns[9]].unique()
            print(columns_list)
            for column_name in columns_list:
                print(column_name)
                # filter table df with only column value
                column_df = table_df[table_df[table_df.columns[9]] == column_name]
                print(column_df)
                # create json dict with keys of field name and values for the same column name entries
                json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                # Convert the dictionary to a JSON string
                json_string = json.dumps(json_dict)
                print(json_string)
                try:
                    update_database_single_value_AOC(db_config, table_name, cin_column_name, cin_column_value,
                                                     company_column_name, company_name, column_name, json_string,
                                                     year_value)
                except Exception as e:
                    print(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                          f"with data {json_string}")
    common_sql_tables_list = common_df[common_df.columns[8]].unique()
    for common_table_name in common_sql_tables_list:
        common_table_df = common_df[common_df[common_df.columns[8]] == common_table_name]
        common_columns_list = common_table_df[common_table_df.columns[9]].unique()
        print(common_columns_list)
        for common_column_name in common_columns_list:
            print(common_column_name)
            # filter table df with only column value
            common_column_df = common_table_df[common_table_df[common_table_df.columns[9]] == common_column_name]
            print(common_column_df)
            # create json dict with keys of field name and values for the same column name entries
            common_json_dict = common_column_df.set_index(common_table_df.columns[0])['Value'].to_dict()
            # Convert the dictionary to a JSON string
            common_json_string = json.dumps(common_json_dict)
            print(common_json_string)
            print(years)
            for year in years:
                if year is None or year == '':
                    continue
                try:
                    update_database_single_value_AOC(db_config, common_table_name, cin_column_name,
                                                     cin_column_value,
                                                     company_column_name, company_name, common_column_name,
                                                     common_json_string,
                                                     year)
                except Exception as e:
                    print(f"Exception {e} occurred while updating data in dataframe for {common_table_name} "
                          f"with data {common_json_string}")
    print(common_sql_tables_list)
    output_dataframes_list.append(current_year_df)
    output_dataframes_list.append(previous_year_df)
    output_dataframes_list.append(common_df)
    output_dataframes_list.append(financial_parameter_df)
    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for df in output_dataframes_list:
            df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
            row_index += len(df.index) + 2


def aoc_xbrl_db_update(db_config,config_dict,cin,company_name,xml_file_path,file_date):
    try:
        if not os.path.exists(xml_file_path):
            raise FileNotFoundError(f"The Mapping file '{xml_file_path}' is not found.")
        try:
            xml_tree = Et.parse(xml_file_path)
            xml_root = xml_tree.getroot()
            # xml_str = Et.tostring(xml_root, encoding='unicode')
        except Exception as e:
            raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))
        nodes_list = str(config_dict['Consolidated_nodes']).split()
        parent_node = nodes_list[0]
        child_node = nodes_list[1]
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        update_query_xbrl = """UPDATE documents
                                                SET form_data_extraction_needed = 'Y'
                                                WHERE document_date_year = %s AND
                                                      category = 'other attachments' AND
                                                      document LIKE '%XBRL financial statements%' AND
                                                      cin = %s AND company = %s"""
        values_xbrl = (file_date,cin,company_name)
        print(update_query_xbrl % values_xbrl)
        cursor.execute(update_query_xbrl,values_xbrl)
        connection.commit()
        consolidated_status = get_single_value_from_xml(xml_root,parent_node,child_node)
        if consolidated_status == 'Yes':
            update_query_consolidated = """UPDATE documents
                                        SET form_data_extraction_needed = 'Y'
                                        WHERE document_date_year = %s AND
                                              category = 'other attachments' AND
                                              document LIKE '%XBRL document in respect Consolidated%' AND
                                              cin = %s AND company = %s"""
            values = (file_date,cin,company_name)
            print(update_query_consolidated % values)
            cursor.execute(update_query_consolidated,values)
            connection.commit()
            cursor.close()
            connection.close()
    except Exception as e:
        print(f"Exception occured while inserting into db for xbrl")
        return False
    else:
        return True
# config_excel_path = r"C:\MCA Portal\Config.xlsx"
# config_sheet_name = 'AOC XBRL'
# config_dict_xbrl,status = create_main_config_dictionary(config_excel_path,config_sheet_name)
# map_file_path = config_dict_xbrl['mapping file path']
# map_sheet_name = config_dict_xbrl['mapping file sheet name']
# json_file_path = "all_data.json"
# output_file_path = "XBRL.xlsx"
# db_config = {
#     "host": "162.241.123.123",
#     "user": "classle3_deal_saas",
#     "password": "o2i=hi,64u*I",
#     "database": "classle3_mns_credit",
# }
# cin = 'U01210MH1999PTC119449'
# company = 'DHUMAL INDUSTRIES INDIA PRIVATE LIMITED'
# AOC_XBRL_JSON_to_db(db_config,config_dict_xbrl,map_file_path,map_sheet_name,json_file_path,output_file_path,cin,company)