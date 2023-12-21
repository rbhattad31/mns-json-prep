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
import logging
from logging_config import setup_logging


def extract_between_phrases(input_sentence, start_phrase, end_phrase):
    pattern = re.compile(f'{re.escape(start_phrase)}(.*?){re.escape(end_phrase)}', re.DOTALL)
    match = pattern.search(input_sentence)

    if match:
        return match.group(1).strip()
    else:
        return None

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
        logging.info(f"Below error occurred for processing parent node: {parent_node} and child node: {child_node}"
              f"\n {e}")
        return None

def JSONtoDB_AOC_XBRL_straight(Cin,CompanyName,json_file_path,target_header):
    years = []
    setup_logging()
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
                        #logging.info("Going Straight")
                        #current_year_value = row[1]
                        #logging.info(current_year_value)
                        row_values = [value for i, value in enumerate(row) if (
                                (isinstance(row[i], (float, int)) and not math.isnan(row[i])) or (
                                isinstance(row[i], str) and row[i] != "NaN"))]
                        current_year_value = row_values[1]
                        try:
                            previous_year_value = row_values[2]
                        except Exception as e:
                            logging.info(f"Exception in straight values {e} for {target_header}")
                            previous_year_value = 0
                        # if len(row) > 2 and ((isinstance(row[2], (float, int)) and not math.isnan(row[2])) or (isinstance(row[2], str) and row[2] != "NaN")):
                        #     previous_year_value = row[2]
                        #     logging.info(previous_year_value)
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
                    #     logging.info(current_year_value)
                    #     if len(row) > 3 and row[3] is not None:
                    #         previous_year_value = row[3]
                    #         logging.info(previous_year_value)
                    #     current_year_value = re.sub(r'\([^)]*\)', '', current_year_value)
                    #     previous_year_value = re.sub(r'\([^)]*\)', '', previous_year_value)
                    #     values.append(current_year_value)
                    #     values.append(previous_year_value)
                    # elif len(row) >= 4 and row[0] == target_header and ((isinstance(row[1], (float, int)) and math.isnan(row[1])) or (isinstance(row[2], str) and row[1] == "NaN")) and ((isinstance(row[2], (float, int)) and not math.isnan(row[2])) or (isinstance(row[2], str) and row[2] != "NaN")):
                    #     current_year_value = row[2]
                    #     logging.info(current_year_value)
                    #     if len(row) > 4 and row[4] is not None:
                    #         previous_year_value = row[4]
                    #         logging.info(previous_year_value)
                    #     current_year_value= re.sub(r'\([^)]*\)', '', current_year_value)
                    #     previous_year_value = re.sub(r'\([^)]*\)', '', previous_year_value)
                    #     values.append(current_year_value)
                    #     values.append(previous_year_value)
                    else:
                        value_previous_year = None
                        value_current_year = None
                        #logging.info("Going for breaks")
                        if not header_found:
                            if row[0] == target_header:
                                header_found = True
                            else:
                                continue
                        if header is None and any(math.isnan(x) for x in row[1:] if isinstance(x, (float, int))):
                            header = row[0]
                            #logging.info(header)
                        elif header is not None and isinstance(row[0], (float, int)) and math.isnan(row[0]):
                            row_values = [value for i, value in enumerate(row) if (
                                        (isinstance(row[i], (float, int)) and not math.isnan(row[i])) or (
                                            isinstance(row[i], str) and row[i] != "NaN"))]
                            #logging.info(row_values)
                            value_current_year = row_values[0]
                            value_previous_year = row_values[1]
                            value_current_year = re.sub(r'\([^)]*\)', '', str(value_current_year))
                            value_previous_year = re.sub(r'\([^)]*\)', '', str(value_previous_year))
                            # if row[1] is not None:
                            #     value_current_year = row[1]
                            #     logging.info(value_current_year)
                            #     if len(row) > 2 and row[2] is not None:
                            #         value_previous_year = row[2]
                            #         logging.info(previous_year_value)
                            #     value_current_year = re.sub(r'\([^)]*\)', '', value_current_year)
                            #     value_previous_year = re.sub(r'\([^)]*\)', '', value_previous_year)
                            # elif row[2] is not None:
                            #     value_current_year = row[2]
                            #     logging.info(value_current_year)
                            #     if len(row) > 3 and row[3] is not None:
                            #         value_previous_year = row[3]
                            #         logging.info(previous_year_value)
                            #     value_current_year = re.sub(r'\([^)]*\)', '', value_current_year)
                            #     value_previous_year = re.sub(r'\([^)]*\)', '', value_previous_year)
                            # elif row[3] is not None:
                            #     value_current_year = row[3]
                            #     logging.info(value_current_year)
                            #     if len(row) > 4 and row[4] is not None:
                            #         value_previous_year = row[4]
                            #         logging.info(previous_year_value)
                            #     value_current_year = re.sub(r'\([^)]*\)', '', value_current_year)
                            #     value_previous_year = re.sub(r'\([^)]*\)', '', value_previous_year)
                            # else:

                            value_found = True
                            values.append(value_current_year)
                            values.append(value_previous_year)
                            header_found = False
                        else:
                            logging.info("Breaks header not found")
                            continue
                except Exception as e:
                    logging.info(f"Main Exception in straight values {e} for {target_header}")
                    exc_type, exc_value, exc_traceback = sys.exc_info()

                    # Get the formatted traceback as a string
                    traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

                    # logging.info the traceback details
                    for line in traceback_details:
                        logging.info(line.strip())
                    continue
                if value_found:
                    break
            if value_found:
                break

    # logging.info the results
    # if current_year_value is not None:
    #     logging.info(f"Current Year Value for '{target_header}': {current_year_value}")
    #     if previous_year_value is not None:
    #         logging.info(f"Previous Year Value for '{target_header}': {previous_year_value}")
    #     else:
    #         logging.info(f"Previous Year Value for '{target_header}': Not available")
    # else:
    #     logging.info(f"'{target_header}' not found in any table.")

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
    # logging.info the extracted years
    # logging.info(years[0])
    # logging.info(values[0])
    return values
    # for insert_data in zip(years,values):
    #     year = insert_data[0]
    #     value = insert_data[1]
    #     logging.info(year,value)

# json_file_path = "all_data.json"
# target_header = "Turnover of highest contributing product or service"
#  JSONtoDB_AOC_XBRL_straight(None,None,json_file_path,target_header)


def JSON_Fields_for_breaks(json_file_path,input_header):
    setup_logging()
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
                #logging.info(row)
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
            # logging.info("Header:", header)
            # logging.info("Value:", value)
            return values
        else:
            logging.info("No NaN value found below the header.")
            return []
    else:
        logging.info("Header not found in the JSON structure.")
        return []


def capture_values_from_text(json_file_path,input_sentence,field_name,nature):
    setup_logging()
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
                # keyword_position = complete_sentence.find(input_sentence)
                # sentence_part = complete_sentence[keyword_position + len(input_sentence):]
                # # Split the sentence part into words and capture the next two words
                # words = re.findall(r'\S+', sentence_part)
                # if len(words) >= 2:
                #     next_two_words = " ".join(words[:2])
                #     logging.info(f"Next Two Words: {next_two_words}")
                #     if 'not' in next_two_words.lower():
                #         next_two_words = 'Not Applicable'
                #     elif 'not' not in next_two_words.lower():
                #         next_two_words = 'Applicable'
                #     else:
                #         pass
                #     return next_two_words
                print(nature)
                print(field_name)
                if nature == 'Standalone':
                    end_sentence = 'Disclosure in auditors report relating to public offer and term loans'
                    companies_caro_value = extract_between_phrases(complete_sentence,input_sentence,end_sentence)
                    print(companies_caro_value)
                else:
                    companies_caro_value = None
                return companies_caro_value
        else:
            logging.info("Word not found")
    else:
        return complete_sentence


def Auditor_information(sentence, keyword):
    setup_logging()
    keyword_position = sentence.find(keyword)
    if keyword == 'contains adverse remarks':
        logging.info("Contains adverse remarks")
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

def update_database_single_value_AOC(db_config, table_name, cin_column_name, cin_value,company_name_column_name,company_name, column_name, column_value,year,nature,filing_standard):
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
        if column_name == 'financials_pnl_revenue_breakup' and filing_standard == 'IND_AS_Taxanomy':
            column_value = json.dumps(json_dict)
        else:
            first_key = next(iter(json_dict))
            first_value = json_dict[first_key]
            column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and {}='{}' and {}='{}'".format(table_name, cin_column_name, cin_value,company_name_column_name,company_name,'year',year,'nature',nature)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0 and column_name!='nature':
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {}='{}' AND {}='{}'".format(table_name, column_name,
                                                                                      column_value, cin_column_name,
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
    elif column_name == 'nature':
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {}='{}' AND nature is null".format(
            table_name, column_name,
            column_value, cin_column_name,
            cin_value,
            company_name_column_name,
            company_name,
            'Year',
            year)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating nature")
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      column_value)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()
def AOC_XBRL_JSON_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, json_file_path, output_file_path,cin_column_value, company_name,aoc_xbrl_first_file_found):
    try:
        setup_logging()
        output_dataframes_list = []
        cin_column_name = config_dict['cin_column_name_in_db']
        company_column_name = config_dict['company_name_column_name_in_db']
        if not os.path.exists(map_file_path):
            raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

        try:
            df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
            # logging.info(df_map)
        except Exception as e:
            raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))

        # adding an empty column to mapping df
        df_map['Value'] = None
        filing_standard_check = capture_values_from_text(json_file_path, None, None,None)
        filing_standard = None
        if str(config_dict['IND_Taxonomy_Condition']) in filing_standard_check:
            filing_standard = config_dict['IND_Taxonomy_Keyword']
        elif str(config_dict['AS_Taxonomy_Condition']) in filing_standard_check:
            filing_standard = config_dict['Taxonomy_Keyword']
        else:
            filing_standard = None
        logging.info(filing_standard)
        # creating new filtered dataframes for single and group values
        single_df = df_map[(df_map[df_map.columns[2]] == config_dict['single_type_indicator']) & ((df_map[df_map.columns[7]] == filing_standard) | (df_map[df_map.columns[7]] == 'Common'))]

        if not os.path.exists(json_file_path):
            raise FileNotFoundError(f"The XML file '{json_file_path}' is not found.")
        nature = None
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
                logging.info(f"{child_nodes}:{values}")
                million_keyword = 'Millions of INR'
                crores_keyword = 'Crores of INR'
                lakh_keyword = 'Lakhs of INR'
                billion_keyword = 'Billions of INR'
                trillion_keyword = 'Trillions of INR'
                if len(values) != 0:
                    if year_category == 'Previous':
                        try:
                            values[1]=values[1].replace(',','')
                            if million_keyword in filing_standard_check:
                                num_value = (float(values[1]))*1000000
                                logging.info("In Millions")
                            elif crores_keyword in filing_standard_check:
                                logging.info("In Crores")
                                num_value = (float(values[1]))*10000000
                            elif lakh_keyword in filing_standard_check:
                                logging.info("In lakhs")
                                num_value = (float(values[1]))*100000
                            elif billion_keyword in filing_standard_check:
                                logging.info("In Billion")
                                num_value = (float(values[1]))*1000000000
                            elif trillion_keyword in filing_standard_check:
                                logging.info("In trillion")
                                num_value = (float(values[1]))*1000000000000
                            else:
                                logging.info("Normal Value")
                                num_value = float(values[1])
                            single_df.at[index, 'Value'] = round(num_value,2)
                        except Exception as e:
                            single_df.at[index, 'Value'] = values[1]
                    elif year_category == 'Current':
                        try:
                            values[0]=values[0].replace(',','')
                            if million_keyword in filing_standard_check:
                                logging.info("In Millions")
                                num_value = (float(values[0]))*1000000
                            elif crores_keyword in filing_standard_check:
                                logging.info("In Crores")
                                num_value = (float(values[0]))*10000000
                            elif lakh_keyword in filing_standard_check:
                                logging.info("In lakhs")
                                num_value = (float(values[0]))*100000
                            elif billion_keyword in filing_standard_check:
                                logging.info("In Billion")
                                num_value = (float(values[0]))*1000000000
                            elif trillion_keyword in filing_standard_check:
                                logging.info("In trillion")
                                num_value = (float(values[0]))*1000000000000
                            else:
                                logging.info("Normal Value")
                                num_value = float(values[0])
                            single_df.at[index, 'Value'] = round(num_value,2)
                        except Exception as e:
                            single_df.at[index,'Value'] = values[0]
                    elif year_category == config_dict['Financial_Parameter_Keyword']:
                        if field_name == 'proposed_dividend':
                            if values[0] == 0 or values[0] == 0.00 or values[0] == '0' or values[0] == '0.00' or values[0] == 0.0 or values[0] == '0.0':
                                dividend_value = 'No'
                            else:
                                dividend_value = 'Yes'
                            single_df.at[index, 'Value'] = dividend_value
                        elif field_name == 'companies_caro_applicable':
                            companies_value = capture_values_from_text(json_file_path,child_nodes,field_name,nature)
                            single_df.at[index, 'Value'] = companies_value
                        else:
                            try:
                                if million_keyword in filing_standard_check:
                                    logging.info("In Millions")
                                    num_value = (float(values[0])) * 1000000
                                elif crores_keyword in filing_standard_check:
                                    logging.info("In Crores")
                                    num_value = (float(values[0])) * 10000000
                                elif lakh_keyword in filing_standard_check:
                                    logging.info("In lakhs")
                                    num_value = (float(values[0])) * 100000
                                elif billion_keyword in filing_standard_check:
                                    logging.info("In Billion")
                                    num_value = (float(values[0])) * 1000000000
                                elif trillion_keyword in filing_standard_check:
                                    logging.info("In trillion")
                                    num_value = (float(values[0])) * 1000000000000
                                else:
                                    logging.info("Normal Value")
                                    num_value = float(values[0])
                                values[0]=values[0].replace(',','')
                                single_df.at[index, 'Value'] = round(num_value,2)
                            except Exception as e:
                                single_df.at[index,'Value'] = values[0]
                    else:
                        single_df.at[index, 'Value'] = None
            elif parent_node == config_dict['Constant_Keyword']:
                if field_name == 'filing_standard':
                    value = filing_standard
                else:
                    value = child_nodes
                single_df.at[index, 'Value'] = value
            elif parent_node == config_dict['Description_Keyword']:
                words_value = capture_values_from_text(json_file_path,child_nodes,field_name,nature)
                single_df.at[index, 'Value'] = words_value
            elif parent_node == config_dict['Auditor_Keyword']:
                auditor_complete_sentence = capture_values_from_text(json_file_path, None, None,nature)
                auditor_value = Auditor_information(auditor_complete_sentence,child_nodes)
                logging.info(f"{field_name}:{auditor_value}")
                single_df.at[index, 'Value'] = auditor_value
        logging.info(single_df)
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
            logging.info(current_formula)
            try:
                # logging.info(f'{current_formula=}')
                if 'None' in current_formula:
                    current_formula = current_formula.replace('None', '0')
                    # logging.info(f'{current_formula=}')
                if 'nan' in current_formula:
                    current_formula = current_formula.replace('nan', '0')
                # Calculate the value using the provided formula and insert it
                current_year_df.at[
                    current_year_df[current_year_df['Field_Name'] == current_formula_field_name].index[
                        0], 'Value'] = round(eval(current_formula),2)
            except Exception as e:
                logging.info(f"Exception occured for current year formula {current_formula_field_name}: {current_formula} {e}")
            except (NameError, SyntaxError):
                # Handle the case where the formula is invalid or contains a missing field name
                logging.info(f"Invalid formula for {current_formula_field_name}: {current_formula}")
        for _, row in previous_year_formula_df.iterrows():
            previous_formula = row['Child_Nodes']
            previous_formula_field_name = row['Field_Name']
            for previous_field_name in previous_year_df['Field_Name']:
                previous_pattern = r'\b' + re.escape(previous_field_name) + r'\b'
                previous_formula = re.sub(previous_pattern, str(
                    previous_year_df[previous_year_df['Field_Name'] == previous_field_name]['Value'].values[0]).replace(',',''),
                                          previous_formula)
            previous_formula = re.sub(r'\([^)]*\)', '', previous_formula)
            logging.info(previous_formula)
            try:
                # logging.info(f'{previous_formula=}')
                if 'None' in previous_formula:
                    previous_formula = previous_formula.replace('None', '0')
                    # logging.info(f'{previous_formula=}')
                if 'nan' in previous_formula:
                    previous_formula = previous_formula.replace('nan', '0')
                # Calculate the value using the provided formula and insert it
                previous_year_df.at[
                    previous_year_df[previous_year_df['Field_Name'] == previous_formula_field_name].index[
                        0], 'Value'] = round(eval(previous_formula),2)
            except Exception as e:
                logging.info(f"Exception occured for Previous year formula {previous_formula_field_name}: {previous_formula} {e}")
            except (NameError, SyntaxError):
                # Handle the case where the formula is invalid or contains a missing field name
                logging.info(f"Exception occurred while processing previous year data - \n"
                      f" Invalid formula for {previous_formula_field_name}: {previous_formula}")
        years = []
        natures = []
        current_year = current_year_df[current_year_df['Field_Name'] == 'year']['Value'].values[0]
        previous_year = previous_year_df[previous_year_df['Field_Name'] == 'year']['Value'].values[0]
        current_year_nature = current_year_df[current_year_df['Field_Name'] == 'nature']['Value'].values[0]
        previous_year_nature = previous_year_df[previous_year_df['Field_Name'] == 'nature']['Value'].values[0]
        years.append(current_year)
        years.append(previous_year)
        natures.append(current_year_nature)
        natures.append(previous_year_nature)
        if current_year is None:
            raise Exception(f"Exception occurred while extracting year value {current_year} from current year data")
        if previous_year is None:
            raise Exception(f"Exception occurred while extracting year value {previous_year} from previous year data")
        single_df_list = []
        if not aoc_xbrl_first_file_found:
            single_df_list.append(current_year_df)
        single_df_list.append(previous_year_df)
        single_df_list.append(financial_parameter_df)
        for df in single_df_list:
            logging.info(df)
            sql_tables_list = df[df.columns[8]].unique()
            logging.info(sql_tables_list)
            year_value = df[df['Field_Name'] == 'year']['Value'].values[0]
            nature_value = df[df['Field_Name'] == 'nature']['Value'].values[0]
            logging.info(year_value)
            logging.info(nature_value)
            for table_name in sql_tables_list:
                table_df = df[df[df.columns[8]] == table_name]
                columns_list = table_df[table_df.columns[9]].unique()
                logging.info(columns_list)
                for column_name in columns_list:
                    logging.info(column_name)
                    # filter table df with only column value
                    column_df = table_df[table_df[table_df.columns[9]] == column_name]
                    logging.info(column_df)
                    # create json dict with keys of field name and values for the same column name entries
                    json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                    # Convert the dictionary to a JSON string
                    json_string = json.dumps(json_dict)
                    logging.info(json_string)
                    try:
                        update_database_single_value_AOC(db_config, table_name, cin_column_name, cin_column_value,
                                                         company_column_name, company_name, column_name, json_string,
                                                         year_value,nature_value,filing_standard)
                    except Exception as e:
                        logging.info(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                              f"with data {json_string}")
        common_sql_tables_list = common_df[common_df.columns[8]].unique()
        if aoc_xbrl_first_file_found:
            years = years[1:]
        if aoc_xbrl_first_file_found:
            natures = natures[1:]
        for common_table_name in common_sql_tables_list:
            logging.info(common_table_name)
            if common_table_name != config_dict['financials_table_name']:
                logging.info("Continuing table")
                continue
            common_table_df = common_df[common_df[common_df.columns[8]] == common_table_name]
            logging.info(common_table_df)
            common_columns_list = common_table_df[common_table_df.columns[9]].unique()
            logging.info(common_columns_list)
            for common_column_name in common_columns_list:
                logging.info(common_column_name)
                if common_column_name != config_dict['auditor_comments_column_name']:
                    logging.info("continuing column")
                    continue
                logging.info(common_column_name)
                # filter table df with only column value
                common_column_df = common_table_df[common_table_df[common_table_df.columns[9]] == common_column_name]
                logging.info(common_column_df)
                if common_column_name == config_dict['auditor_comments_column_name']:
                    auditor_comments_row_index = common_column_df[common_column_df[common_column_df.columns[9]] ==
                                                       config_dict['auditor_comments_column_name']].index[0]
                    if auditor_comments_row_index is not None:
                        comment_value = common_column_df.loc[auditor_comments_row_index, 'Value']
                        logging.info(f'{comment_value=}')
                        if comment_value == 'No':
                            report_value = '''As per Auditors Report, the accounts give a true and fair view, as per the accounting principles generally accepted, of the
                                              state of affairs in the case of Balance sheet and, Profit or Loss in the case of Profit & Loss Accounts. Auditors Report is
                                              Unqualified i.e. Clean'''
                            logging.info(report_value)
                            auditor_report_row_index = common_table_df[common_table_df[common_table_df.columns[9]] ==
                                                                       config_dict[
                                                                           'disclosures_auditor_report_column_name']].index[0]
                            director_report_row_index = common_table_df[common_table_df[common_table_df.columns[9]] ==
                                                                        config_dict[
                                                                            'disclosures_director_report_column_name']].index[0]
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
        for common_table_name in common_sql_tables_list:
            common_table_df = common_df[common_df[common_df.columns[8]] == common_table_name]
            common_columns_list = common_table_df[common_table_df.columns[9]].unique()
            logging.info(common_columns_list)
            for common_column_name in common_columns_list:
                logging.info(common_column_name)
                # filter table df with only column value
                common_column_df = common_table_df[common_table_df[common_table_df.columns[9]] == common_column_name]
                logging.info(common_column_df)
                common_json_dict = common_column_df.set_index(common_table_df.columns[0])['Value'].to_dict()
                common_json_string = json.dumps(common_json_dict)
                common_json_string = common_json_string.replace('\n', ' ')
                logging.info(common_json_string)
                logging.info(years)
                for year,nature in zip(years,natures):
                    if year is None or year == '':
                        continue
                    try:
                        update_database_single_value_AOC(db_config, common_table_name, cin_column_name,
                                                         cin_column_value,
                                                         company_column_name, company_name, common_column_name,
                                                         common_json_string,
                                                         year,nature,filing_standard)
                    except Exception as e:
                        logging.info(f"Exception {e} occurred while updating data in dataframe for {common_table_name} "
                              f"with data {common_json_string}")
        logging.info(common_sql_tables_list)
        output_dataframes_list.append(current_year_df)
        output_dataframes_list.append(previous_year_df)
        output_dataframes_list.append(common_df)
        output_dataframes_list.append(financial_parameter_df)
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            for df in output_dataframes_list:
                df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                row_index += len(df.index) + 2
    except Exception as e:
        logging.info(f"Exception occured while inserting into DB for AOC XBRL")
        return False
    else:
        return True


def aoc_xbrl_db_update(db_config,config_dict,cin,company_name,xml_file_path,file_date):
    try:
        setup_logging()
        if not os.path.exists(xml_file_path):
            raise FileNotFoundError(f"The Mapping file '{xml_file_path}' is not found.")
        try:
            xml_tree = Et.parse(xml_file_path)
            xml_root = xml_tree.getroot()
            # xml_str = Et.tostring(xml_root, encoding='unicode')
        except Exception as e:
            raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))
        nodes_list = str(config_dict['Consolidated_nodes']).split(',')
        parent_node = nodes_list[0]
        child_node = nodes_list[1]
        # values_xbrl = (file_date, cin, company_name)
        # values = (cin, company_name)
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = "select * from documents where cin=%s and document like '%%XBRL financial statements%%'  and Category = 'Other Attachments' and document_date_year=%s"
        values = (cin, file_date)
        print(query % values)
        cursor.execute(query, values)
        result = cursor.fetchall()
        print(result)
        if len(result) != 0:
            update_query_xbrl = "update documents set form_data_extraction_needed = 'Y' where cin=%s and document_date_year = %s and document like '%%XBRL financial statements%%'  and Category = 'Other Attachments'"
            values = (cin, file_date)
            print(update_query_xbrl % values)
            cursor.execute(update_query_xbrl, values)
            connection.commit()
        # update_query_xbrl_no = """UPDATE documents SET form_data_extraction_needed = 'N'
        #                                                 WHERE category = 'Other Attachments' AND
        #                                                       document LIKE '%%XBRL financial statements%%' AND
        #                                                       cin = %s AND company = %s"""
        # logging.info(update_query_xbrl_no % values)
        # cursor.execute(update_query_xbrl_no,values)
        # connection.commit()
        # update_query_xbrl = """UPDATE documents
        #                                         SET form_data_extraction_needed = 'Y'
        #                                         WHERE document_date_year = %s AND
        #                                               category = 'Other Attachments' AND
        #                                               document LIKE '%%XBRL financial statements%%' AND
        #                                               cin = %s AND company = %s"""
        # logging.info(update_query_xbrl % values_xbrl)
        # cursor.execute(update_query_xbrl,values_xbrl)
        # connection.commit()
        consolidated_status = get_single_value_from_xml(xml_root,parent_node,child_node)
        if consolidated_status == 'Yes':
            update_query_consolidated = """UPDATE documents
                                        SET form_data_extraction_needed = 'Y'
                                        WHERE document_date_year = %s AND
                                              category = 'Other Attachments' AND
                                              document LIKE '%%XBRL document in respect Consolidated%%' AND
                                              cin = %s AND company = %s"""
            values = (file_date,cin,company_name)
            logging.info(update_query_consolidated % values)
            cursor.execute(update_query_consolidated,values)
            connection.commit()
            cursor.close()
            connection.close()
        else:
            update_query_consolidated = """UPDATE documents
                                                    SET form_data_extraction_needed = 'N'
                                                    WHERE document_date_year = %s AND
                                                          category = 'Other Attachments' AND
                                                          document LIKE '%%XBRL document in respect Consolidated%%' AND
                                                          cin = %s AND company = %s"""
            values = (file_date, cin, company_name)
            logging.info(update_query_consolidated % values)
            cursor.execute(update_query_consolidated, values)
            connection.commit()
            cursor.close()
            connection.close()
    except Exception as e:
        logging.info(f"Exception occured while inserting into db for xbrl {e}")
        return False
    else:
        return True


# config_excel_path = r"C:\MCA Portal\Config.xlsx"
# config_sheet_name = 'AOC XBRL'
# config_dict_xbrl,status = create_main_config_dictionary(config_excel_path,config_sheet_name)
# map_file_path = config_dict_xbrl['mapping file path']
# map_sheet_name = config_dict_xbrl['mapping file sheet name']
# json_file_path = r"C:\Users\BRADSOL123\Desktop\XBRL financial statements duly authenticated as per section 134 (including Board's report,auditor's report and other documents)-26112019.json"
# output_file_path = r"C:\Users\BRADSOL123\Desktop\XBRL financial statements duly authenticated as per section 134 (including Board's report,auditor's report and other documents)-26112019.xlsx"
# db_config = {
#     "host": "162.241.123.123",
#     "user": "classle3_deal_saas",
#     "password": "o2i=hi,64u*I",
#     "database": "classle3_mns_credit",
# }
# cin = 'U45201RJ2014PTC044956'
# company = 'JCC INFRAPROJECTS PRIVATE LIMITED'
# AOC_XBRL_JSON_to_db(db_config,config_dict_xbrl,map_file_path,map_sheet_name,json_file_path,output_file_path,cin,company,True)