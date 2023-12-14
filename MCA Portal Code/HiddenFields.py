import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import json
import logging
from logging_config import setup_logging
import mysql.connector
from Config import create_main_config_dictionary

def update_database_single_value_DIR(db_config, table_name, cin_column_name, cin_value,column_name, column_value,din_column_name,din):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    # json_dict = json.loads(column_value)
    # num_elements = len(json_dict)
    # # if column_name == "financials_auditor" and num_elements == 1:
    # #     first_key = next(iter(json_dict))
    # #     first_value_json_list = json_dict[first_key]
    # #     json_string = json.dumps(first_value_json_list)
    # #     column_value = json_string
    # if num_elements == 1:
    #     first_key = next(iter(json_dict))
    #     first_value = json_dict[first_key]
    #     column_value = first_value
    # else:
    #     column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}'".format(table_name, cin_column_name, cin_value,din_column_name,din)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0 and column_name!='nature':
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(table_name, column_name,
                                                                                      column_value, cin_column_name,
                                                                                      cin_value,
                                                                                      din_column_name,
                                                                                      din)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, cin_column_name,
                                                                                      din_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      din,
                                                                                        column_value)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def Get_Age(DOB):
    # Given date in the "dd/mm/yyyy" format
    given_date_string = DOB

    # Parse the given date string
    given_date = datetime.strptime(given_date_string, "%d/%m/%Y")

    # Get the current date
    current_date = datetime.now()

    # Calculate the age
    age = current_date.year - given_date.year - (
            (current_date.month, current_date.day) < (given_date.month, given_date.day))
    return age


def Convert_HiddenDOBFormat(DOB):
    # Original date string
    original_date_string = DOB

    # Parse the original date string
    original_date = datetime.strptime(original_date_string, "%Y-%m-%dT%H:%M:%S%z")

    # Format the date in the desired format
    formatted_date_string = original_date.strftime("%d/%m/%Y")

    return formatted_date_string


def Get_MultipleFields(Data, fieldname):
    field_elements = Data.find_all('field', {'name': fieldname})
    Values = [field.find('text').text if field.find('text') else None for field in field_elements]
    filtered_values = [value for value in Values if value is not None]
    return filtered_values


def Get_MultipleDateFields(Data, fieldname):
    field_elements = Data.find_all('field', {'name': fieldname})
    Values = [field.find('date').text if field.find('date') else None for field in field_elements]
    filtered_values = [value for value in Values if value is not None]
    return filtered_values


def Get_HiddenXmlFields(Data, fieldname):
    # Find the field with name="hiName"
    field = Data.find('field', {'name': fieldname})

    # Extract the text value within the <text> tag
    if field:
        value = field.find('text').text
        return value
    else:
        return 'Field with name ' + field + ' not found in file'


def dir_hidden_fields(db_config,xml_file_path,map_file_path,config_dict,din_list,cin):
    setup_logging()
    try:
        # Read the XML content from the file
        with open(xml_file_path, 'r') as file:
            xml_content = file.read()

        # Parse the XML content using BeautifulSoup
        soup = BeautifulSoup(xml_content, 'xml')

        cin_column_name = config_dict['cin_column_name_in_db']
        din_column_name = config_dict['din_column_name_in_db']
        # Read Config File
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name='Sheet1')

        df_map['Value'] = None
        single_df = df_map[df_map[df_map.columns[2]] == 'Single']
        temp_date_list = []
        temp_age_list = []
        for index, row in single_df.iterrows():
            Fields = ['name', 'gender', 'date_of_birth', 'age', 'nationality', 'father_name', 'address']
            Field = str(row.iloc[0]).strip()
            parent_node = str(row.iloc[3]).strip()
            if Field in Fields:
                if Field is not None:
                    if Field in ['date_of_birth', 'nationality','age']:
                        value = Get_MultipleDateFields(soup, parent_node)
                        logging.info(value)
                        if Field == 'date_of_birth':
                            for date in value:
                                value = Convert_HiddenDOBFormat(date)
                                temp_date_list.append(value)
                            value = temp_date_list
                        elif Field == 'age':
                            for date in temp_date_list:
                                age = Get_Age(date)
                                temp_age_list.append(age)
                            value = temp_age_list
                    else:
                        value = Get_MultipleFields(soup, parent_node)
                    single_df.at[index, 'Value'] = value
        sql_tables_list = single_df[single_df.columns[5]].unique()
        logging.info(single_df)
        logging.info(sql_tables_list)
        for table_name in sql_tables_list:
            table_df = single_df[single_df[single_df.columns[5]] == table_name]
            columns_list = table_df[table_df.columns[6]].unique()
            for column_name in columns_list:
                # filter table df with only column value
                column_df = table_df[table_df[table_df.columns[6]] == column_name]
                # create json dict with keys of field name and values for the same column name entries
                json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                # Convert the dictionary to a JSON string
                value_list = json_dict.get(column_name,[])
                for value_to_insert,din in zip(value_list,din_list):
                    logging.info(din,value_to_insert)
                    update_database_single_value_DIR(db_config,table_name,cin_column_name,cin,column_name,value_to_insert,din_column_name,din)
        # Name = Get_MultipleFields(soup, 'Full1_C')
        # logging.info('Name-', str(Name) + '\n')
        # Nationality = Get_MultipleDateFields(soup, 'Nationality1_N')
        # logging.info('Nationality-', str(Nationality) + '\n')
        # DOB = Get_MultipleDateFields(soup, 'Birthdated1_D')
        #
        # for date in DOB:
        #     D_O_B = Convert_HiddenDOBFormat(date)
        #     logging.info('DOB-', str(D_O_B) + '\n')
        #     Age = Get_Age(D_O_B)
        #     logging.info('Age-', str(Age) + '\n')
        # FatherName = Get_MultipleFields(soup, 'Father1_C')
        # logging.info('FatherName-', str(FatherName) + '\n')
        # Address = Get_MultipleFields(soup, 'PresentAdd1_C')
        # logging.info('Address-', str(Address) + '\n')
        # Gender = Get_MultipleFields(soup, 'TextField1')
        # logging.info('Gender-', str(Gender) + '\n')
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))
    # Replace 'your_file.xml' with the actual path to your XML file

# db_config = {
#         "host": "162.241.123.123",
#         "user": "classle3_deal_saas",
#         "password": "o2i=hi,64u*I",
#         "database": "classle3_mns_credit"
#     }
# xml_file_path = r"C:\MCA Portal\L27100MH1907PLC000268\CONTROL RISKS INDIA PRIVATE LIMITED\Change in Directors\Form DIR-12-12032021_signed_hidden.xml"
# map_file_path = r"C:\MCA Portal\DIR-12_nodes_config.xlsx"
# din_list = ['07430352']
# cin = 'L25111MH1988PLC048950'
# excel_file_path = r"C:\MCA Portal\Config.xlsx"
# sheet_name = 'DIR'
# config_dict,config_status = create_main_config_dictionary(excel_file_path,sheet_name)
# dir_hidden_fields(db_config,xml_file_path,map_file_path,config_dict,din_list,cin)