import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import json
import logging
from logging_config import setup_logging
import mysql.connector
from Config import create_main_config_dictionary

def update_database_single_value_form11(db_config, config_dict, table_name, cin_column_name, cin_value,
                                 column_name, column_value,check_value,financial_year):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    financial_year_column_name = config_dict['financial_year_column_name']
    din_column_name = config_dict['din_column_name']
    id_column_name = config_dict['signer_id_column_name']
    nominee_id_column_name = config_dict['nominee_id_column_name']
    category_column_name = config_dict['category_column_name']


    # json_dict = json.loads(column_value)
    # num_elements = len(json_dict)
    #
    # if num_elements == 1:
    #     first_key = next(iter(json_dict))
    #     first_value = json_dict[first_key]
    #     if first_value is None:
    #         print(f"Value is None for column '{column_name}' to update in table '{table_name}'")
    #         db_cursor.close()
    #         db_connection.close()
    #         return
    #     column_value = first_value.replace('"', '\\"').replace("'", "\\'")
    #     # print(column_value)
    # else:
    #     column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    if table_name == config_dict['authorized_signatories_table_name']:
        query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}'".format(table_name, cin_column_name, cin_value,
                                                                        din_column_name, check_value)
    elif table_name == config_dict['individual_partners_table_name']:
        query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}' AND {} = '{}'".format(table_name, cin_column_name,
                                                                        cin_value,
                                                                        id_column_name,
                                                                        check_value,
                                                                        financial_year_column_name,
                                                                        financial_year
                                                                        )
    elif table_name == config_dict['body_corporates_table_name']:
        query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}'".format(table_name, cin_column_name,
                                                                        cin_value,
                                                                        nominee_id_column_name,
                                                                        check_value
                                                                        )
    elif table_name == config_dict['summary_designated_partners_table_name']:
        query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}' AND {}='{}'".format(table_name, cin_column_name,
                                                                        cin_value,
                                                                        category_column_name,
                                                                        check_value,
                                                                        financial_year_column_name,
                                                                        financial_year
                                                                        )
    else:
        print(f"Irrelevant table {table_name} to update data for Form11")
        db_cursor.close()
        db_connection.close()
        return
    print(query)

    try:
        db_cursor.execute(query)
    except mysql.connector.Error as e:
        print(f"Exception {e} occurred while executing db selection query for table {table_name}")
    except Exception as e:
        print(f"Exception {e} occurred while executing db selection query for table {table_name}")

    result = db_cursor.fetchall()
    # print(result)
    print(column_value)
    # if cin value already exists, update
    if len(result) > 0:
        if table_name == config_dict['authorized_signatories_table_name']:
            print(f"cin {cin_value} is exist in table {table_name}")
            update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} ='{}'".format(table_name,
                                                                                         column_name, column_value,
                                                                                         cin_column_name, cin_value,
                                                                                         din_column_name,
                                                                                         check_value
                                                                                         )
        elif table_name == config_dict['individual_partners_table_name']:
            print(f"cin {cin_value} is exist in table {table_name}")
            # print(f"Entry already exist for cin '{cin_value}' with value '{column_value}' for column '{column_name}' "
            #       f"for financial year '{financial_year}', hence updating")
            update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {} = '{}'".format(table_name,
                                                                                          column_name,
                                                                                          column_value,
                                                                                          cin_column_name,
                                                                                          cin_value,
                                                                                          id_column_name,
                                                                                          check_value,
                                                                                          financial_year_column_name,
                                                                                          financial_year
                                                                                          )
        elif table_name == config_dict['body_corporates_table_name']:
            print(f"cin {cin_value} is exist in table {table_name}")
            # print(f"Entry already exist for cin '{cin_value}' with value '{column_value}' for column '{column_name}' "
            #       f"for financial year '{financial_year}', hence updating")
            update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(table_name,
                                                                                          column_name,
                                                                                          column_value,
                                                                                          cin_column_name,
                                                                                          cin_value,
                                                                                          nominee_id_column_name,
                                                                                          check_value
                                                                                          )
        elif table_name == config_dict['summary_designated_partners_table_name']:
            print(f"cin {cin_value} is exist in table {table_name}")
            # print(f"Entry already exist for cin '{cin_value}' with value '{column_value}' for column '{column_name}' "
            #       f"for financial year '{financial_year}', hence updating")
            update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {}='{}'".format(table_name,
                                                                                          column_name,
                                                                                          column_value,
                                                                                          cin_column_name,
                                                                                          cin_value,
                                                                                          category_column_name,
                                                                                          check_value,
                                                                                          financial_year_column_name,
                                                                                          financial_year
                                                                                          )
        else:
            print(f"Irrelevant table {table_name} to update data for Form11")
            db_cursor.close()
            db_connection.close()
            return
        print(update_query)
        try:
            db_cursor.execute(update_query)
        except Exception as e:
            raise e
        else:
            print(f"updated form 11 data in table {table_name}")
            db_cursor.close()
            db_connection.close()
            return

    # if cin value doesn't exist, insert
    else:
        # print(type(cin_value))
        # print(type(column_value))
        if table_name == config_dict['authorized_signatories_table_name']:
            print(f"cin {cin_value} is not exist in table {table_name}")
            insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name,
                                                                                          cin_column_name,
                                                                                          din_column_name,
                                                                                          column_name,
                                                                                          cin_value,
                                                                                          check_value,
                                                                                          column_value
                                                                                          )
        elif table_name == config_dict['individual_partners_table_name']:
            print(f"cin {cin_value} is not exist in table {table_name}")
            insert_query = "INSERT INTO {} ({}, {}, {},{}) VALUES ('{}', '{}', '{}', '{}')".format(table_name,
                                                                                          cin_column_name,
                                                                                          column_name,
                                                                                          id_column_name,
                                                                                          financial_year_column_name,
                                                                                          cin_value,
                                                                                          column_value,
                                                                                          check_value,
                                                                                          financial_year
                                                                                          )
        elif table_name == config_dict['body_corporates_table_name']:
            print(f"cin {cin_value} is not exist in table {table_name}")
            insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name,
                                                                                          cin_column_name,
                                                                                          column_name,
                                                                                          nominee_id_column_name,
                                                                                          cin_value,
                                                                                          column_value,
                                                                                          check_value
                                                                                          )
        elif table_name == config_dict['summary_designated_partners_table_name']:
            print(f"cin {cin_value} is not exist in table {table_name}")
            insert_query = "INSERT INTO {} ({}, {}, {},{}) VALUES ('{}', '{}', '{}', '{}')".format(table_name,
                                                                                          cin_column_name,
                                                                                          column_name,
                                                                                          category_column_name,
                                                                                          financial_year_column_name,
                                                                                          cin_value,
                                                                                          column_value,
                                                                                          check_value,
                                                                                          financial_year
                                                                                          )
        else:
            db_cursor.close()
            db_connection.close()
            return
        print(insert_query)
        try:
            db_cursor.execute(insert_query)
        except Exception as e:
            raise e
        else:
            print(f"Inserted into table {table_name}")
            db_cursor.close()
            db_connection.close()
            return


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
    formatted_date_string = original_date.strftime("%d/%m/%y")

    return formatted_date_string


def Get_MultipleFields(Data, fieldname):
    field_elements = Data.find_all('field', {'name': fieldname})
    Values = [
    field.find('text').text if field.find('text') else (
        field.find('float').text if field.find('float') else (
            field.find('integer').text if field.find('integer') else None
        )
    )
    for field in field_elements]
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


def form11_hidden_fields(db_config,xml_file_path,map_file_path,config_dict,din_list,cin,id_list,nominee_id_list,financial_year,category_list):
    setup_logging()
    try:
        # Read the XML content from the file
        with open(xml_file_path, 'r') as file:
            xml_content = file.read()

        # Parse the XML content using BeautifulSoup
        soup = BeautifulSoup(xml_content, 'xml')

        cin_column_name = config_dict['cin_column_name']
        din_column_name = config_dict['din_column_name']
        # Read Config File
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name='Sheet1')
        df_map['Value'] = None
        single_df = df_map[df_map[df_map.columns[2]] == 'Hidden']
        for index, row in single_df.iterrows():
            Field = str(row.iloc[0]).strip()
            parent_node = str(row.iloc[3]).strip()
            if Field is not None:
                if Field == 'partner' or Field == 'indian_desig_partner' or Field == 'other_desig_partner' or Field == 'total':
                    category_value_list = []
                    parent_node_list = str(row.iloc[3]).split(',')
                    for node in parent_node_list:
                        value_list = Get_MultipleFields(soup,node)
                        for value in value_list:
                            if Field == 'partner' or Field == 'indian_desig_partner':
                                try:
                                    value = int(float(value))
                                except Exception as e:
                                    pass
                            category_value_list.append(value)
                    print(category_value_list)
                    single_df.at[index, 'Value'] = category_value_list
                elif Field == 'date_of_appointment' or Field == 'date_of_cessation':
                    temp_date_list = []
                    value = Get_MultipleDateFields(soup, parent_node)
                    for date in value:
                        date_value = Convert_HiddenDOBFormat(date)
                        temp_date_list.append(date_value)
                    single_df.at[index, 'Value'] = temp_date_list
                else:
                    value = Get_MultipleFields(soup, parent_node)
                    print(value)
                    single_df.at[index, 'Value'] = value
        sql_tables_list = single_df[single_df.columns[6]].unique()
        logging.info(single_df)
        logging.info(sql_tables_list)
        for table_name in sql_tables_list:
            table_df = single_df[single_df[single_df.columns[6]] == table_name]
            columns_list = table_df[table_df.columns[7]].unique()
            for column_name in columns_list:
                # filter table df with only column value
                column_df = table_df[table_df[table_df.columns[7]] == column_name]
                # create json dict with keys of field name and values for the same column name entries
                json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                # Convert the dictionary to a JSON string
                value_list = json_dict.get(column_name,[])
                if table_name == config_dict['authorized_signatories_table_name']:
                    list = din_list
                elif table_name == config_dict['individual_partners_table_name']:
                    list = id_list
                elif table_name == config_dict['body_corporates_table_name']:
                    list = nominee_id_list
                elif table_name == config_dict['summary_designated_partners_table_name']:
                    list = category_list
                else:
                    list = []
                print(list)
                print(value_list)
                for value_to_insert,check_value in zip(value_list,list):
                    logging.info(check_value,value_to_insert)
                    update_database_single_value_form11(db_config,config_dict,table_name,cin_column_name,cin,column_name,value_to_insert,check_value,financial_year)
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