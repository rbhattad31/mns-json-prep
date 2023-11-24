import requests
import json
import mysql.connector
from Config import create_main_config_dictionary
import os
import pandas as pd
import re
import sys
import traceback
def update_database_single_value_GST(db_config, table_name, cin_column_name, cin_value,company_name_column_name,company_name, column_name, column_value,gst_number):
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
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and {}='{}'".format(table_name, cin_column_name, cin_value,company_name_column_name,company_name,'gstin',gst_number)
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
                                                                                      'gstin',
                                                                                      gst_number)
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

def insert_gst_number(db_config,config_dict,cin,company):
    try:
        url = config_dict['pan_to_gst_url']
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        pan_number_query = "select pan from Company where cin=%s"
        values = (cin,)
        cursor.execute(pan_number_query,values)
        pan_number = cursor.fetchone()[0]
        print(pan_number)
        payload = json.dumps({
            "panNumber": pan_number
        })
        headers = {
            'Subscriptionkey': config_dict['api_subscription_key'],
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            json_response = response.json()
            print(json_response)
            result = json_response['result']
            print(result)
            output_df = []
            for gst_details in result:
                gst_number = gst_details[config_dict['gst_key_response']]
                gst_status = gst_details[config_dict['status_key_response']]
                if gst_status == config_dict['status_active_keyword']:
                    print(gst_number, gst_status)
                    df_map = fetch_gst_details(config_dict,gst_number,gst_status)
                    output_df.append(df_map)
            output_file_path = r'C:\MCA Portal\gst.xlsx'
            cin_column_name = config_dict['cin_column_name']
            company_column_name = config_dict['company_column_name']
            for df in output_df:
                gstin = df[df['Field_name'] == 'gstin']['Value'].values[0]
                tables_list = df[df.columns[2]].unique()
                for table in tables_list:
                    table_df = df[df[df.columns[2]] == table]
                    columns_list = table_df[table_df.columns[3]].unique()
                    for column in columns_list:
                        column_df = table_df[table_df[table_df.columns[3]] == column]
                        json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                        # Convert the dictionary to a JSON string
                        json_string = json.dumps(json_dict)
                        update_database_single_value_GST(db_config,table,cin_column_name,cin,company_column_name,company,column,json_string,gstin)
            with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
                row_index = 0
                for df in output_df:
                    df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                    row_index += len(df.index) + 2

        else:
            raise Exception("Not able to connect to API")
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error in fetching GST number from API {e}")
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # Print the traceback details
        for line in traceback_details:
            print(line.strip())
        return False
    else:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        update_query = "update orders set gst_status='Y' where cin = %s"
        values_cin = (cin,)
        print(update_query % values_cin)
        cursor.execute(update_query,values_cin)
        connection.commit()
        cursor.close()
        connection.close()
        return True

def fetch_gst_details(config_dict,gst_number,status):
    try:
        map_file_path = config_dict['map_file_path']
        map_file_sheet_name = config_dict['map_file_sheet_name']
        if not os.path.exists(map_file_path):
            raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")
        try:
            df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
            # print(df_map)
        except Exception as e:
            raise Exception("Below exception occurred while reading mapping file " + '\n' + str(e))
        df_map['Value'] = None
        url = config_dict['gst_url']
        payload = json.dumps({
            "gstin": gst_number
        })
        headers = {
            'Subscriptionkey': config_dict['api_subscription_key'],
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            gst_json_response = response.json()
            gst_details_result = gst_json_response[config_dict['gst_result_keyword']][config_dict['gst_result_keyword']][config_dict['gstn_detailed_keyword']]
            for index,row in df_map.iterrows():
                field_name = str(row.iloc[0]).strip()
                json_node = str(row.iloc[1]).strip()
                table = str(row.iloc[2]).strip()
                column = str(row.iloc[3]).strip()
                if field_name == config_dict['filings_keyword']:
                    value = gst_json_response[config_dict['gst_result_keyword']][config_dict['gst_result_keyword']][json_node]
                    value = json.dumps(value)
                    value = value.replace("'", '"')
                elif field_name == config_dict['gstin_keyword']:
                    value = gst_number
                elif field_name == config_dict['status_keyword']:
                    value = status
                else:
                    value = gst_details_result[json_node]
                    if field_name == config_dict['state_keyword'] or field_name == config_dict['state_jurisdiction_keyword']:
                        pattern = re.compile(r'STATE - ([\w\s]+)(?:,|$)')
                        # Use the findall method to extract the state information
                        matches = pattern.findall(value)
                        # Print the result
                        if matches:
                            value = matches[0]
                    elif field_name == config_dict['centre_jurisdiction_keyword']:
                        pattern = re.compile(r'COMMISSIONERATE - (\w+)')
                        # Use the findall method to extract the state information
                        matches = pattern.findall(value)
                        # Print the result
                        if matches:
                            value = matches[0]
                    elif field_name == config_dict['nature_of_business_activities_keyword']:
                        value = '\n'.join(value)
                df_map.at[index, 'Value'] = value
            return df_map
    except Exception as e:
        print(f"Error in fetching GST Details {e}")


# db_config = {
#         "host": "162.241.123.123",
#         "user": "classle3_deal_saas",
#         "password": "o2i=hi,64u*I",
#         "database": "classle3_mns_credit",
#     }
# excel_path = r"C:\MCA Portal\Config.xlsx"
# sheet_name = 'GST'
# config_dict,status = create_main_config_dictionary(excel_path,sheet_name)
# insert_gst_number(db_config,config_dict,'U45201RJ2014PTC044956','JCC INFRAPROJECTS PRIVATE LIMITED')