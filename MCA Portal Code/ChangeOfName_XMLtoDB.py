import pandas as pd
import xml.etree.ElementTree as Et
import os
import json
import mysql.connector
from Config import create_main_config_dictionary
from DBFunctions import update_database_single_value
from word2number import w2n

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
        print(f"An error occurred: {e}")
        return None


def ChangeOfName_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,output_file_path, cin_column_value, company_name):
    Cin_Column_Name = config_dict['cin_column_name_in_db']
    Company_column_name = config_dict['company_name_column_name_in_db']
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
    results = []
    for index,row in single_df.iterrows():
        field_name = str(row.iloc[0]).strip()
        parent_node = str(row.iloc[3]).strip()
        child_nodes = str(row.iloc[4]).strip()
        sql_table_name = str(row.iloc[5]).strip()
        column_name = str(row.iloc[6]).strip()
        column_json_node = str(row.iloc[7]).strip()
        value = get_single_value_from_xml(xml_root,parent_node,child_nodes)
        try:
            number = w2n.word_to_num(value)
            single_df.at[index, 'Value'] = number
        except ValueError:
            single_df.at[index, 'Value'] = value
        results.append([field_name, value, sql_table_name, column_name, column_json_node])

    sql_tables_list = single_df[single_df.columns[5]].unique()
    for sql_table in sql_tables_list:
        table_df = single_df[single_df[single_df.columns[5]] == sql_table]
        column_list = table_df[table_df.columns[6]].unique()
        for column in column_list:
            column_df = table_df[table_df[table_df.columns[6]] == column]
            json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
            json_string = json.dumps(json_dict)
            print(json_string)
            try:
                update_database_single_value(db_config, sql_table, Cin_Column_Name, cin_column_value,
                                             Company_column_name, company_name, column, json_string)
            except Exception as e:
                print(f"Exception {e} occurred while updating data in dataframe for {sql_table_name} "
                      f"with data {json_string}")
    output_dataframes_list.append(single_df)
    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()



