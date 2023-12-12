import sys
import traceback
import logging
import pandas as pd
import xml.etree.ElementTree as Et
import os
import mysql.connector
from logging_config import setup_logging
pd.set_option('display.max_columns', None)


def extract_table_values_from_xml(xml_root, table_node_name, child_nodes):
    data_list = []
    setup_logging()
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


def insert_datatable_with_table(db_config, sql_table_name, column_names_list, df_row, name_column_name,
                                cin_column_name):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True
    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # logging.info(result_dict)
    name = str(result_dict[name_column_name]).replace('"', '\\"').replace("'", "\\'")
    # logging.info(name)
    cin = result_dict[cin_column_name]
    # logging.info(cin)
    select_query = f"SELECT * FROM {sql_table_name} WHERE {name_column_name} = '{name}' AND {cin_column_name} = '{cin}'"
    logging.info(select_query)
    db_cursor.execute(select_query)
    result = db_cursor.fetchall()
    logging.info(len(result))
    if len(result) > 0:  # If matching record found
        # Insert the record
        result_dict.pop(name_column_name)
        result_dict.pop(cin_column_name)
        # logging.info(result_dict)
        # logging.info(column_names_list)
        column_names_list = list(column_names_list)
        column_names_list.remove(name_column_name)
        column_names_list.remove(cin_column_name)
        # logging.info(column_names_list)
        update_query = f'''UPDATE {sql_table_name}
                        SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])} 
                        WHERE {cin_column_name} = '{cin}' AND {name_column_name} = '{name}' '''
        logging.info(update_query)
        db_cursor.execute(update_query)

        logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
    else:
        logging.info(f"Values for cin '{cin}' with name '{name}' not exists in table '{sql_table_name}'")
    db_cursor.close()
    db_connection.close()


def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
              output_file_path, cin):
    setup_logging()
    config_dict_keys = ['group_type_indicator', 'cin_column_name',
                        'field_name_index', 'type_index',
                        'parent_node_index',
                        'child_nodes_index', 'sql_table_name_index',
                        'column_name_index', 'column_json_node_index',
                        'name_column_name'
                        ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]
    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

    # field_name_index = int(config_dict['field_name_index'])
    single_group_type_index = int(config_dict['type_index'])
    parent_node_index = int(config_dict['parent_node_index'])
    child_nodes_index = int(config_dict['child_nodes_index'])
    sql_table_name_index = int(config_dict['sql_table_name_index'])
    column_name_index = int(config_dict['column_name_index'])
    # column_json_node_index = int(config_dict['column_json_node_index'])

    name_column_name = config_dict['name_column_name']

    if not os.path.exists(map_file_path):
        raise FileNotFoundError(f"The Mapping file '{map_file_path}' is not found.")

    try:
        df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        # logging.info(df_map)
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

    cin_column_name = config_dict['cin_column_name']

    # initializing empty list to save all the result dataframes for saving to output excel
    output_dataframes_list = []

    # extract group values
    for index, row in group_df.iterrows():
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[sql_table_name_index]).strip()
        column_names = str(row.iloc[column_name_index]).strip()
        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]
        logging.info(column_names_list)
        table_node_name = parent_node
        # logging.info(table_node_name)
        try:
            logging.info(table_node_name)
            logging.info(child_nodes)
            table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
            table_df = pd.DataFrame(table_in_list)
            # logging.info(table_df)
            table_df[name_column_name] = (table_df.iloc[:, 0] + ' ' + table_df.iloc[:, 1] + ' ' +
                                          table_df.iloc[:, 2])
            # logging.info(table_df)
            # Drop the first three columns by index
            table_df = table_df.drop(table_df.columns[:3], axis=1)
            # logging.info(table_df)

            # Move the combined column to index 0
            table_df = table_df[[name_column_name] + [col for col in table_df.columns if col != name_column_name]]
            # logging.info(table_df)
            table_df.columns = column_names_list
            # logging.info(table_df)

        except Exception as e:
            logging.error(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
            continue

        # logging.info(table_df)

        table_df[cin_column_name] = cin

        column_names_list.append(cin_column_name)

        table_df.columns = column_names_list
        table_df = table_df[table_df[column_names_list[0]].notna()]
        logging.info(table_df)

        for _, df_row in table_df.iterrows():
            try:
                insert_datatable_with_table(db_config, sql_table_name, table_df.columns, df_row, name_column_name,
                                            cin_column_name)
            except Exception as e:
                logging.info(f"Exception '{e}' occurred while inserting below table row in table {sql_table_name}- \n",
                      df_row)
        logging.info(f"DB execution is complete for {sql_table_name}")
        output_dataframes_list.append(table_df)

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # logging.info(dataframe)
            dataframe.to_excel(writer, sheet_name=config_dict['output_sheet_name'], index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()


def form_fillip_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                          output_file_path, cin):
    try:
        setup_logging()
        xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path, cin)
    except Exception as e:
        logging.error("Below Exception occurred while processing Form Fillip file: \n ", e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.error(line.strip())

        return False
    else:
        return True
