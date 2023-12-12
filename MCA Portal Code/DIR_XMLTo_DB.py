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


def extract_table_values_from_hidden_xml(xml_root, table_node_name, child_nodes):
    setup_logging()
    logging.info(xml_root)
    data_list = []
    child_nodes_list = [x.strip() for x in child_nodes.split(',')]
    logging.info(table_node_name)
    logging.info(child_nodes_list)
    field_values_list = []
    ns = {'ns0': 'http://example.com/ns/form'}
    director1_subform = xml_root.findall(".//ns0:subform[@name='TextField1_C']", namespaces=ns)
    logging.info(director1_subform)
    if director1_subform is not None:
        logging.info("Found")
    # Found subform with name 'Director1'
    # Process it here
    else:
        logging.info("Not found")
    # No subform with name 'Director1' found

    # for director_node in xml_root.findall(f".//subform[@name={table_node_name}]"):
    # for director_node in xml_root.findall(f".//subform[@name='{table_node_name}']"):
    #     logging.info(director_node)
    field_values = {}
    temp_list = []
    field_value = xml_root.find(".//field[@name='Full1_C']")
    logging.info(field_value)
    for field_name in child_nodes_list:
        field_value = xml_root.find(f".//field name='{field_name}'").find('value').find('text').text
        field_values[field_name] = field_value
        temp_list.append(field_value)
    field_values_list.append(field_values)
    data_list.append(temp_list)
    logging.info(field_values_list)
    logging.info(data_list)
    return data_list


def insert_datatable_with_table(config_dict, db_config, sql_table_name, column_names_list, df_row):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # logging.info(result_dict)
    cin_column_name = config_dict['cin_column_name_in_db']
    cin = result_dict[cin_column_name]
    logging.info(f'{cin=}')
    din_column_name = config_dict['din_column_name_in_db']
    din = result_dict[din_column_name]
    logging.info(f'{din=}')

    designation_after_event_column_name = config_dict['designation_after_event_column_name_in_db']
    designation_after_event = result_dict[designation_after_event_column_name]
    logging.info(f'{designation_after_event=}')

    if cin is None or din is None or designation_after_event is None:
        raise Exception(f" One of the Value of CIN, DIN and 'Designation after event' values are empty for director"
                        f" in table {sql_table_name} "
                        f"with below data \n {list(df_row)} ")
    else:
        select_query = (f"SELECT * FROM {sql_table_name} WHERE {cin_column_name} = '{cin}' AND {din_column_name}"
                        f" = '{din}' AND {designation_after_event_column_name} = '{designation_after_event}'")

    logging.info(select_query)
    db_cursor.execute(select_query)
    result = db_cursor.fetchall()
    logging.info(len(result))
    if len(result) == 0:  # If no matching record found
        # Insert the record
        insert_query = f"""
        INSERT INTO {sql_table_name}
        SET {', '.join([f'{col} = %s' for col in column_names_list])};
        """
        logging.info(insert_query)
        logging.info(tuple(df_row.values))
        db_cursor.execute(insert_query, tuple(df_row.values))
        # logging.info(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
    else:
        result_dict.pop(cin_column_name)
        result_dict.pop(din_column_name)
        result_dict.pop(designation_after_event_column_name)

        column_names_list = list(column_names_list)
        column_names_list.remove(cin_column_name)
        column_names_list.remove(din_column_name)
        column_names_list.remove(designation_after_event_column_name)

        update_query = f'''UPDATE {sql_table_name}
                        SET {', '.join([f"{col} = '{str(result_dict[col])}'" for col in column_names_list])} 
                        WHERE {cin_column_name} = '{cin}' AND {din_column_name} = '{din}' AND
                        {designation_after_event_column_name} = '{designation_after_event}' '''

        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")

    db_cursor.close()
    db_connection.close()


def update_attachment_table(db_config, config_dict, sql_table_name, column_names_list, df_row):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # logging.info(result_dict)

    din_column_name = config_dict['din_column_name_in_db']
    din = result_dict[din_column_name]

    pan_column_name = config_dict['pan_column_name_in_db']
    pan = result_dict[pan_column_name]

    cin_column_name = config_dict['cin_column_name_in_db']
    cin = result_dict[cin_column_name]

    if din is None or cin is None:
        raise f"din '{din}' or cin '{cin}' value extracted from xml file is None"
    else:
        query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}' ".format(sql_table_name, din_column_name, din,
                                                                         cin_column_name, cin)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    logging.info(len(result))
    if sql_table_name == 'authorized_signatories':
        phone_number_column_name = config_dict['phone_number_column_name_in_db']
        phone_number = result_dict[phone_number_column_name]

        email_column_name = config_dict['email_column_name_in_db']
        email = result_dict[email_column_name]
        if len(result) > 0:
            if pan is not None:
                try:
                    update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' ".format(sql_table_name,
                                                                                                   pan_column_name,
                                                                                                   pan, din_column_name,
                                                                                                   din, cin_column_name,
                                                                                                   cin
                                                                                                   )

                    logging.info(update_query)
                    db_cursor.execute(update_query)
                except Exception as e:
                    logging.info(e)
            if phone_number is not None:
                try:
                    update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(
                        sql_table_name,
                        phone_number_column_name,
                        phone_number,
                        din_column_name,
                        din, cin_column_name,
                        cin)
                    logging.info(update_query)
                    db_cursor.execute(update_query)
                except Exception as e:
                    logging.info(e)
            if email is not None:
                try:
                    update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(sql_table_name,
                                                                                                  email_column_name,
                                                                                                  email,
                                                                                                  din_column_name,
                                                                                                  din, cin_column_name,
                                                                                                  cin)
                    logging.info(update_query)
                    db_cursor.execute(update_query)
                except Exception as e:
                    logging.info(e)
            logging.info(f"Updated entry in table {sql_table_name}")
        else:
            logging.info(f"Entry for din '{din}' with cin '{cin}' not exists in table {sql_table_name}")
    if sql_table_name == 'director_network':
        if len(result) > 0:
            if pan is not None:
                try:
                    update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(sql_table_name,
                                                                                                  pan_column_name,
                                                                                                  pan,
                                                                                                  din_column_name, din,
                                                                                                  cin_column_name,
                                                                                                  cin
                                                                                                  )

                    logging.info(update_query)
                    db_cursor.execute(update_query)
                    logging.info(f"Updated entry in table {sql_table_name}")
                except Exception as e:
                    logging.info(e)
        else:
            logging.info(f"Entry for din '{din}' with cin '{cin}' not exists in table {sql_table_name}")
    db_cursor.close()
    db_connection.close()


def xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, hidden_xml_file_path,
              output_file_path, cin_column_value, company_name, filing_date):
    setup_logging()
    # field_name_index = 0
    xml_type_index = 1
    single_group_type_index = 2
    parent_node_index = 3
    child_nodes_index = 4
    table_name_index = 5
    column_name_index = 6
    # column_json_node_index = 7

    config_dict_keys = [
    ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]
    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

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

    try:
        hidden_xml_tree = Et.parse(hidden_xml_file_path)
        hidden_xml_root = hidden_xml_tree.getroot()
        # xml_str = Et.tostring(xml_root, encoding='unicode')
    except Exception as e:
        raise Exception("Below exception occurred while reading xml file " + '\n' + str(e))

    cin_column_name_in_db = config_dict['cin_column_name_in_db']
    company_name_column_name_in_db = config_dict['company_name_column_name_in_db']

    # initializing empty list to save all the result dataframes for saving to output excel
    output_dataframes_list = []

    table_df_direct = pd.DataFrame()
    table_df_hidden = pd.DataFrame()

    event_abbreviation_list = [x.strip() for x in config_dict['event_abbreviation_list'].split(',')]
    event_list = [x.strip() for x in config_dict['event_list'].split(',')]
    event_dict = dict(zip(event_abbreviation_list, event_list))
    logging.info(event_dict)
    designation_abbreviation_list = [x.strip() for x in config_dict['designation_abbreviation_list'].split(',')]
    designation_list = [x.strip() for x in config_dict['designation_list'].split(',')]
    designation_dict = dict(zip(designation_abbreviation_list, designation_list))
    logging.info(designation_dict)

    # extract group values
    for index, row in group_df.iterrows():
        xml_type = str(row.iloc[xml_type_index])
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[table_name_index]).strip()
        column_names = str(row.iloc[column_name_index]).strip()
        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]

        table_node_name = parent_node
        # logging.info(table_node_name)
        if xml_type == config_dict['direct_xml_type_indicator']:
            try:
                logging.info(table_node_name)
                logging.info(child_nodes)
                table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
                table_df_direct = pd.DataFrame(table_in_list)
                table_df_direct.columns = column_names_list
                logging.info(table_df_direct)
            except Exception as e:
                logging.info(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
                continue

        elif xml_type == config_dict['hidden_xml_type_indicator']:
            continue
            # try:
            #     logging.info(table_node_name)
            #     logging.info(child_nodes)
            #     table_in_list = extract_table_values_from_hidden_xml(hidden_xml_root, table_node_name, child_nodes)
            #     table_df_hidden = pd.DataFrame(table_in_list)
            #     table_df_hidden.columns = column_names_list
            #     logging.info(table_df_hidden)
            # except Exception as e:
            #     logging.info(f'Exception {e} occurred while extracting data from xml for table {table_node_name} at line'
            #           f' number ')
            #     traceback.logging.info_exc()
            #     continue
        else:
            pass
        # table_df = pd.concat([table_df_direct, table_df_hidden])
        table_df = table_df_direct
        table_df[config_dict['filing_date_column_name']] = filing_date
        logging.info(table_df)

        table_df[cin_column_name_in_db] = cin_column_value
        table_df[company_name_column_name_in_db] = company_name

        column_names_list.append(cin_column_name_in_db)
        column_names_list.append(company_name_column_name_in_db)

        table_df.columns = column_names_list
        table_df = table_df[table_df[column_names_list[0]].notna()]
        logging.info(table_df)

        # current year
        # current_year = datetime.now().year
        # table_df['date_of_birth'] = pd.to_datetime(table_df['date_of_birth'])
        # table_df['Birth_Year'] = table_df['Birth_Date'].dt.year
        # table_df['age'] = current_year - table_df['Birth_Year']

        for i, df_row in table_df.iterrows():
            event_value = table_df.loc[i, config_dict['event_column_name']]
            table_df.loc[i, config_dict['event_column_name']] = event_dict.get(event_value, event_value)

            designation_value = table_df.loc[i, config_dict['designation_column_name']]
            table_df.loc[i, config_dict['designation_column_name']] = designation_dict.get(designation_value,
                                                                                           designation_value)
        logging.info(table_df)
        table_df.loc[table_df[config_dict['event_column_name']] ==
                     config_dict['appointment_keyword'], config_dict['date_of_appointment_column_name']] = \
            table_df[config_dict['event_date_column_name']]

        table_df.loc[table_df[config_dict['event_column_name']] ==
                     config_dict['resignation_keyword'], config_dict['date_of_cessation_column_name']] = \
            table_df[config_dict['event_date_column_name']]
        logging.info(table_df)

        for _, df_row in table_df.iterrows():
            try:
                insert_datatable_with_table(config_dict, db_config, sql_table_name, table_df.columns, df_row)
            except Exception as e:
                logging.info(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
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


def attachment_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                         output_file_path, cin):
    setup_logging()
    # field_name_index = 0
    single_group_type_index = 2
    parent_node_index = 3
    child_nodes_index = 4
    table_name_index = 5
    column_name_index = 6
    # column_json_node_index = 7

    cin_column_name_in_db = config_dict['cin_column_name_in_db']

    config_dict_keys = [
    ]

    missing_keys = [key for key in config_dict_keys if key not in config_dict]
    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

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

    # initializing empty list to save all the result dataframes for saving to output excel
    output_dataframes_list = []

    # extract group values
    for index, row in group_df.iterrows():
        parent_node = str(row.iloc[parent_node_index]).strip()
        child_nodes = str(row.iloc[child_nodes_index]).strip()
        sql_table_name = str(row.iloc[table_name_index]).strip()
        column_names = str(row.iloc[column_name_index]).strip()
        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]

        table_node_name = parent_node
        # logging.info(table_node_name)

        try:
            logging.info(table_node_name)
            logging.info(child_nodes)
            table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
            table_df = pd.DataFrame(table_in_list)
            table_df[cin_column_name_in_db] = cin
            logging.info(table_df)
            column_names_list.append(cin_column_name_in_db)
        except Exception as e:
            logging.info(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
            continue

        table_df.columns = column_names_list
        table_df = table_df[table_df[column_names_list[0]].notna()]
        logging.info(table_df)

        # current year
        # current_year = datetime.now().year
        # table_df['date_of_birth'] = pd.to_datetime(table_df['date_of_birth'])
        # table_df['Birth_Year'] = table_df['Birth_Date'].dt.year
        # table_df['age'] = current_year - table_df['Birth_Year']

        for _, df_row in table_df.iterrows():
            logging.info(df_row)
            try:
                update_attachment_table(db_config, config_dict, sql_table_name, table_df.columns, df_row)
            except Exception as e:
                logging.info(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
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


def dir_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, hidden_xml_file_path,
                  output_file_path, cin_column_value, company_name, filing_date):
    try:
        setup_logging()
        xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path, hidden_xml_file_path,
                  output_file_path, cin_column_value, company_name, filing_date)
    except Exception as e:
        logging.error("Below Exception occurred while processing DIR file: \n ", e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.error(line.strip())

        return False
    else:
        return True


def dir_attachment_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                             output_file_path, cin):
    try:
        setup_logging()
        attachment_xml_to_db(db_config, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                             output_file_path, cin)
    except Exception as e:
        logging.error("Below Exception occurred while processing DIR file: \n ", e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        # logging.info the traceback details
        for line in traceback_details:
            logging.error(line.strip())

        return False
    else:
        return True