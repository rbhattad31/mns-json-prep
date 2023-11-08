import pandas as pd
import xml.etree.ElementTree as Et
import os


pd.set_option('display.max_columns', None)


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


def insert_datatable_with_table(db_cursor, sql_table_name, column_names_list, df_row):
    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # print(result_dict)

    where_clause = f'SELECT * FROM {sql_table_name} WHERE '
    for key, value in result_dict.items():
        if value is not None:
            where_clause += f"`{key}` = '{value}' AND "
        else:
            where_clause += f"(`{key}` is NULL OR `{key}` = '') AND "

    select_query = where_clause[:-4]
    print(select_query)
    db_cursor.execute(select_query)
    result = db_cursor.fetchall()
    print(len(result))
    if len(result) == 0:  # If no matching record found
        # Insert the record
        insert_query = f"""
                        INSERT INTO {sql_table_name}
                        SET {', '.join([f'{col} = %s' for col in column_names_list])};
                        """
        db_cursor.execute(insert_query, tuple(df_row.values))
        # print(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
    else:
        print(f"Entry with values already exists in table {sql_table_name}")


def xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
              output_file_path, cin_column_value, company_name):
    config_dict_keys = [
    ]
    missing_keys = [key for key in config_dict_keys if key not in config_dict]

    if missing_keys:
        raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

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

    cin_column_name_in_db = config_dict['cin_column_name_in_db']
    company_name_column_name_in_db = config_dict['company_name_column_name_in_db']

    # initializing list to save single values data
    results = []

    # print(single_df)
    # extract single values
    for index, row in single_df.iterrows():
        field_name = str(row.iloc[0]).strip()
        parent_node = str(row.iloc[3]).strip()
        child_nodes = str(row.iloc[4]).strip()
        sql_table_name = str(row.iloc[5]).strip()
        column_name = str(row.iloc[6]).strip()
        column_json_node = str(row.iloc[7]).strip()

        value = get_single_value_from_xml(xml_root, parent_node, child_nodes)
        # print(value)
        single_df.at[index, 'Value'] = value
        results.append([field_name, value, sql_table_name, column_name, column_json_node])
        # print(results)
    # print(single_df)

    print("Completed processing single rows")

    total_outstanding_amount = single_df[single_df[single_df.columns[0]]
                                         == config_dict['total_outstanding_amount_field_name']]['Value'].values[0]
    payment_due_reason = single_df[single_df[single_df.columns[0]]
                                   == config_dict['payment_due_reason_field_name']]['Value'].values[0]
    # print(total_outstanding_amount)
    # print(payment_due_reason)

    # extract group values
    for index, row in group_df.iterrows():
        # field_name = str(row.iloc[0]).strip()
        parent_node = str(row.iloc[3]).strip()
        child_nodes = str(row.iloc[4]).strip()
        sql_table_name = str(row.iloc[5]).strip()
        column_names = str(row.iloc[6]).strip()
        # column_json_node = str(row.iloc[7]).strip()

        table_node_name = parent_node
        # print(table_node_name)
        try:
            table_in_list = extract_table_values_from_xml(xml_root, table_node_name, child_nodes)
        except Exception as e:
            print(f'Exception {e} occurred while extracting data from xml for table {table_node_name}')
            continue
        # print(table_in_list)
        table_df = pd.DataFrame(table_in_list)

        column_names_list = column_names.split(',')
        column_names_list = [x.strip() for x in column_names_list]
        table_df[cin_column_name_in_db] = cin_column_value
        table_df[company_name_column_name_in_db] = company_name
        table_df[config_dict['total_outstanding_amount_field_name']] = total_outstanding_amount
        table_df[config_dict['payment_due_reason_field_name']] = payment_due_reason

        column_names_list.append(cin_column_name_in_db)
        column_names_list.append(company_name_column_name_in_db)
        column_names_list.append(config_dict['total_outstanding_amount_field_name'])
        column_names_list.append(config_dict['payment_due_reason_field_name'])

        column_names_list = [x.strip() for x in column_names_list]
        table_df.columns = column_names_list
        table_df = table_df[table_df[column_names_list[0]].notna()]
        print(table_df)

        for _, df_row in table_df.iterrows():
            try:
                insert_datatable_with_table(db_cursor, sql_table_name, table_df.columns, df_row)
            except Exception as e:
                print(f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                      df_row)
        print(f"DB execution is complete for {sql_table_name}")
        output_dataframes_list.append(table_df)

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()


def msme_xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                   output_file_path, cin_column_value, company_name):
    try:
        xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                  output_file_path, cin_column_value, company_name)
    except Exception as e:
        print("Below Exception occurred while processing msme file: \n ", e)
        return False
    else:
        return True
