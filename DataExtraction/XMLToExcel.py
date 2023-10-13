import pandas as pd
import xml.etree.ElementTree as Et
import json

pd.set_option('display.max_columns', None)


def get_single_value(xml_root, parent_node, child_node):
    element = xml_root.find(f'.//{parent_node}//{child_node}')
    # print(element)
    return element.text if element is not None else None


def update_datatable_single_value(db_cursor, table_name, cin_column_name, cin_value,
                                  company_name_column_name,
                                  company_name, column_name, column_value):
    # query = f'''Insert into {table_name}({unique_column_name}, {column_name}) Values('{unique_value}', '{value}') ON
    # DUPLICATE KEY UPDATE {column_name} = '{value}';'''

    query = "SELECT * FROM {} WHERE {} = '{}'".format(table_name, cin_column_name, cin_value)
    # print(query)
    db_cursor.execute(query)

    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    result = db_cursor.fetchall()
    # print(result)
    if len(result) > 0:
        # print("Value exists")
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(table_name, column_name,
                                                                                      column_value, cin_column_name,
                                                                                      cin_value,
                                                                                      company_name_column_name,
                                                                                      company_name
                                                                                      )
        print(update_query)
        db_cursor.execute(update_query)
    else:
        # print("Value does not exist")
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      column_value)
        # print(insert_query)
        db_cursor.execute(insert_query)


def extract_table_values(xml_root, table_node_name, child_nodes):
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


def sql_insert(db_cursor, sql_table_name, column_names_list, df_row):
    insert_query = f"""
                        INSERT INTO {sql_table_name}
                        SET {', '.join([f'{col} = %s' for col in column_names_list])};
                        """
    db_cursor.execute(insert_query, tuple(df_row.values))


def xml_to_excel(db_connection, db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path,
                 output_file_path):
    output_dataframes_list = []
    df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
    df_map['Value'] = None
    single_df = df_map[df_map['Type'] == config_dict['single_type_indicator']]
    group_df = df_map[df_map['Type'] == config_dict['group_type_indicator']]
    xml_tree = Et.parse(xml_file_path)
    xml_root = xml_tree.getroot()

    cin_column_name_in_db = config_dict['cin_column_name_in_db']
    cin_column_value = 'L32102KA1945PLC020800'
    company_name_column_name_in_db = config_dict['company_name_column_name_in_db']
    company_name = 'WIPRO LIMITED'

    results = []

    # extract single values
    for index, row in single_df.iterrows():
        field_name = str(row['Field_Name']).strip()
        type_value = str(row['Type']).strip()
        parent_node = str(row['Parent_Node']).strip()
        child_nodes = str(row['Child_Nodes']).strip()
        sql_table_name = str(row['Table_Name']).strip()
        column_name = str(row['Column_Name']).strip()
        column_json_node = str(row['Column_JSON_Node']).strip()

        if type_value == config_dict['single_type_indicator']:
            value = get_single_value(xml_root, parent_node, child_nodes)
            single_df.at[index, 'Value'] = value
            # if sql_table_name == config_dict['principal_business_activities_table_name']:
            #     year_end_date = value
            results.append([field_name, value, sql_table_name, column_name, column_json_node])
    # print(single_df)

    # update single values in datatable
    sql_tables_list = single_df['Table_Name'].unique()
    # for each distinct table value, filter the df with table value and find columns
    for sql_table_name in sql_tables_list:
        if sql_table_name == config_dict['principal_business_activities_table_name']:
            continue
        # print(sql_table_name)
        # filter only table
        table_df = single_df[single_df['Table_Name'] == sql_table_name]
        # print(table_df)

        columns_list = table_df['Column_Name'].unique()
        # print(columns_list)
        for column_name in columns_list:
            # print(column_name)
            column_df = table_df[table_df['Column_Name'] == column_name]
            # print(column_df)

            json_dict = column_df.set_index('Field_Name')['Value'].to_dict()
            # Convert the dictionary to a JSON string
            json_string = json.dumps(json_dict)
            print(json_string)
            if sql_table_name == 'Company':
                company_name_column_name = 'legal_name'
            else:
                company_name_column_name = company_name_column_name_in_db
            update_datatable_single_value(db_cursor, sql_table_name, cin_column_name_in_db, cin_column_value,
                                          company_name_column_name,
                                          company_name, column_name,
                                          json_string)

    single_output_df = pd.DataFrame(results, columns=['Field Name', 'Value', 'Table Name', 'Column Name',
                                                      'Column ' 'JSON Node'])
    # print(single_output_df)
    output_dataframes_list.append(single_output_df)
    print("Completed processing single rows")
    # extract group values
    for index, row in group_df.iterrows():
        field_name = str(row['Field_Name']).strip()
        type_value = str(row['Type']).strip()
        parent_node = str(row['Parent_Node']).strip()
        child_nodes = str(row['Child_Nodes']).strip()
        sql_table_name = str(row['Table_Name']).strip()
        column_names = str(row['Column_Name']).strip()
        column_json_node = str(row['Column_JSON_Node']).strip()
        if type_value == config_dict['group_type_indicator']:
            table_node_name = parent_node
            table_in_list = extract_table_values(xml_root, table_node_name, child_nodes)
            table_df = pd.DataFrame(table_in_list)
            hold_sub_assoc_table = table_df.copy()
            table_df[cin_column_name_in_db] = cin_column_value
            table_df[company_name_column_name_in_db] = company_name
            column_names_list = column_names.split(',')
            column_names_list.append(cin_column_name_in_db)
            column_names_list.append(company_name_column_name_in_db)
            column_names_list = [x.strip() for x in column_names_list]
            # print(column_names_list)
            if not field_name == config_dict['Hold_Sub_Assoc_field_name']:
                table_df.columns = column_names_list

            # update group values in datatable
            if field_name == config_dict['principal_business_activities_field_name']:
                year_end_date = single_df[single_df['Field_Name'] == config_dict['year_field_name']]['Value'].values[0]
                # print(f'{year_end_date=}')
                table_df['Year'] = year_end_date
                table_df = table_df[table_df[column_names_list[0]].notna()]
                # print(table_df)
                for _, df_row in table_df.iterrows():
                    insert_query = f"""
                    INSERT INTO {sql_table_name}
                    SET {', '.join([f'{col} = %s' for col in table_df.columns])};
                    """
                    # print(insert_query)
                    # print(df_row.values)
                    db_cursor.execute(insert_query, tuple(df_row.values))

                # Another way but not working
                # Generate the SQL query for updating data
                # insert_query = f"INSERT INTO {sql_table_name} SET "
                # insert_query += ', '.join([f'{col} = %s' for col in table_df.columns])
                # print(insert_query)
                # # Create a list of tuples for parameterized query
                # data_to_update = [tuple(row) for row in table_df.values]
                # print(data_to_update)
                # db_cursor.executemany(insert_query, data_to_update)
                # db_connection.commit()
                print("DB execution is complete for principal business activities")
            elif field_name == config_dict['director_shareholdings_field_name']:
                year_end_date = single_df[single_df['Field_Name'] == config_dict['year_field_name']]['Value'].values[0]
                # print(f'{year_end_date=}')
                table_df['Year'] = year_end_date
                table_df = table_df[table_df[column_names_list[0]].notna()]
                # print(table_df)
                for _, df_row in table_df.iterrows():
                    insert_query = f"""
                    INSERT INTO {sql_table_name}
                    SET {', '.join([f'{col} = %s' for col in table_df.columns])};
                    """
                    # print(insert_query)
                    # print(df_row.values)
                    db_cursor.execute(insert_query, tuple(df_row.values))
                print("DB execution is complete for director shareholdings table")
            elif field_name == config_dict['Hold_Sub_Assoc_field_name']:
                # print(hold_sub_assoc_table)
                hold_sub_assoc_table.columns = [column_names_list[0], column_names_list[1],
                                                config_dict['Hold_Sub_Assoc_column_name'], column_names_list[2]]
                hold_sub_assoc_table = hold_sub_assoc_table[hold_sub_assoc_table[column_names_list[0]].notna()]
                # print(hold_sub_assoc_table)

                hold_sub_assoc_db_table_columns = [column_names_list[0], column_names_list[1], column_names_list[2]]

                # print(sql_table_name)
                sql_tables_list = [x.strip() for x in sql_table_name.split(',')]
                # print(sql_tables_list)

                for _, df_row in hold_sub_assoc_table.iterrows():
                    hold_sub_assoc_value = df_row[config_dict['Hold_Sub_Assoc_column_name']]
                    cin_value = df_row[column_names_list[1]]
                    cin_length = len(cin_value)
                    # print(hold_sub_assoc_value)
                    # print(cin_value)
                    # print(cin_length)

                    # removing value of hold sub assoc value to have only the values to append to datatables
                    df_row.pop(config_dict['Hold_Sub_Assoc_column_name'])

                    # hold_sub_assoc_value = 'ASSOC'
                    if hold_sub_assoc_value == config_dict['associate_keyword_in_xml']:
                        associate_tables_list = [item for item in sql_tables_list if str(hold_sub_assoc_value).lower() in
                                                 item.lower()]
                        # cin_length = 21    # These values are for testing, uncomment for all the tables testing
                        if cin_length == 21:
                            # companies
                            sql_table_companies = [item for item in associate_tables_list if 'companies' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_companies, hold_sub_assoc_db_table_columns, df_row)
                        # cin_length = 8
                        if cin_length == 8:
                            # llp
                            sql_table_llp = [item for item in associate_tables_list if 'llp' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_llp, hold_sub_assoc_db_table_columns, df_row)
                        # cin_length = 218
                        if cin_length != 21 and cin_length != 8:
                            # others
                            sql_table_others = [item for item in associate_tables_list if 'others' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_others, hold_sub_assoc_db_table_columns, df_row)

                    # hold_sub_assoc_value = 'HOLD'
                    if hold_sub_assoc_value == config_dict['holding_keyword_in_xml']:
                        holding_tables_list = [item for item in sql_tables_list if str(hold_sub_assoc_value).lower() in
                                               item.lower()]
                        # cin_length = 21
                        if cin_length == 21:
                            # companies
                            sql_table_companies = [item for item in holding_tables_list if 'companies' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_companies, hold_sub_assoc_db_table_columns, df_row)
                        # cin_length = 8
                        if cin_length == 8:
                            # llp
                            sql_table_llp = [item for item in holding_tables_list if 'llp' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_llp, hold_sub_assoc_db_table_columns, df_row)
                        # cin_length = 218
                        if cin_length != 21 and cin_length != 8:
                            # others
                            sql_table_others = [item for item in holding_tables_list if 'others' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_others, hold_sub_assoc_db_table_columns, df_row)

                    # hold_sub_assoc_value = 'JOINT'
                    if hold_sub_assoc_value == config_dict['joint_venture_keyword_in_xml']:
                        joint_tables_list = [item for item in sql_tables_list if str(hold_sub_assoc_value).lower() in
                                             item.lower()]
                        # cin_length = 21
                        if cin_length == 21:
                            # companies
                            sql_table_companies = [item for item in joint_tables_list if 'companies' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_companies, hold_sub_assoc_db_table_columns, df_row)
                        # cin_length = 8
                        if cin_length == 8:
                            # llp
                            sql_table_llp = [item for item in joint_tables_list if 'llp' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_llp, hold_sub_assoc_db_table_columns, df_row)
                        # cin_length = 218
                        if cin_length != 21 and cin_length != 8:
                            # others
                            sql_table_others = [item for item in joint_tables_list if 'others' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_others, hold_sub_assoc_db_table_columns, df_row)

                    # hold_sub_assoc_value = 'SUBS'
                    if hold_sub_assoc_value == config_dict['subsidiary_keyword_in_xml']:
                        subsidiary_tables_list = [item for item in sql_tables_list if str(hold_sub_assoc_value).lower() in
                                                  item.lower()]
                        # cin_length = 21
                        if cin_length == 21:
                            # companies
                            sql_table_companies = [item for item in subsidiary_tables_list if 'companies' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_companies, hold_sub_assoc_db_table_columns, df_row)
                        # cin_length = 8
                        if cin_length == 8:
                            # llp
                            sql_table_llp = [item for item in subsidiary_tables_list if 'llp' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_llp, hold_sub_assoc_db_table_columns, df_row)
                        # cin_length = 218
                        if cin_length != 21 and cin_length != 8:
                            # others
                            sql_table_others = [item for item in subsidiary_tables_list if 'others' in item.lower()][0]
                            sql_insert(db_cursor, sql_table_others, hold_sub_assoc_db_table_columns, df_row)
                    else:
                        pass

                print("DB execution is complete for director shareholdings table")
                pass
            elif field_name == config_dict['director_remuneration_field_name']:
                pass
            else:
                pass
            output_dataframes_list.append(table_df)

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            # print(dataframe)
            dataframe.to_excel(writer, sheet_name=config_dict['output_sheet_name'], index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()
