import pandas as pd
import xml.etree.ElementTree as Et

# pass xml parse variable to functions - Complete
# config file - template file path, all hard values - complete
# main program that invokes all functions and reads config file - Complete
# Output file name is Output/DDMMYYYY/OutputFileName - complete
# separate map df into separate dfs of single and group rows - complete
# for loop for each df - complete
# all tables in one sheet - Complete
# empty the single variable values after for loop is completed -


# append the table at the end of the sheet without reading the existing data or copy
# from all sheets to single sheets , delete other sheets


def get_single_value(xml_root, parent_node, child_node):
    element = xml_root.find(f'.//{parent_node}//{child_node}')
    print(element)
    return element.text if element is not None else None


def extract_table_values(xml_root, table_node_name, child_nodes):
    data_list = []
    child_nodes_list = [x.strip() for x in child_nodes.split(',')]
    print(child_nodes_list)
    print(table_node_name)
    for data in xml_root.findall(f'.//{table_node_name}'):
        temp_list = []
        for node in child_nodes_list:
            print(node)
            try:
                node_value = data.find(node).text
            except AttributeError:
                node_value = None
            print(node_value)
            temp_list.append(node_value)
        print(temp_list)
        data_list.append(temp_list)
        print(data_list)
    return data_list


def xml_to_excel(config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path):
    output_dataframes_list = []
    df_map = pd.read_excel(map_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
    single_df = df_map[df_map['Type'] == config_dict['single_type_indicator']]
    group_df = df_map[df_map['Type'] == config_dict['group_type_indicator']]
    xml_tree = Et.parse(xml_file_path)
    xml_root = xml_tree.getroot()

    results = []

    for index, row in single_df.iterrows():
        field_name = str(row['Field_Name']).strip()
        print(field_name)
        type_value = str(row['Type']).strip()
        print(type_value)
        parent_node = str(row['Parent_Node']).strip()
        print(parent_node)
        child_nodes = str(row['Child_Nodes']).strip()
        print(child_nodes)

        if type_value == config_dict['single_type_indicator']:
            value = get_single_value(xml_root, parent_node, child_nodes)
            print("Value of ", parent_node, " and ", child_nodes, " is ", value)
            results.append([field_name, value])
            print(results)

    single_output_df = pd.DataFrame(results, columns=['Field Name', 'Value'])
    print(single_output_df)
    output_dataframes_list.append(single_output_df)

    for index, row in group_df.iterrows():
        field_name = str(row['Field_Name']).strip()
        print(field_name)
        type_value = str(row['Type']).strip()
        print(type_value)
        parent_node = str(row['Parent_Node']).strip()
        print(parent_node)
        child_nodes = str(row['Child_Nodes']).strip()
        print(child_nodes)
        if type_value == config_dict['group_type_indicator']:
            table_node_name = parent_node
            table_in_list = extract_table_values(xml_root, table_node_name, child_nodes)
            table_df = pd.DataFrame(table_in_list)
            output_dataframes_list.append(table_df)

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
        row_index = 0
        for dataframe in output_dataframes_list:
            dataframe.to_excel(writer, sheet_name=config_dict['output_sheet_name'], index=False, startrow=row_index)
            row_index += len(dataframe.index) + 2

    output_dataframes_list.clear()
