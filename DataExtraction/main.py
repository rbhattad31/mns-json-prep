from PDFToXML import extract_xfa_data, save_xfa_data_to_xml
from XMLToExcel import xml_to_excel
from CreateConfigDictionary import create_main_config_dictionary
from datetime import date
import os

# read config file
config_file_path = 'Input/Config.xlsx'
config_sheet_name = 'Main'
config_dict = create_main_config_dictionary(config_file_path, config_sheet_name)
print(config_dict)

# pdf to xml
pdf_file_path = config_dict['pdf path']
pdf_file_name = os.path.basename(pdf_file_path)
print(pdf_file_name)
print(pdf_file_path)
xml_file_path = str(pdf_file_path).replace('.pdf', '.xml')
print(xml_file_path)
xfa_data = extract_xfa_data(pdf_file_path)
save_xfa_data_to_xml(xfa_data, xml_file_path)

# xml to Excel

map_file_path = config_dict['mapping file path']
map_file_sheet_name = config_dict['mapping file sheet name']
output_file_folder = config_dict['output excel file folder path']

output_file_name = str(pdf_file_name).replace('.pdf', '.xlsx')

today_date = date.today().strftime("%m%d%Y")

output_dir_path = os.path.join(output_file_folder, today_date)
output_file_path = os.path.join(output_dir_path, output_file_name)

if os.path.exists(output_dir_path):
    if os.path.exists(output_file_path):
        os.remove(output_file_path)
else:
    os.mkdir(output_dir_path)

print(output_file_path)

xml_to_excel(config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path)
