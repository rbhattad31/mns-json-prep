from PDFToXML import extract_xfa_data, save_xfa_data_to_xml
from Programs_MSME.MSME_XMLToDB import msme_xml_to_db
from CreateConfigDictionary import create_main_config_dictionary
from datetime import date
import os
from WriteToDB import db_cursor

# read config file
config_file_path = 'Input/Config.xlsx'
config_sheet_name = 'MSME'
config_dict = create_main_config_dictionary(config_file_path, config_sheet_name)

for key, value in config_dict.items():
    print(f'{key}: {value}')

config_dict_keys = ['pdf path',
                    'mapping file path',
                    'mapping file sheet name',
                    'output excel file folder path'
                    ]
missing_keys = [key for key in config_dict_keys if key not in config_dict]

if missing_keys:
    raise KeyError(f"The following keys are missing in config file: {', '.join(missing_keys)}")

# pdf to xml
pdf_file_path = config_dict['pdf path']
pdf_file_name = os.path.basename(pdf_file_path)
print(pdf_file_name)
print(pdf_file_path)

# xml_file_path = str(pdf_file_path).replace('.pdf', '.xml')
# xfa_data = extract_xfa_data(pdf_file_path)
# save_xfa_data_to_xml(xfa_data, xml_file_path)


xml_file_path = 'Input/MSME/1_Form MSME FORM I-14092023_signed_1 - Copy.xml'
# xml_file_path = 'Input/MSME/2_Form MSME FORM I-21072022_signed_2 - Copy.xml'
# xml to Excel

map_file_path = config_dict['mapping file path']
# map_file_path = 'Input/Subsidiary_associate_MGT7_config.xlsx'
# map_file_path = 'Input/business activity mgt7 config.xlsx'

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

cin = 'L27109PN1999PLC016417'
company_name = 'ISMT LIMITED'
result = msme_xml_to_db(db_cursor, config_dict, map_file_path, map_file_sheet_name, xml_file_path, output_file_path, cin, company_name)

if result:
    print("process complete for msme")
else:
    print("process failed for msme")

