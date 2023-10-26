import time

import PyPDF2 as pypdf
import re
from pathlib import Path
import pdfplumber
import xml.etree.ElementTree as ET
import os
import glob
import fitz
import datetime
import shutil
import mysql.connector
# Get the current date
current_date = datetime.date.today()

# Format the date as dd-MM-yyyy
formatted_date = current_date.strftime("%d-%m-%Y")

# Print the formatted date

def get_embedded_pdfs(input_pdf_path, output_path,file_name_hidden_pdf):
    os.makedirs(output_path, exist_ok=True)
    doc = fitz.open(input_pdf_path)

    item_name_dict = {}
    for each_item in doc.embfile_names():
        item_name_dict[each_item] = doc.embfile_info(each_item)["filename"]

    for item_name, file_name in item_name_dict.items():
        out_pdf =  output_path + "\\" + file_name.replace('.pdf' ,'') + file_name_hidden_pdf + '.pdf'
        print(out_pdf)
      ## get embeded_file in bytes
        fData = doc.embfile_get(item_name)
      ## save embeded file
        #print(fData)
        with open(out_pdf, 'wb') as outfile:
            outfile.write(fData)
def extract_xfa_data(pdf_path,filename):
    def findInDict(needle, haystack):
        for key in haystack.keys():
            try:
                value = haystack[key]
            except:
                continue
            if key == needle:
                return value
            if isinstance(value, dict):
                x = findInDict(needle, value)
                if x is not None:
                    return x

    pdfobject = open(pdf_path, 'rb')
    pdf = pypdf.PdfReader(pdfobject)
    xfa = findInDict('/XFA', pdf.resolved_objects)
    if xfa is not None:
        if 'MSME' in filename or 'AOC-4' in filename or 'CHG' in filename or 'LLP' in filename or 'Form8' in filename or 'FiLLiP' in filename:
            xml = xfa[7].get_object().get_data()
            return xml
        else:
            xml = xfa[9].get_object().get_data()
            return xml
    else:
        return None


def extract_tables_from_pdf(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        tables = []
        column_names = None  # Initialize to None
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                if column_names is None:
                    # Assuming the first row contains column names
                    column_names = table[0]
                else:
                    # Ensure subsequent pages have the same column structure
                    #table[0] = column_names
                    table.insert(0,column_names)
                tables.append(table)
        print(tables)
        return tables


def clean_xml_name(name):
    # Remove characters that are not allowed in XML element names
    name = re.sub(r'[^a-zA-Z0-9_\-]', '_', name)
    # Ensure the name starts with a letter or underscore
    if not name[0].isalpha() and name[0] != '_':
        name = '_' + name
    return name


def tables_to_xml(tables):
    root = ET.Element("tables")
    for i, table in enumerate(tables):
        table_elem = ET.SubElement(root, f"table_{i+1}")

        # Assuming the first row contains column names
        column_names = [clean_xml_name(col) for col in table[0]]

        for row in table[1:]:  # Skip the first row with column names
            row_elem = ET.SubElement(table_elem, "row")
            for col_name, value in zip(column_names, row):
                col_elem = ET.SubElement(row_elem, col_name)
                col_elem.text = str(value)
    return root


def save_xfa_data_to_xml(xml_tree, output_path):
    if xml_tree:
        tree = ET.ElementTree(xml_tree)
        tree.write(output_path, encoding="utf-8", xml_declaration=True)
        print(f'XML data has been saved to {output_path}')
    else:
        print('No data found to save.')
def write_xml_data(xfa_data,output_xml_path):
    with open(output_xml_path, 'wb') as xml_file:
        xml_file.write(xfa_data)

def PDFtoXML(pdf_path,file_name):
    # file_names = [file.name for file in folder_path.iterdir() if file.is_file()]
    try:
        xfa_data = extract_xfa_data(pdf_path, file_name)

        if xfa_data:
            # If XFA data is found, use the existing code to save it as XML
            xml_file_path = pdf_path.replace('.pdf', '.xml')
            """
            if '.pdf' in file_name:
                xml_file_path = os.path.join(folder_path, file_name.replace('.pdf', '.xml'))
            else:
                xml_file_path = os.path.join(folder_path, file_name)
            """
            if '.xml' not in xml_file_path:
                xml_file_path = xml_file_path + '.xml'
            write_xml_data(xfa_data, xml_file_path)
            print("Extracted XFA data for ", file_name)
            print("Saved to", xml_file_path)
            return xml_file_path, True
        else:
            # If XFA data is not found, extract table data and save it as XML
            tables = extract_tables_from_pdf(pdf_path)
            if tables:
                xml_tree = tables_to_xml(tables)
                xml_file_path = pdf_path.replace('.pdf', '.xml')
                """
                if '.pdf' in file_name:
                    xml_file_path = os.path.join(folder_path, file_name.replace('.pdf', '.xml'))
                else:
                    xml_file_path = os.path.join(folder_path, file_name)
                """
                if '.xml' not in xml_file_path:
                    xml_file_path = xml_file_path + '.xml'
                save_xfa_data_to_xml(xml_tree, xml_file_path)
                print("Extracted table data for ", file_name)
                print("Saved table data to", xml_file_path)
                return xml_file_path, True
            else:
                print("No XFA data or tables found in the PDF.")
                return None, False
    except Exception as e:
        print(f"Exception in concerting pdf to xml {e}")
        return None,False


def CheckHiddenAttachemnts(xml_file_path,folder_path,pdf_path,file_name):
    hidden_xml_list = []
    if os.path.exists(xml_file_path):
        # Check if the file name contains "MGT"
        if "MGT" in os.path.basename(xml_file_path):
            try:
                # Parse the XML file
                tree = ET.parse(xml_file_path)
                root = tree.getroot()

                # Find the specific path to NO_BUSINESS_ACT
                path = ".//NO_BUSINESS_ACT"

                # Find and extract the NO_BUSINESS_ACT node
                no_business_act = root.find(path)

                if no_business_act is not None:
                    # print("NO_BUSINESS_ACT:", no_business_act.text)
                    number_business = int(no_business_act.text)
                    print(number_business)
                    if number_business > 10:
                        business_activity_folder_name = os.path.join(folder_path, "Business Activity Details")
                        if os.path.exists(business_activity_folder_name):
                            shutil.rmtree(business_activity_folder_name)
                        if not os.path.exists(business_activity_folder_name):
                            # If it doesn't exist, create it
                            os.makedirs(business_activity_folder_name)
                        print("Downloading the Hidden Attachments")
                        get_embedded_pdfs(pdf_path, business_activity_folder_name,file_name)
                        files_in_Business_folder = os.listdir(business_activity_folder_name)
                        for files in files_in_Business_folder:
                            if "Business Activity" not in files:
                                file_path = os.path.join(business_activity_folder_name, files)
                                os.remove(file_path)
                                print("Deleted", file_path)
                        files_in_Business_folder = os.listdir(business_activity_folder_name)
                        for business_files in files_in_Business_folder:
                            business_xml_file_path = os.path.join(business_activity_folder_name,business_files.replace('.pdf', '.xml'))
                            business_pdf_path=os.path.join(business_activity_folder_name,business_files)
                            result = PDFtoXML(business_pdf_path,business_files)
                            hidden_xml_list.append(result[0])
                else:
                    print("NO_BUSINESS_ACT not found at the specified path.")

                subsidiary_path = ".//HOLD_SUB_ASSOC"
                subsidiary = root.find(subsidiary_path)
                print(subsidiary.text)
                if subsidiary.text is not None:
                    subsidiary_folder_name = os.path.join(folder_path, "List of Subsidiaries")
                    if os.path.exists(subsidiary_folder_name):
                        shutil.rmtree(subsidiary_folder_name)
                    if not os.path.exists(subsidiary_folder_name):
                        # If it doesn't exist, create it
                        os.makedirs(subsidiary_folder_name)
                    get_embedded_pdfs(pdf_path, subsidiary_folder_name,file_name)
                    files_in_Subsidiary_Folder = os.listdir(subsidiary_folder_name)
                    for files in files_in_Subsidiary_Folder:
                        if not any(keyword not in files for keyword in ["Subsidiaries", "Holding", "Associate", "Joint Venture"]):
                            file_path = os.path.join(subsidiary_folder_name, files)
                            os.remove(file_path)
                            print("Deleted", file_path)
                    files_in_Subsidiary_Folder = os.listdir(subsidiary_folder_name)
                    for subsidiary_files in files_in_Subsidiary_Folder:
                        business_xml_file_path = os.path.join(subsidiary_folder_name,subsidiary_files.replace('.pdf', '.xml'))
                        subsidiary_pdf_path = os.path.join(subsidiary_folder_name, subsidiary_files)
                        if "Subsidiaries" in subsidiary_files or "Holding" in subsidiary_files or "Associate" in subsidiary_files or "Joint Venture"  in subsidiary_files:
                            result_subsidiary = PDFtoXML(subsidiary_pdf_path, subsidiary_files)
                            hidden_xml_list.append(result_subsidiary[0])
                            return hidden_xml_list
                    return hidden_xml_list
                else:
                    print("No Subsidiary Found")
                    return hidden_xml_list
            except ET.ParseError as e:
                print("Error parsing the XML file:", str(e))
        else:
            print("XML file name does not contain 'MGT'")
            return hidden_xml_list
    else:
        print("XML file does not exist")
        return hidden_xml_list
    return hidden_xml_list


def fetch_form_extraction_file_data_from_table(connection,Cin,Company):
    try:
        if connection:
            cursor = connection.cursor()
            # Construct the SQL query
            connection.commit()
            time.sleep(5)
            query = "select * from documents where cin=%s and company=%s and form_data_extraction_needed='Y' and Download_Status='Downloaded' and form_data_extraction_status='Pending'"
            values = (Cin,Company)
            print(query % values)
            cursor.execute(query,values)

            # Get the column names from the cursor description
            column_names = [desc[0] for desc in cursor.description]

            # Fetch all the rows
            rows = cursor.fetchall()
            Status = "Pass"
            print(rows)
            return rows,Status

    except mysql.connector.Error as error:
        print("Error:", error)
        return None,None





