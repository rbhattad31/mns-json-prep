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
from DIR2PDFToXML import DIR2_pdf_to_xml
from logging_config import setup_logging
import logging
import sys
import traceback
from Form18_old_files_pdf_to_xml import extract_form_data
# Get the current date
current_date = datetime.date.today()

# Format the date as dd-MM-yyyy
formatted_date = current_date.strftime("%d-%m-%Y")

today_date = current_date.strftime("%d-%m-%Y")
# logging.info the formatted date
user_name = os.getlogin()

def get_embedded_pdfs(input_pdf_path, output_path,file_name_hidden_pdf,db_config,cin,company_name):
    setup_logging()
    os.makedirs(output_path, exist_ok=True)
    doc = fitz.open(input_pdf_path)

    item_name_dict = {}
    for each_item in doc.embfile_names():
        item_name_dict[each_item] = doc.embfile_info(each_item)["filename"]

    for item_name, file_name in item_name_dict.items():
        out_pdf =  output_path + "\\" + file_name
        logging.info(out_pdf)
      ## get embeded_file in bytes
        fData = doc.embfile_get(item_name)
      ## save embeded file
        #logging.info(fData)
        with open(out_pdf, 'wb') as outfile:
            outfile.write(fData)
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            connection.autocommit = True

            check_query = 'select * from documents where cin = %s and document_download_path = %s'
            values = (cin,out_pdf)
            logging.info(check_query % values)
            cursor.execute(check_query,values)
            result = cursor.fetchall()
            if len(result) == 0:
                logging.info("Inserting hidden attachment into db")
                query = "Insert into documents(cin,company,Category,document,form_data_extraction_status,created_date,created_by,form_data_extraction_needed,Download_Status,DB_insertion_status,document_download_path) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                values = (cin, company_name, 'Hidden Attachment', file_name, 'Success', current_date, user_name, 'Y',
                          'Downloaded', 'Pending',out_pdf)
                logging.info(query % values)
                cursor.execute(query, values)
            cursor.close()
            connection.close()
        except Exception as e:
            logging.info(f"Error in updating in db for hidden attachment {e}")
            continue


def extract_xfa_data(pdf_path,filename):
    setup_logging()
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
    logging.info(xfa)
    if xfa is not None:
        if 'MSME' in filename or 'AOC-4' in filename or 'CHG' in filename or 'Form8' in filename or 'FiLLiP' in filename or 'Form 8' in filename or 'Form 18' in filename or 'Form 32' in filename:
            xml = xfa[7].get_object().get_data()
            return xml
        elif 'DIR' in filename or 'Form11' in filename:
            xml_DIR= []
            for i in [7,13]:
                xml = xfa[i].get_object().get_data()
                xml_DIR.append(xml)
            return xml_DIR
        elif 'INC-22' in filename:
            digit_count = sum(c.isdigit() for c in filename)
            logging.info(digit_count)
            xml_INC = []
            if digit_count == 10:
                xml = xfa[7].get_object().get_data()
                return xml
            else:
                for i in [7,19]:
                    xml = xfa[i].get_object().get_data()
                    xml_INC.append(xml)
                return xml_INC
        else:
            xml = xfa[9].get_object().get_data()
            return xml
    else:
        return None


def extract_tables_from_pdf(pdf_path):
    setup_logging()
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
        logging.info(tables)
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
    setup_logging()
    if xml_tree:
        tree = ET.ElementTree(xml_tree)
        tree.write(output_path, encoding="utf-8", xml_declaration=True)
        logging.info(f'XML data has been saved to {output_path}')
    else:
        logging.info('No data found to save.')


def write_xml_data(xfa_data,output_xml_path):
    with open(output_xml_path, 'wb') as xml_file:
        xml_file.write(xfa_data)


def PDFtoXML(pdf_path,file_name):
    # file_names = [file.name for file in folder_path.iterdir() if file.is_file()]
    setup_logging()
    try:
        if 'DIR_2'.lower() in file_name.lower() or 'DIR-2'.lower() in file_name.lower() or 'DIR 2'.lower() in file_name.lower():
            DIR_XML_File_Path,DIR_2_Status = DIR2_pdf_to_xml(pdf_path)
            if DIR_2_Status:
                return DIR_XML_File_Path,True
            else:
                return None,False
        xfa_data = extract_xfa_data(pdf_path, file_name)
        
        if xfa_data:
            # If XFA data is found, use the existing code to save it as XML
            if 'DIR' in file_name or 'Form11' in file_name:
                xml_plain = xfa_data[0]
                xml_hidden = xfa_data[1]
                xml_plain_file_path = pdf_path.replace('.pdf', '.xml')
                xml_hidden_file_path = pdf_path.replace('.pdf', '_hidden.xml')
                write_xml_data(xml_plain, xml_plain_file_path)
                write_xml_data(xml_hidden,xml_hidden_file_path)
                return xml_plain_file_path,True
            elif 'INC-22' in file_name:
                digit_count = sum(c.isdigit() for c in file_name)
                logging.info(digit_count)
                if digit_count == 10:
                    xml_file_path = pdf_path.replace('.pdf', '.xml')
                    if '.xml' not in xml_file_path:
                        xml_file_path = xml_file_path + '.xml'
                    write_xml_data(xfa_data, xml_file_path)
                    logging.info(f"Extracted XFA data for {file_name}")
                    logging.info(f"Saved to {xml_file_path}")
                    return xml_file_path, True
                else:
                    xml_plain = xfa_data[0]
                    xml_hidden = xfa_data[1]
                    xml_plain_file_path = pdf_path.replace('.pdf', '.xml')
                    xml_hidden_file_path = pdf_path.replace('.pdf', '_hidden.xml')
                    write_xml_data(xml_plain, xml_plain_file_path)
                    write_xml_data(xml_hidden, xml_hidden_file_path)
                    return xml_plain_file_path, True
            else:
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
                logging.info(f"Extracted XFA data for {file_name}")
                logging.info(f"Saved to {xml_file_path}")
                return xml_file_path, True
        elif 'Form 18'.lower() in str(file_name).lower():
            xml_file_path = pdf_path.replace('.pdf', '.xml')
            if '.xml' not in xml_file_path:
                xml_file_path = xml_file_path + '.xml'
            form18_old_files = extract_form_data(pdf_path,xml_file_path)
            if form18_old_files:
                return xml_file_path,True
            else:
                return xml_file_path,False
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
                logging.info("Extracted table data for ", file_name)
                logging.info("Saved table data to", xml_file_path)
                return xml_file_path, True
            else:
                logging.info("No XFA data or tables found in the PDF.")
                return None, False
    except Exception as e:
        logging.info(f"Exception in concerting pdf to xml {e}")
        exc_type, exc_value, exc_traceback = sys.exc_info()
            # Get the formatted traceback as a string
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

            # logging.info the traceback details
        for line in traceback_details:
            logging.info(line.strip())
        return None,False


def CheckHiddenAttachemnts(xml_file_path,folder_path,pdf_path,file_name,db_config,cin,company_name):
    hidden_xml_list = []
    setup_logging()
    if os.path.exists(pdf_path):
        # Check if the file name contains "MGT"
        if "MGT" in os.path.basename(pdf_path):
            try:
                # Parse the XML file
                tree = ET.parse(xml_file_path)
                root = tree.getroot()

                # Find the specific path to NO_BUSINESS_ACT
                path = ".//NO_BUSINESS_ACT"

                # Find and extract the NO_BUSINESS_ACT node
                no_business_act = root.find(path)

                if no_business_act is not None:
                    # logging.info("NO_BUSINESS_ACT:", no_business_act.text)
                    number_business = int(no_business_act.text)
                    logging.info(number_business)
                    if number_business > 10:
                        business_activity_folder_name = os.path.join(folder_path, "Business Activity Details")
                        if os.path.exists(business_activity_folder_name):
                            shutil.rmtree(business_activity_folder_name)
                        if not os.path.exists(business_activity_folder_name):
                            # If it doesn't exist, create it
                            os.makedirs(business_activity_folder_name)
                        logging.info("Downloading the Hidden Attachments")
                        get_embedded_pdfs(pdf_path, business_activity_folder_name,file_name,db_config,cin,company_name)
                        files_in_Business_folder = os.listdir(business_activity_folder_name)
                        for files in files_in_Business_folder:
                            if "Business Activity" not in files:
                                file_path = os.path.join(business_activity_folder_name, files)
                                os.remove(file_path)
                                logging.info("Deleted", file_path)
                        files_in_Business_folder = os.listdir(business_activity_folder_name)
                        for business_files in files_in_Business_folder:
                            business_xml_file_path = os.path.join(business_activity_folder_name,business_files.replace('.pdf', '.xml'))
                            business_pdf_path=os.path.join(business_activity_folder_name,business_files)
                            result = PDFtoXML(business_pdf_path,business_files)
                            hidden_xml_list.append(result[0])
                else:
                    logging.info("NO_BUSINESS_ACT not found at the specified path.")

                subsidiary_path = ".//HOLD_SUB_ASSOC"
                subsidiary = root.find(subsidiary_path)
                logging.info(subsidiary.text)
                if subsidiary.text is not None:
                    subsidiary_folder_name = os.path.join(folder_path, "List of Subsidiaries")
                    if os.path.exists(subsidiary_folder_name):
                        shutil.rmtree(subsidiary_folder_name)
                    if not os.path.exists(subsidiary_folder_name):
                        # If it doesn't exist, create it
                        os.makedirs(subsidiary_folder_name)
                    get_embedded_pdfs(pdf_path, subsidiary_folder_name,file_name,db_config,cin,company_name)
                    files_in_Subsidiary_Folder = os.listdir(subsidiary_folder_name)
                    for files in files_in_Subsidiary_Folder:
                        if not any(keyword not in files for keyword in ["Subsidiaries", "Holding", "Associate", "Joint Venture"]):
                            file_path = os.path.join(subsidiary_folder_name, files)
                            os.remove(file_path)
                            logging.info("Deleted", file_path)
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
                    logging.info("No Subsidiary Found")
                    return hidden_xml_list
            except ET.ParseError as e:
                logging.info("Error parsing the XML file:", str(e))

        elif "DIR" in os.path.basename(pdf_path):
            DIR_hidden_attachment_folder = os.path.join(folder_path,"DIR_2",str(os.path.basename(pdf_path)).replace('.pdf',''))
            if not os.path.exists(DIR_hidden_attachment_folder):
                os.makedirs(DIR_hidden_attachment_folder)
            get_embedded_pdfs(pdf_path,DIR_hidden_attachment_folder,file_name,db_config,cin,company_name)
            DIR_hidden_attachment_files = os.listdir(DIR_hidden_attachment_folder)
            for DIR_hidden in DIR_hidden_attachment_files:
                print(DIR_hidden)
                if not any(keyword not in DIR_hidden for keyword in ["DIR_2", "DIR-2", "DIR 2", "DIR-2-"]):
                    DIR_file_path = os.path.join(DIR_hidden_attachment_folder,DIR_hidden)
                    print(f"Removing{DIR_file_path}")
                    os.remove(DIR_file_path)
            DIR_hidden_attachment_files = os.listdir(DIR_hidden_attachment_folder)
            for DIR_2_files in DIR_hidden_attachment_files:
                if 'DIR_2'.lower() in DIR_2_files.lower() or 'DIR-2'.lower() in DIR_2_files.lower() or 'DIR 2'.lower() in DIR_2_files.lower() or 'DIR-2-'.lower() in DIR_2_files.lower():
                    print("Converting DIR-2 to XML")
                    DIR_2_PDF_Path = os.path.join(DIR_hidden_attachment_folder,DIR_2_files)
                    print(DIR_2_PDF_Path)
                    result_DIR2 = PDFtoXML(DIR_2_PDF_Path,DIR_2_files)
                    hidden_xml_list.append(result_DIR2[0])
                    return hidden_xml_list
            return hidden_xml_list
        else:
            logging.info("XML file name does not contain 'MGT'")
            return hidden_xml_list
    else:
        logging.info("XML file does not exist")
        return hidden_xml_list
    return hidden_xml_list


def fetch_form_extraction_file_data_from_table(connection,Cin,Company):
    try:
        setup_logging()
        if connection:
            cursor = connection.cursor()
            # Construct the SQL query
            connection.commit()
            time.sleep(5)
            query = "select * from documents where cin=%s and company=%s and form_data_extraction_needed='Y' and Download_Status='Downloaded' and form_data_extraction_status='Pending'"
            values = (Cin,Company)
            logging.info(query % values)
            cursor.execute(query,values)

            # Get the column names from the cursor description
            column_names = [desc[0] for desc in cursor.description]

            # Fetch all the rows
            rows = cursor.fetchall()
            Status = "Pass"
            logging.info(rows)
            return rows,Status

    except mysql.connector.Error as error:
        logging.info("Error:", error)
        return None,None




