import mysql.connector
from logging_config import setup_logging
import fitz
import logging
import os
import datetime
import xml.etree.ElementTree as ET

current_date = datetime.date.today()

# Format the date as dd-MM-yyyy
formatted_date = current_date.strftime("%d-%m-%Y")

today_date = current_date.strftime("%d-%m-%Y")
# logging.info the formatted date
user_name = os.getlogin()


def check_missing_years(db_config,cin):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True

    missing_years_query = """SELECT *
                            FROM documents
                            WHERE cin = '{}'
                              AND form_data_extraction_needed = 'Y'
                              AND document LIKE '%AOC-4(XBRL)%'
                              AND LOWER(Category) LIKE '%annual returns%'
                              AND YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y')) NOT IN (
                                SELECT DISTINCT YEAR(STR_TO_DATE(document_date_year, '%d-%m-%Y'))
                                FROM documents
                                WHERE Category = 'Other Attachments'
                                  AND cin = '{}'
                                  AND (
                                    document LIKE '%XBRL document in respect Consolidated%'
                                    OR document LIKE '%XBRL financial statements%'
                                  )
                              );
                                """.format(cin,cin)
    print(missing_years_query)
    cursor.execute(missing_years_query)
    missing_year_result = cursor.fetchall()
    cursor.close()
    connection.close()
    return missing_year_result


def get_embedded_pdfs(input_pdf_path, output_path,file_name_hidden_pdf,db_config,cin,company_name,date):
    try:
        setup_logging()
        os.makedirs(output_path, exist_ok=True)
        doc = fitz.open(input_pdf_path)

        item_name_dict = {}
        for each_item in doc.embfile_names():
            item_name_dict[each_item] = doc.embfile_info(each_item)["filename"]

        file_date = str(date).replace('-','')
        for item_name, file_name in item_name_dict.items():
            out_pdf = output_path + "\\" + file_name
            if '.xml' not in out_pdf:
                out_pdf = out_pdf + '.xml'
            fData = doc.embfile_get(item_name)
            if '.xml' in str(file_name).lower():
                logging.info(file_name)
                with open(out_pdf, 'wb') as outfile:
                    outfile.write(fData)
                xbrl_nature,filing_standard = nature(out_pdf)
                print(xbrl_nature)
                if xbrl_nature is not None:
                    logging.info("Going with nature from xml nodes")
                    if 'consolidated' in str(xbrl_nature).lower() or 'conso' in str(xbrl_nature).lower():
                        xbrl_file_name = f"XBRL document in respect Consolidated financial statement-{file_date}"
                    else:
                        xbrl_file_name = f"XBRL financial statements duly authenticated as per section 134-{file_date}"
                else:
                    logging.info("Xbrl nature was not found from nodes so going with file name")
                    if 'consolidated' in str(file_name).lower() or 'conso' in str(file_name).lower():
                        xbrl_file_name = f"XBRL document in respect Consolidated financial statement-{file_date}"
                    else:
                        xbrl_file_name = f"XBRL financial statements duly authenticated as per section 134-{file_date}"
                updated_file_path = os.path.join(output_path,xbrl_file_name)
                if '.xml' not in updated_file_path:
                    updated_file_path = updated_file_path + '.xml'
                print(f"Old path:{out_pdf}")
                print(f"New path:{updated_file_path}")
                os.rename(out_pdf,updated_file_path)
                category = f"XBRL XML Attachment({filing_standard})"
                try:
                    connection = mysql.connector.connect(**db_config)
                    cursor = connection.cursor()
                    connection.autocommit = True

                    check_query = 'select * from documents where cin = %s and document_download_path = %s'
                    values = (cin,updated_file_path)
                    logging.info(check_query % values)
                    cursor.execute(check_query,values)
                    result = cursor.fetchall()
                    if len(result) == 0:
                        logging.info("Inserting hidden attachment into db for XBRL XMl hidden")
                        query = "Insert into documents(cin,company,Category,document,form_data_extraction_status,created_date,created_by,form_data_extraction_needed,Download_Status,DB_insertion_status,document_download_path,document_date_year) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        values = (cin, company_name, category, xbrl_file_name, 'Pending', current_date, user_name, 'Y',
                                      'Downloaded', 'Pending',updated_file_path,date)
                        logging.info(query % values)
                        cursor.execute(query, values)
                    cursor.close()
                    connection.close()
                except Exception as e:
                    logging.info(f"Error in updating in db for xml hidden attachment {e}")
                    continue
            else:
                continue
    except Exception as e:
        logging.info(f"Exception occured in generating XBRL XML attachments {e}")
        return False
    else:
        return True


def xbrl_xml_attachment(db_config,cin,company_name):
    try:
        missing_years = check_missing_years(db_config,cin)
        if len(missing_years) == 0:
            print("All xbrl files are there")
            update_xbrl_flag(db_config,cin,'N')
            return True
        else:
            for missing_year in missing_years:
                filename = missing_year[4]
                date = missing_year[5]
                file_path = missing_year[8]
                output_directory = os.path.dirname(file_path)
                print(f"{filename} {date} {file_path}")
                hidden_attachement_insertion = get_embedded_pdfs(file_path,output_directory,None,db_config,cin,company_name,date)
                if hidden_attachement_insertion:
                    logging.info(f"Hidden attachments successfully saved and inserted for {filename}")
    except Exception as e:
        logging.info(f"Main Exception occured in XBRL Hidden attachments program {e}")
        return False
    else:
        update_xbrl_flag(db_config,cin,'Y')
        return True


def get_single_value_from_xml(xml_root, parent_node, child_node):
    try:
        namespaces = {
            'xsi': "http://www.w3.org/2001/XMLSchema-instance",
            'in-gaap': "http://www.icai.org/xbrl/taxonomy/2016-03-31/in-gaap",
            'link': "http://www.xbrl.org/2003/linkbase",
            'in-ca': "http://www.icai.org/xbrl/taxonomy/2016-03-31/in-ca",
            'iso4217': "http://www.xbrl.org/2003/iso4217",
            'xbrli': "http://www.xbrl.org/2003/instance",
            'xbrldi': "http://xbrl.org/2006/xbrldi",
            'xlink': "http://www.w3.org/1999/xlink"
        }
        setup_logging()
        if child_node == 'nan':
            elements = xml_root.findall(f'.//{parent_node}')
        else:
            elements = xml_root.findall(f'.//{parent_node}//{child_node}',namespaces)

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
        logging.info(f"An error occurred: {e}")
        return None


def update_xbrl_flag(db_config,cin,xbrl_status):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_locked_query = "update orders set run_xbrl = %s where cin=%s"
        values = (xbrl_status, cin)
        logging.info(update_locked_query % values)
        cursor.execute(update_locked_query, values)
        connection.commit()
    except Exception as e:
        print(f"Excpetion occured while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def nature(xml_file_path):
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        # Define the XBRL namespace
        namespaces = {
            'xbrli': 'http://www.xbrl.org/2003/instance',
            'xbrldi': 'http://xbrl.org/2006/xbrldi',
            'in-gaap': 'http://www.icai.org/xbrl/taxonomy/2016-03-31/in-gaap',
            'in-ca' : 'http://www.icai.org/xbrl/taxonomy/2016-03-31/in-ca'
        }
        nature_context = root.findall('.//in-ca:NatureOfReportStandaloneConsolidated', namespaces)
        if len(nature_context) == 0:
            namespaces['in-ca'] = 'http://www.icai.org/xbrl/taxonomy/2017-03-31/in-ca'
            filing_standard = 'IND-AS-Taxonomy'
            nature_context = root.findall('.//in-ca:NatureOfReportStandaloneConsolidated', namespaces)
            nature = nature_context[0].text
        else:
            nature = nature_context[0].text
            filing_standard = 'AS-Taxonomy'
        return nature,filing_standard
    except Exception as e:
        logging.info(f"Error in finding nature {e}")
        return None
