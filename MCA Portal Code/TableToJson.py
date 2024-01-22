import tabula
import pandas as pd
import json
import fitz
import logging
from logging_config import setup_logging
import sys
import traceback
import os
from PyPDF2 import PdfReader, PdfWriter

def extract_tables_to_temp_pdf(input_pdf, temp_pdf):
    pdf_reader = PdfReader(input_pdf)
    pdf_writer = PdfWriter()

    for page_num in range(len(pdf_reader.pages)):
        try:
            # Try to extract tables from the current page
            tables = tabula.read_pdf(input_pdf, pages=page_num + 1, multiple_tables=True)

            # If tables are extracted, add the page to the temporary PDF
            pdf_writer.add_page(pdf_reader.pages[page_num])

        except Exception as e:
            print(f"Error processing page {page_num + 1}: {e}")

    # Save the temporary PDF file
    with open(temp_pdf, 'wb') as temp_pdf_file:
        pdf_writer.write(temp_pdf_file)
    return True


def json_generation(pdf_file, json_file_path):
    try:
        setup_logging()

        # Call delete_page_with_no_tables to create a temporary PDF
        temp_pdf_file = "temp.pdf"
        if not extract_tables_to_temp_pdf(pdf_file, temp_pdf_file):
            # Handle the case where deleting pages fails
            return False

        # Continue with the rest of the JSON generation using the temporary PDF
        tables = tabula.read_pdf(temp_pdf_file, pages='all', multiple_tables=True)

        all_data = []

        for table_num, df in enumerate(tables):
            table_data = df.values.tolist()
            headers = df.columns.tolist()
            if not isinstance(table_data, list):
                table_data = [table_data]
            table_data = [headers] + table_data
            table_dict = {
                f"table_{table_num}": table_data
            }

            all_data.append(table_dict)

        doc = fitz.open(temp_pdf_file)
        plain_text_by_page = []

        for page_num in range(doc.page_count):
            page = doc.load_page(page_num)
            plain_text = page.get_text()
            plain_text_by_page.append(plain_text)

        all_data.append({"plain_text_by_page": plain_text_by_page})

        json_data = json.dumps(all_data, indent=2)

        with open(json_file_path, "w") as json_file:
            json_file.write(json_data)

        # Remove the temporary PDF file
        #os.remove(temp_pdf_file)
    except Exception as e:
        logging.error(f"Exception occurred while generating json for xbrl {e}")
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)

        for line in traceback_details:
            logging.info(line.strip())
        return False
    else:
        return True
