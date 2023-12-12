import tabula
import pandas as pd
import json
import fitz
import logging
from logging_config import setup_logging  # PyMuPDF library for extracting text
# Specify the PDF file path
def json_generation(pdf_file,json_file_path):
    try:
        setup_logging()
        tables = tabula.read_pdf(pdf_file, pages='all', multiple_tables=True)

        # Initialize an empty list to hold the table data and plain text
        all_data = []

        # Loop through the extracted tables
        for table_num, df in enumerate(tables):
            # Convert the DataFrame to a list of lists
            table_data = df.values.tolist()

            # Create a dictionary for each table
            table_dict = {
                f"table_{table_num}": table_data
            }

            all_data.append(table_dict)

        # Use PyMuPDF to extract plain text from the PDF and separate it by page
        doc = fitz.open(pdf_file)
        plain_text_by_page = []

        for page_num in range(doc.page_count):
            page = doc.load_page(page_num)
            plain_text = page.get_text()
            plain_text_by_page.append(plain_text)

        # Add the plain text by page to the data
        all_data.append({"plain_text_by_page": plain_text_by_page})

        # Convert the list of dictionaries to a JSON string
        json_data = json.dumps(all_data, indent=2)

        # Save the JSON data to a file
        with open(json_file_path, "w") as json_file:
            json_file.write(json_data)
    except Exception as e:
        logging.error(f"Exception occured while generating json for xbrl {e}")
        return False
    else:
        return True

#pdf_file = r"C:\Users\mns-admin\Documents\POWERA~1\MNSCRE~1\Output\U50400~1\TVSMOB~1\OTHERA~1\XBRLFI~1.PDF"
#json_file_path = 'output.json'
#json_generation(pdf_file,json_file_path)

# Use tabula.read_pdf to extract all tables

