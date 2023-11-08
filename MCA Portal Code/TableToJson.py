import tabula
import pandas as pd
import json
import fitz  # PyMuPDF library for extracting text

# Specify the PDF file path
def json_generation(pdf_file,json_file_path):
    try:
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
        print(f"Exception occured while generating json for xbrl {e}")
        return False
    else:
        return True

#pdf_file = r"C:\MCA portal Test\Extracted PDFs\XML\18_XBRL (110000) document in respect Consolidated financial statement-19012022_19-01-2022.pdf"

# Use tabula.read_pdf to extract all tables

