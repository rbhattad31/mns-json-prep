import fitz
import xml.etree.ElementTree as ET
import re


def sanitize_xml_node_name(name):
    # Remove invalid characters from the field name to create a valid XML node name
    sanitized_name = re.sub(r'\W|^(?=\d)', '_', name)
    return sanitized_name


def extract_form_data(pdf_path, output_xml_path):
    try:
        # Create the root element of the XML tree
        root = ET.Element("form_data")

        # Open the PDF file
        pdf_document = fitz.open(pdf_path)

        # Iterate through each page in the PDF
        for page_number in range(pdf_document.page_count):
            page = pdf_document[page_number]

            # Get all form widgets on the page
            widgets = page.widgets()

            # Iterate through each form widget
            for widget in widgets:
                # Check if the widget is a form field
                if widget.field_type:
                    # Get the field name and value
                    field_name = widget.field_name

                    # Use the page object to get the text from the widget
                    field_value = page.get_text("text", clip=widget.rect)

                    # Sanitize the field name to create a valid XML node name
                    sanitized_name = sanitize_xml_node_name(field_name)

                    # Create a new XML element for the field
                    field_element = ET.SubElement(root, sanitized_name)
                    field_element.text = field_value

        # Close the PDF file
        pdf_document.close()

        # Create an ElementTree object from the root
        tree = ET.ElementTree(root)

        # Write the XML tree to a file
        tree.write(output_xml_path, encoding="utf-8", xml_declaration=True)

    except Exception as e:
        print(f"Exception occured while converting to xml for Form 18 old files")
        return False
    else:
        return True

# Example usage

# Replace 'your_pdf_file.pdf' with the path to your PDF file
# pdf_path = r"C:\Users\BRADSOL123\Documents\Form 18-130606.PDF"
#
# # Replace 'output.xml' with the desired output XML file path
# output_xml_path = r"C:\Users\BRADSOL123\Documents\Form 18-130606.xml"
#
# extract_form_data(pdf_path, output_xml_path)
