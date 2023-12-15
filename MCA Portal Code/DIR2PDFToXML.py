import fitz
import xml.etree.ElementTree as ET
import pytesseract
from PIL import Image
import re

def image_to_text(image_path):
    return pytesseract.image_to_string(Image.open(image_path), lang='eng')

def find_pan(text):
    pan_pattern = re.compile(r'\b[A-Z]{5}[0-9]{4}[A-Z]\b')
    pan_match = re.search(pan_pattern, text)
    return pan_match.group() if pan_match else None

def find_mobile_number(text):
    mobile_pattern = re.compile(r'\b\d{10}\b')
    mobile_match = re.search(mobile_pattern, text)
    return mobile_match.group() if mobile_match else None

def find_email(text):
    email_pattern = re.compile(r'\b[\w\.-]+@[\w\.-]+\.\w+\b')
    email_match = re.search(email_pattern, text)
    return email_match.group() if email_match else None

def find_name(text):
    name_pattern = re.compile(r'Name(?: \(in full\))?:\s*([^\n]*)')  # Assumes a simple first name and last name pattern
    name_match = re.search(name_pattern, text)
    return name_match.group(1) if name_match else None

def find_din(text):
    din_pattern = re.compile(r'\b\d{8}\b')
    din_match = re.search(din_pattern, text)
    return din_match.group() if din_match else None


def DIR2_pdf_to_xml(pdf_path):
    pdf_document = fitz.open(pdf_path)
    xml_path = str(pdf_path).replace('.pdf','.xml')
    with open(xml_path, "w", encoding="utf-8") as xml_file:
        xml_file.write("<?xml version='1.0' encoding='utf-8'?>\n")
        xml_file.write("<PDFData>\n")

    for page_num in range(pdf_document.page_count):
        page = pdf_document.load_page(page_num)
        page_text = page.get_text()
        page_data = ET.Element("PageData")

        for img_index, image in enumerate(page.get_images(full=True)):
            xref = image[0]
            base_image = pdf_document.extract_image(xref)
            image_data = base_image["image"]

            with open(f"temp_image_{img_index}.png", "wb") as img_file:
                img_file.write(image_data)
            text = image_to_text(f"temp_image_{img_index}.png")

            text_element = ET.SubElement(page_data, "Text")
            text_element.text = text

            # Find and add PAN, Mobile Number, and Email if present
            pan = find_pan(text)
            if pan:
                pan_element = ET.SubElement(page_data, "PAN")
                pan_element.text = pan

            mobile = find_mobile_number(text)
            if mobile:
                mobile_element = ET.SubElement(page_data, "MobileNumber")
                mobile_element.text = mobile

            email = find_email(text)
            if email:
                email_element = ET.SubElement(page_data, "Email")
                email_element.text = email

            name = find_name(text)
            if name:
                name_element = ET.SubElement(page_data, "Name")
                name_element.text = name
                
            din = find_din(text)
            print(din)
            if din:
                din_element = ET.SubElement(page_data, "DIN")
                din_element.text = din

        tree = ET.ElementTree(page_data)
        with open(xml_path, "ab") as xml_file:
            tree.write(xml_file, encoding="utf-8")

    with open(xml_path, "a", encoding="utf-8") as xml_file:
        xml_file.write("</PDFData>\n")

    return xml_path,True

