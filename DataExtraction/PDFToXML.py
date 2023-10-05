import PyPDF2 as pypdf
import re


def extract_xfa_data(pdf_path):
    def findInDict(needle, haystack):
        for key in haystack.keys():
            try:
                value = haystack[key]
            except Exception as e:
                continue
            if key == needle:
                return value
            if isinstance(value, dict):
                x = findInDict(needle, value)
                if x is not None:
                    return x

    try:
        pdfobject = open(pdf_path, 'rb')
        pdf = pypdf.PdfReader(pdfobject)
        xfa = findInDict('/XFA', pdf.resolved_objects)
        if xfa and len(xfa) >= 10:
            xml = xfa[9].get_object().get_data()
            return xml
        else:
            raise Exception('XFA data not found or incomplete.')
    except FileNotFoundError as e:
        print(f"Error: The file '{pdf_path}' was not found.")
    except pypdf.utils.PdfReadError as e:
        print(f"Error: Failed to read PDF file '{pdf_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        try:
            pdfobject.close()
        except NameError:
            pass


def save_xfa_data_to_xml(xfa_data, output_path):
    if xfa_data:
        try:
            with open(output_path, 'wb') as xml_file:
                xml_file.write(xfa_data)
            print(f'XFA data has been saved to {output_path}')
        except FileNotFoundError as e:
            print(f"Error: The file '{output_path}' could not be created or accessed.")
        except Exception as e:
            print(f"An error occurred while saving the XML file: {e}")
    else:
        print('No XFA data found in the PDF.')


if __name__ == '__main__':
    pdf_file_path = "Input/Form MGT-7-22092022_signed - Json data L&T.pdf"
    xml_file_path = 'L99999MH1946PLC004768.xml'

    xfa_data = extract_xfa_data(pdf_file_path)
    save_xfa_data_to_xml(xfa_data, xml_file_path)
