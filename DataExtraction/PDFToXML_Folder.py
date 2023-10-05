import PyPDF2 as pyPDF
from pathlib import Path


def extract_xfa_data(arg_pdf_path):
    pdf_object = None

    def find_in_dict(needle, haystack):
        for key in haystack.keys():
            try:
                value = haystack[key]
            except Exception as e:
                continue
            if key == needle:
                return value
            if isinstance(value, dict):
                x = find_in_dict(needle, value)
                if x is not None:
                    return x

    try:
        pdf_object = open(arg_pdf_path, 'rb')
        pdf = pyPDF.PdfReader(pdf_object)
        xfa = find_in_dict('/XFA', pdf.resolved_objects)
        if xfa is not None and len(xfa) >= 10:
            xml = xfa[9].get_object().get_data()
            return xml
        else:
            raise Exception('XFA data not found or incomplete.')
    except FileNotFoundError as e:
        print(e)
        print(f"Error: The file '{arg_pdf_path}' was not found.")
    except pyPDF.utils.PdfReadError as e:
        print(e)
        print(f"Error: Failed to read PDF file '{arg_pdf_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        try:
            pdf_object.close()
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
    folder_path = Path('Input')
    try:
        if folder_path.exists():
            file_names = [file.name for file in folder_path.iterdir() if file.is_file() and Path(file).suffix == ".pdf"]
            print(f'{file_names}')
            for file_name in file_names:
                print("file name: ", file_name)
                pdf_path = str(folder_path / file_name)
                print("pdf file path: ", pdf_path)
                xml_file_path = str(folder_path / file_name.replace('.pdf', '.xml'))
                print("new xml file path: ", xml_file_path)
                xfa_data = extract_xfa_data(pdf_path)
                print("extracted xfa data")
                save_xfa_data_to_xml(xfa_data, xml_file_path)
                # print("Extracted xfa data for ", file_name)
                # print("Saved to: ", xml_file_path)
        else:
            print("The Folder is not available - {0}", {folder_path})
    except Exception as e:
        print(e)

