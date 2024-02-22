import mysql.connector
import json
from bs4 import BeautifulSoup
from datetime import datetime


def files_table(db_config, cin):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        files_query = 'select * from documents where cin = %s and document like "%%MGT%%" and form_data_extraction_needed = "Y"'
        value = (cin,)
        cursor.execute(files_query, value)
        file_results = cursor.fetchall()
        cursor.close()
        connection.close()
        # Create a new BeautifulSoup object
        soup = BeautifulSoup(features="html.parser")

        # Create the table element
        table = soup.new_tag('table', style='width: 60%; border-collapse: collapse;')

        # Create table headers
        headers = ['Name','Date','Status']
        header_row = soup.new_tag('tr')
        for header in headers:
            th = soup.new_tag('th', style='border: 1px solid black; padding: 8px;')
            th.string = header
            header_row.append(th)
        table.append(header_row)

        # Populate table with data
        for result in file_results:
            name = result[4]
            date = result[5]
            row = soup.new_tag('tr')
            status = result[10]
            # Add data to the row
            data = [name,date,status]
            for idx, item in enumerate(data):
                td = soup.new_tag('td', style='border: 1px solid black; padding: 8px;')
                td['style'] += 'color: black;'
                td.string = str(item)
                row.append(td)
            table.append(row)
        soup.append(table)

        # Return the HTML table as a string
        return str(soup)
    except Exception as e:
        print(f"Exception in generating Directors Table {e}")
        return None
