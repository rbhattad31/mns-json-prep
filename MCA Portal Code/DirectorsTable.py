import mysql.connector
import json
from bs4 import BeautifulSoup
from datetime import datetime


def directors_table(db_config, cin):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        active_query = 'select * from authorized_signatories where cin = %s and extracted_from = "Master Data" order by cin,din'
        value = (cin,)
        cursor.execute(active_query, value)
        active_director_results = cursor.fetchall()
        cursor.close()
        connection.close()
        # Create a new BeautifulSoup object
        soup = BeautifulSoup(features="html.parser")

        # Create the table element
        table = soup.new_tag('table', style='width: 60%; border-collapse: collapse;')

        # Create table headers
        headers = ['DIN','PAN','Name','Status']
        header_row = soup.new_tag('tr')
        for header in headers:
            th = soup.new_tag('th', style='border: 1px solid black; padding: 8px;')
            th.string = header
            header_row.append(th)
        table.append(header_row)

        # Populate table with data
        for active_result in active_director_results:
            din = active_result[4]
            name = active_result[5]
            active_row = soup.new_tag('tr')
            pan = active_result[3]
            # Add data to the row
            active_data = [din,pan,name,'Active']
            for idx, item in enumerate(active_data):
                td = soup.new_tag('td', style='border: 1px solid black; padding: 8px;')
                td['style'] += 'color: black;'
                td.string = str(item)
                active_row.append(td)
            table.append(active_row)
            # Add row to table
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        inactive_query = 'select * from authorized_signatories where cin = %s and extracted_from is NULL and date_of_cessation is not NULL order by cin,din'
        value = (cin,)
        cursor.execute(inactive_query, value)
        inactive_director_results = cursor.fetchall()
        cursor.close()
        connection.close()
        for inactive_result in inactive_director_results:
            din = inactive_result[4]
            name = inactive_result[5]
            pan = inactive_result[3]
            inactive_row = soup.new_tag('tr')

            # Add data to the row
            inactive_data = [din,pan,name,'InActive']
            for idx, item in enumerate(inactive_data):
                td = soup.new_tag('td', style='border: 1px solid black; padding: 8px;')
                td['style'] += 'color: black;'
                td.string = str(item)
                inactive_row.append(td)
            table.append(inactive_row)
        # Append table to soup
        soup.append(table)

        # Return the HTML table as a string
        return str(soup)
    except Exception as e:
        print(f"Exception in generating Directors Table {e}")
        return None


def directors_shareholdings_table(db_config, cin):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        director_shareholdings_query = 'select * from director_shareholdings where cin = %s and din_pan != ""'
        value = (cin,)
        cursor.execute(director_shareholdings_query, value)
        director_shareholdings_results = cursor.fetchall()
        cursor.close()
        connection.close()
        # Create a new BeautifulSoup object
        soup = BeautifulSoup(features="html.parser")

        # Create the table element
        table = soup.new_tag('table', style='width: 60%; border-collapse: collapse;')

        # Create table headers
        headers = ['Name','Percentage_holding']
        header_row = soup.new_tag('tr')
        for header in headers:
            th = soup.new_tag('th', style='border: 1px solid black; padding: 8px;')
            th.string = header
            header_row.append(th)
        table.append(header_row)

        # Populate table with data
        for result in director_shareholdings_results:
            name = result[9]
            row = soup.new_tag('tr')
            try:
                percentage_holding = float(result[13])
                percentage_holding = round(percentage_holding,2)
            except Exception as e:
                percentage_holding = result[13]
            # Add data to the row
            data = [name,percentage_holding]
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
