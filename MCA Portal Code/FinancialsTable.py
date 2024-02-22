import mysql.connector
import json
from bs4 import BeautifulSoup
from datetime import datetime


def financials_table(db_config, cin):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = 'select * from financials where cin = %s order by year DESC'
        value = (cin,)
        cursor.execute(query, value)
        results = cursor.fetchall()

        # Create a new BeautifulSoup object
        soup = BeautifulSoup(features="html.parser")

        # Create the table element
        table = soup.new_tag('table', style='width: 60%; border-collapse: collapse;')

        # Create table headers
        headers = ['Year', 'Taxonomy', 'Nature', 'Difference value of Assets', 'Difference value of Liabilities',
                   'PNL Difference']
        header_row = soup.new_tag('tr')
        for header in headers:
            th = soup.new_tag('th', style='border: 1px solid black; padding: 8px;')
            th.string = header
            header_row.append(th)
        table.append(header_row)

        # Populate table with data
        for result in results:
            year = result[6]
            try:
                date_obj = datetime.strptime(year, '%d/%m/%Y')
                year = date_obj.strftime('%Y')
            except:
                pass

            try:
                date_obj = datetime.strptime(year, '%Y-%m-%d')
                year = date_obj.strftime('%Y')
            except:
                pass
            taxonomy = result[10]
            nature = result[8]
            subtotals = result[13]
            pnl_items = result[14]
            subtotals_dict = json.loads(subtotals)
            pnl_dict = json.loads(pnl_items)
            try:
                assets_difference = subtotals_dict['diffrence_value_of_assets']
            except:
                assets_difference = None
            try:
                liabilities_difference = subtotals_dict['diffrence_value_of_liabilities']
            except:
                liabilities_difference = None
            try:
                pnl_difference = pnl_dict['difference_value']
            except:
                pnl_difference = None
            # Create a new row
            row = soup.new_tag('tr')

            # Add data to the row
            data = [year, taxonomy, nature, assets_difference, liabilities_difference, pnl_difference]
            for idx, item in enumerate(data):
                td = soup.new_tag('td', style='border: 1px solid black; padding: 8px;')
                if idx >= 3 and item != 0:
                    td['style'] += 'background-color: red; color: black;'
                else:
                    td['style'] += 'color: black;'
                td.string = str(item)
                row.append(td)

            # Add row to table
            table.append(row)

        # Append table to soup
        soup.append(table)

        # Return the HTML table as a string
        return str(soup)
    except Exception as e:
        print(f"Exception in generating Fianancials Table {e}")
        return None


def aoc_files_table(db_config, cin):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        files_query = 'select * from documents where cin = %s and (document like "%%AOC%%" or document like "%%XBRL document in respect Consolidated%%"  or document like "%%XBRL financial statements%%") and form_data_extraction_needed = "Y" and form_data_extraction_status = "Failure"'
        value = (cin,)
        cursor.execute(files_query, value)
        file_results = cursor.fetchall()
        cursor.close()
        connection.close()
        # Create a new BeautifulSoup object
        if len(file_results) != 0:
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
        else:
            soup = "All financials files are processed successfully"
            return soup
    except Exception as e:
        print(f"Exception in generating Directors Table {e}")
        return None
