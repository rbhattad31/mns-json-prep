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
            assets_difference = subtotals_dict['diffrence_value_of_assets']
            liabilities_difference = subtotals_dict['diffrence_value_of_liabilities']
            pnl_difference = pnl_dict['difference_value']

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
        print(f"Exception in generating Fianancials Table")
        return None
