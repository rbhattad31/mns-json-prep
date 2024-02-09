import mysql.connector
from tabulate import tabulate
from bs4 import BeautifulSoup


def FinalTable(db_config,cin):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Define the CIN number

        # Execute query to fetch data
        query = """
            SELECT 
                company_information_status, company_information_comments,
                panextraction_status, panextraction_comments,
                nse_status, nse_exception_message,
                bse_status, bse_exception_message,
                nextcin_status, nextcin_comment,
                lei_status, lei_exception_message,
                legal_history_status, legal_history_comments,
                infomerics_Status, infomerics_comments,
                india_rating_status, india_rating_comments,
                crisil_status, crisil_comments,
                care_status, care_comments,
                brickwork_status, brickwork_comments,
                acuite_status, acuite_comments,
                icra_status, icra_comments,
                gst_status, gst_exception_message,
                index_charges_status, index_charges_comments,
                google_news_status, google_news_exception_message,
                epfo_status, epfo_comments,
                din_status, din_comments
            FROM orders
            WHERE cin = %s
        """

        cursor.execute(query, (cin,))
        result = cursor.fetchone()

        # Close the database connection
        conn.close()

        # Format the data into a table
        html_table = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        table {{
            border-collapse: collapse;
            width: 40%;
        }}
        th, td {{
            border: 1px solid black;
            padding: 8px;
            text-align: left;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        tr.red {{
            background-color: red;
            color: black;
        }}
    </style>
</head>
<body>
    <table>
        <tr>
            <th>Category</th>
            <th>Status</th>
            <th>Comments</th>
        </tr>
        <tr class="{ 'red' if result[0] in ['N', 'P'] else '' }">
            <td>Company Information</td>
            <td>{result[0]}</td>
            <td>{result[1] if result[1] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[2] in ['N', 'P'] else '' }">
            <td>PAN (For LLP)</td>
            <td>{result[2]}</td>
            <td>{result[3] if result[3] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[4] in ['N', 'P'] else '' }">
            <td>NSE</td>
            <td>{result[4]}</td>
            <td>{result[5] if result[5] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[6] in ['N', 'P'] else '' }">
            <td>BSE</td>
            <td>{result[6]}</td>
            <td>{result[7] if result[7] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[8] in ['N', 'P'] else '' }">
            <td>Old Cin</td>
            <td>{result[8]}</td>
            <td>{result[9] if result[9] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[10] in ['N', 'P'] else '' }">
            <td>LEI</td>
            <td>{result[10]}</td>
            <td>{result[11] if result[11] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[12] in ['N', 'P'] else '' }">
            <td>Legal Cases</td>
            <td>{result[12]}</td>
            <td>{result[13] if result[13] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[14] in ['N', 'P'] else '' }">
            <td>Infomerics Rating</td>
            <td>{result[14]}</td>
            <td>{result[15] if result[15] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[16] in ['N', 'P'] else '' }">
            <td>India Rating</td>
            <td>{result[16]}</td>
            <td>{result[17] if result[17] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[18] in ['N', 'P'] else '' }">
            <td>CRISIL Rating</td>
            <td>{result[18]}</td>
            <td>{result[19] if result[19] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[20] in ['N', 'P'] else '' }">
            <td>CARE Rating</td>
            <td>{result[20]}</td>
            <td>{result[21] if result[21] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[22] in ['N', 'P'] else '' }">
            <td>Brickwork Rating</td>
            <td>{result[22]}</td>
            <td>{result[23] if result[23] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[24] in ['N', 'P'] else '' }">
            <td>Acuite Rating</td>
            <td>{result[24]}</td>
            <td>{result[25] if result[25] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[26] in ['N', 'P'] else '' }">
            <td>ICRA Rating</td>
            <td>{result[26]}</td>
            <td>{result[27] if result[27] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[28] in ['N', 'P'] else '' }">
            <td>GST</td>
            <td>{result[28]}</td>
            <td>{result[29] if result[29] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[30] in ['N', 'P'] else '' }">
            <td>Index Charges Check</td>
            <td>{result[30]}</td>
            <td>{result[31] if result[31] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[32] in ['N', 'P'] else '' }">
            <td>News</td>
            <td>{result[32]}</td>
            <td>{result[33] if result[33] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[34] in ['N', 'P'] else '' }">
            <td>EPFO</td>
            <td>{result[34]}</td>
            <td>{result[35] if result[35] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[36] in ['N', 'P'] else '' }">
            <td>Directors Data</td>
            <td>{result[36]}</td>
            <td>{result[37] if result[37] is not None else ''}</td>
        </tr>
    </table>
</body>
</html>
"""

        # Use BeautifulSoup to format the HTML table
        soup = BeautifulSoup(html_table, 'html.parser')
        formatted_table = soup.prettify()

        # Print or use the formatted table
        print(formatted_table)
        # Print or store the table
        return formatted_table
    except Exception as e:
        print(f"Error in fetching final email table {e}")
        return None
