import json
from datetime import datetime


# Load the JSON data from the file
def order_epfo(json_file_path,json_node):
    try:
        with open(json_file_path, "r") as json_file:
            input_data = json.load(json_file)

        # Assuming 'epfo' is the key in your input JSON
        epfo_data = input_data.get("data", {}).get(json_node)

        # Define a function to parse the date string and return a datetime object
        def parse_date(date_str):
            return datetime.strptime(date_str, "%d-%b-%Y %H:%M:%S")

        # Sort each 'epfo' node's payment_details based on the "date_of_credit" key
        for epfo_entry in epfo_data:
            epfo_entry["payment_details"] = sorted(epfo_entry.get("payment_details", []), key=lambda x: parse_date(x["date_of_credit"]), reverse=True)

        input_data["data"][json_node] = epfo_data
        # Save the updated data back to the JSON file
        with open(json_file_path, "w") as json_file:
            json.dump(input_data, json_file, indent=2)
    except Exception as e:
        print("Error in ordering epfo according to date")
        return False
    else:
        return True

# json_file_path = r"C:\Users\BRADSOL123\Documents\U74110DL2007FTC166838_14_12_2023.json"# Replace with your file path
# json_node = 'epfo'
# order_epfo(json_file_path,json_node)