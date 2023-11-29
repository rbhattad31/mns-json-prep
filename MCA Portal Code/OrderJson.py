import json
from collections import OrderedDict


def order_json(config_dict,json_node,input_file_path):
    try:
        with open(input_file_path, "r") as file:
            json_data = json.load(file)

        # Define a single list for both top-level and "lei" object keys
        order_dict = config_dict[json_node]

        # Process the dictionary or a list of dictionaries
        company_data = json_data.get("data", {}).get(json_node)
        print(company_data)
        if isinstance(company_data, list):
            # If it's a list of dictionaries
            print("List")
            for index, company_object in enumerate(company_data):
                # Create an ordered dictionary with custom order of keys
                ordered_json = OrderedDict()
                for key, sub_keys in order_dict.items():
                    if (key == "network") and sub_keys:
                        # If the key is "network" and there are specific ordering requirements, process the inner list
                        network_data = company_object.get(key, {})
                        ordered_network_data = OrderedDict()
                        for sub_key in sub_keys:
                            if sub_key == "companies" or sub_key == "llps":
                                # If the sub-key is "companies," process each dictionary in the list
                                companies_list = network_data.get(sub_key, [])
                                if companies_list is not None:
                                    ordered_companies_list = [OrderedDict(
                                        (company_key, company.get(company_key, "")) for company_key in
                                        order_dict[key][sub_key]) for
                                        company in companies_list]
                                    ordered_network_data[sub_key] = ordered_companies_list

                            else:
                                # For other sub-keys, simply copy the value
                                ordered_network_data[sub_key] = network_data.get(sub_key, "")
                        ordered_json[key] = ordered_network_data
                    else:
                        if sub_keys:
                            # If there are specific ordering requirements for the key, create an ordered dictionary
                            if key in company_object and isinstance(company_object[key], list):
                                # If it's a list of dictionaries, process each dictionary in the list
                                ordered_json[key] = [
                                    OrderedDict((sub_key, item.get(sub_key, "")) for sub_key in sub_keys) for item in
                                    company_object[key]]
                                print("Converted")
                            elif key in company_object and isinstance(company_object[key], dict):
                                ordered_json[key] = OrderedDict()
                                for nested_key, nested_sub_keys in sub_keys.items():
                                    print(nested_key, nested_sub_keys)
                                    if nested_key in company_object[key]:
                                        # Process each sub-key within the nested dictionary
                                        if type(company_object[key][nested_key]) == str:
                                            continue
                                        ordered_json[key][nested_key] = OrderedDict(
                                            (sub_key, company_object[key][nested_key].get(sub_key, "")) for sub_key in
                                            nested_sub_keys
                                        )
                            else:
                                # If it's not a list of dictionaries, simply copy the value
                                ordered_json[key] = company_object.get(key, "")
                                print("Converted")
                        else:
                            # For other keys, simply copy the value
                            ordered_json[key] = company_object.get(key, "")

                # Update the original dictionary within the list with the ordered one
                company_data[index] = dict(ordered_json)

        elif isinstance(company_data, dict):
            # If it's a direct dictionary
            print("Dict")
            ordered_json = OrderedDict()
            for key, sub_keys in order_dict.items():
                if sub_keys:
                    # If there are specific ordering requirements for the key, create an ordered dictionary
                    if key in company_data and isinstance(company_data[key], list):
                        # If it's a list of dictionaries, process each dictionary in the list
                        print("List in dict")
                        ordered_json[key] = [OrderedDict((sub_key, item.get(sub_key, "")) for sub_key in sub_keys) for
                                             item in company_data[key]]
                    else:
                        # If it's not a list of dictionaries, simply copy the value
                        ordered_json[key] = company_data.get(key, "")
                else:
                    # For other keys, simply copy the value
                    ordered_json[key] = company_data.get(key, "")

            # Update the original dictionary with the ordered one
            json_data["data"][json_node] = dict(ordered_json)

        # Save the updated JSON data back to the input file
        with open(input_file_path, "w") as file:
            json.dump(json_data, file, indent=2)

        print(f"Updated JSON saved to {input_file_path}")
    except Exception as e:
        print(f"Error occured while ordering json {e}")
        return False
    else:
        return True
