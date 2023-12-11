import mysql.connector
import json
import os
def connect_to_database(db_config):
    try:
        # Connect to the MySQL server
        connection = mysql.connector.connect(**db_config)
        db_cursor = connection.cursor()
        if connection.is_connected():
            return connection,db_cursor
        else:
            print("Connection is not established.")
            return None,None

    except mysql.connector.Error as error:
        print("Error:", error)
        return None

def fetch_order_data_from_table(connection):
    try:
        if connection:
            cursor = connection.cursor()
            # Construct the SQL query
            query = "SELECT * FROM orders where workflow_status=%s"
            #value1 = ("Download_Pending")
            cursor.execute(query, ("json_loader_pending",))

            # Get the column names from the cursor description
            column_names = [desc[0] for desc in cursor.description]

            # Fetch all the rows
            rows = cursor.fetchall()
            Status="Pass"
            connection.close()
            return column_names, rows,Status

    except mysql.connector.Error as error:
        print("Error:", error)
        return None


def fetch_workflow_status(db_config,cin):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    query = "select workflow_status from orders where cin=%s"
    values = (cin,)
    print(query % values)
    cursor.execute(query,values)
    workflow_status = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    return workflow_status

def update_status(user,Status,db_config,cin):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        query = "UPDATE orders SET workflow_status = %s WHERE payment_by_user = %s and cin=%s"
        print(query)
        cursor.execute(query, (Status,user,cin))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
    finally:
        cursor.close()
        connection.close()

def update_locked_by(dbconfig,Cin):
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    try:
        update_locked_query = "update orders set locked_by = %s where cin=%s"
        user = os.getlogin()
        values = (user, Cin)
        cursor.execute(update_locked_query, values)
        connection.commit()
    except Exception as e:
        print(f"Excpetion occured while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()
def update_logout_status(username, db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        query = "UPDATE user_details SET login_status = 'No' WHERE user_name = %s"
        cursor.execute(query, (username,))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
    finally:
        cursor.close()
        connection.close()


def fetch_user_credentials_from_db(db_config,input_user):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        query = "SELECT user_name, password, login_status FROM user_details"
        cursor.execute(query)
        records = cursor.fetchall()
        #input_user = 'Shalumns1'
        print("Input recieved from Main" ,input_user)

        for db_username, db_password, login_status in records:
            if db_username == input_user and login_status == 'No':
                print("Ready to Login")
                Status="Pass"
                break
            else:
                Status="Fail"

        if Status=="Pass":
            #user_credentials={'username':db_username,
                          #'password':db_password}
            #print(user_credentials)
            return db_username,db_password,Status
        else:
            return None,None,Status

    except Exception as e:
        print(f"Error fetching user credentials from the database: {str(e)}")
        return []

    finally:
        cursor.close()
        connection.close()

def get_db_credentials(config_dict):
    Host = config_dict['Host']
    db_User = config_dict['User']
    Password = config_dict['Password']
    Database = config_dict['Database']
    db_config = {
        "host": Host,
        "user": db_User,
        "password": Password,
        "database": Database,
        "connect_timeout": 6000
    }
    return db_config

def update_xml_extraction_status(Cin,Filename,config_dict,Status):
    db_config = get_db_credentials(config_dict)
    connection,cursor = connect_to_database(db_config)
    update_data_extraction_query = "update documents set form_data_extraction_status=%s where document=%s and cin=%s"
    update_data_extraction_values = (Status,Filename, Cin)
    cursor.execute(update_data_extraction_query,update_data_extraction_values)
    print(update_data_extraction_query % update_data_extraction_values)
    connection.commit()

def get_xml_to_insert(Cin,config_dict):
    db_config = get_db_credentials(config_dict)
    connection, cursor = connect_to_database(db_config)
    query = "SELECT * FROM documents where cin=%s and Download_Status='Downloaded' and form_data_extraction_status='Success' and DB_insertion_status='Pending'"
    value = (Cin,)
    print(query % value)
    cursor.execute(query,value)
    files_to_insert = cursor.fetchall()
    return files_to_insert

def update_database_single_value(db_config, table_name, cin_column_name, cin_value,company_name_column_name,company_name, column_name, column_value):
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}'".format(table_name, cin_column_name, cin_value,company_name_column_name,company_name)
    print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        print(err)
    result = db_cursor.fetchall()
    # print(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(table_name, column_name,
                                                                                      column_value, cin_column_name,
                                                                                      cin_value,
                                                                                      company_name_column_name,
                                                                                      company_name)
        # print(update_query)
        db_cursor.execute(update_query)
        print("Updating")
    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      column_name,
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      column_value)
        # print(insert_query)
        db_cursor.execute(insert_query)
        print("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def update_db_insertion_status(Cin,Filename,config_dict,Status):
    db_config = get_db_credentials(config_dict)
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    update_db_insertion_query = "update documents set DB_insertion_status=%s where document=%s and cin=%s"
    update_db_insertion_values = (Status,Filename, Cin)
    print(update_db_insertion_query % update_db_insertion_values)
    cursor.execute(update_db_insertion_query,update_db_insertion_values)
    connection.commit()
    cursor.close()
    connection.close()

def update_json_loader_db(cindata,config_dict):
    db_config = get_db_credentials(config_dict)
    cin = cindata[2]
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    update_loader_query = "update orders set workflow_status='Loader_generated' where cin=%s"
    values = (cin,)
    cursor.execute(update_loader_query,values)
    print(update_loader_query % values)
    connection.commit()
    cursor.close()
    connection.close()
