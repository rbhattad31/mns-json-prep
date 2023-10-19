import mysql.connector
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
            cursor.execute(query, ("XML_Pending",))

            # Get the column names from the cursor description
            column_names = [desc[0] for desc in cursor.description]

            # Fetch all the rows
            rows = cursor.fetchall()
            Status="Pass"
            return column_names, rows,Status

    except mysql.connector.Error as error:
        print("Error:", error)
        return None


def update_status(user,Status,db_config):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        query = "UPDATE orders SET workflow_status = %s WHERE payment_by_user = %s"
        print(query)
        cursor.execute(query, (Status,user,))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
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
    query = "SELECT * FROM documents where cin=%s and Download_Status='Downloaded' and form_data_extraction_status='Success'"
    value = (Cin,)
    print(query % value)
    cursor.execute(query,value)
    files_to_insert = cursor.fetchall()
    return files_to_insert