import mysql.connector
import json
import os
from logging_config import setup_logging
import logging
import time
import datetime
from datetime import datetime


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
            setup_logging()
            cursor = connection.cursor()
            # Construct the SQL query
            query = "SELECT * FROM orders where process_status=%s and workflow_status in ('XML_Pending','db_insertion_pending','Loader_pending') and (python_locked_by = '' or python_locked_by is NULL)"
            #value1 = ("Download_Pending")
            cursor.execute(query, ('InProgress',))
            logging.info(query, ('InProgress',))
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
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(buffered=True)
    query = "select workflow_status from orders where cin=%s"
    values = (cin,)
    logging.info(query % values)
    cursor.execute(query,values)
    workflow_status = cursor.fetchone()[0]
    time.sleep(1)
    cursor.close()
    connection.close()
    return workflow_status

def update_process_status(Status,db_config,cin, database_id):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        query = "UPDATE orders SET process_status = %s WHERE cin=%s and id = %s"
        print(query)
        cursor.execute(query, (Status,cin,database_id))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
    finally:
        cursor.close()
        connection.close()

def update_status(user,Status,db_config,cin,database_id):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        query = "UPDATE orders SET workflow_status = %s WHERE payment_by_user = %s and cin=%s and id = %s"
        print(query)
        cursor.execute(query, (Status,user,cin,database_id))
        connection.commit()
    except Exception as e:
        print(f"Error updating login status in the database: {str(e)}")
    finally:
        cursor.close()
        connection.close()

def update_locked_by(dbconfig,Cin,database_id):
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    try:
        update_locked_query = "update orders set python_locked_by = %s where cin=%s and id = %s"
        user = os.environ.get('SystemName')
        values = (user, Cin,database_id)
        cursor.execute(update_locked_query, values)
        connection.commit()
    except Exception as e:
        print(f"Excpetion occured while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()

def update_locked_by_empty(dbconfig,Cin,database_id):
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    try:
        update_locked_query = "update orders set python_locked_by = '' where cin=%s and id = %s"
        values = (Cin,database_id)
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
    query = f"SELECT * FROM documents where cin='{Cin}' and Download_Status='Downloaded' and form_data_extraction_status='Success' and DB_insertion_status='Pending' and form_data_extraction_needed = 'Y' ORDER BY STR_TO_DATE(document_date_year, '%d-%m-%Y') DESC"
    print(query)
    cursor.execute(query)
    files_to_insert = cursor.fetchall()
    return files_to_insert

def update_database_single_value(db_config, table_name, cin_column_name, cin_value,company_name_column_name,company_name, column_name, column_value,name,date):
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
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and {} = '{}' and {} = '{}'".format(table_name, cin_column_name, cin_value,company_name_column_name,company_name,'name',name,'date',date)
    print(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        print(err)
    result = db_cursor.fetchall()
    # print(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {} = '{}' AND {} = '{}'".format(table_name, column_name,
                                                                                      column_value, cin_column_name,
                                                                                      cin_value,
                                                                                      company_name_column_name,
                                                                                      company_name,
                                                                                      'name',
                                                                                      name,
                                                                                      'date',
                                                                                      date)
        # print(update_query)
        db_cursor.execute(update_query)
        print("Updating")
    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {},{}) VALUES ('{}', '{}', '{}','{}')".format(table_name, cin_column_name,
                                                                                      company_name_column_name,
                                                                                      'name',
                                                                                      'date',
                                                                                      cin_value,
                                                                                      company_name,
                                                                                      name,
                                                                                      date
                                                                                        )
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
    database_id = cindata[0]
    cin = cindata[2]
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    update_loader_query = "update orders set workflow_status='Loader_generated' where cin=%s and id = %s"
    values = (cin,database_id)
    cursor.execute(update_loader_query,values)
    print(update_loader_query % values)
    connection.commit()
    cursor.close()
    connection.close()


def check_files_and_update(cin,db_config,database_id):
    try:
        non_llp_files_type = ['AOC-4','AOC-4 NBFC','AOC-4 CFS NBFC','XBRL','CHG-1','DIR-12','MSME','MGT-7','CHANGE OF NAME']
        llp_file_types = ['Form8','Form11','CHANGE OF NAME','FiLLiP']
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        python_comments = ''
        connection.autocommit = True
        if len(cin) == 21:
            for file_type in non_llp_files_type:
                check_query = "select * from documents where form_data_extraction_needed = 'Y' and cin = '{}' and document like '%{}%'".format(cin,file_type)
                print(check_query)
                cursor.execute(check_query)
                result = cursor.fetchall()
                if len(result) == 0:
                    comment = "{} file not found".format(file_type)
                    python_comments += comment
                    python_comments = python_comments + " "

        elif len(cin) == 8:
            for file_type in llp_file_types:
                check_query = "select * from documents where form_data_extraction_needed = 'Y' and cin = '{}' and document like '%{}%'".format(cin,file_type)
                print(check_query)
                cursor.execute(check_query)
                result = cursor.fetchall()
                if len(result) == 0:
                    comment = "{} file not found".format(file_type)
                    python_comments += comment
                    python_comments = python_comments + " "

        update_query = 'update orders set python_comments = %s where cin = %s and id = %s'
        update_query_values = (python_comments,cin,database_id)
        print(update_query % update_query_values)
        cursor.execute(update_query,update_query_values)
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error in updating missing file status {e}")

def fetch_download_status(db_config,cin):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(buffered=True)
    query = "select document_download_status from orders where cin=%s"
    values = (cin,)
    logging.info(query % values)
    cursor.execute(query,values)
    download_status = cursor.fetchone()[0]
    time.sleep(1)
    cursor.close()
    connection.close()
    return download_status


def fetch_order_download_data_from_table(connection):
    try:
        if connection:
            setup_logging()
            user = os.environ.get('SystemName')
            cursor = connection.cursor()
            # Construct the SQL query
            query = f"SELECT * FROM orders where process_status in ('InProgress','Exception') and payment_by_user!='' and document_download_status = 'N' and (workflow_status = 'XML_Pending' or workflow_status = 'Payment_success') and (python_locked_by = '' or python_locked_by is NULL or python_locked_by in ('{user}') ) order by (CASE WHEN workflow_status = 'XML_Pending' THEN 0 ELSE 1 END),(CASE WHEN created_date > NOW() - INTERVAL 1 HOUR THEN 0 ELSE 1 END),modified_date,pad_pro_startdate LIMIT 1"
            #value1 = ("Download_Pending")
            logging.info(query)
            cursor.execute(query)
            # Get the column names from the cursor description
            column_names = [desc[0] for desc in cursor.description]

            # Fetch all the rows
            rows = cursor.fetchall()
            result_list = [list(row) for row in rows]
            Status="Pass"
            connection.close()
            return column_names, result_list,Status

    except mysql.connector.Error as error:
        print("Error:", error)
        return None


def update_modified_date(db_config,cin,database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_locked_query = "update orders set modified_date = %s where cin=%s and id = %s"
        current_date = datetime.now()
        today_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
        values = (today_date, cin, database_id)
        logging.info(update_locked_query % values)
        cursor.execute(update_locked_query, values)
        connection.commit()
    except Exception as e:
        print(f"Excpetion occured while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()
        

def update_retry_count(db_config,cin,retry_counter,database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_retry_counter_query = "update orders set retry_counter = %s where cin=%s and id = %s"
        values = (retry_counter, cin,database_id)
        logging.info(update_retry_counter_query % values)
        cursor.execute(update_retry_counter_query, values)
        connection.commit()
    except Exception as e:
        print(f"Excpetion occured while updating retry counter by {e}")
    finally:
        cursor.close()
        connection.close()


def get_retry_count(db_config,cin,database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        retry_counter_query = "select retry_counter from orders where cin = %s and id = %s"
        values = (cin,database_id)
        logging.info(retry_counter_query % values)
        cursor.execute(retry_counter_query, values)
        result = cursor.fetchone()[0]
        logging.info(f"Retry count {result}")
        return result
    except Exception as e:
        logging.info(f"Excpetion occured while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def get_run_xbrl_status(db_config,cin,database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        run_xbrl_query = "select run_xbrl from orders where cin = %s and id = %s"
        values = (cin,database_id)
        logging.info(run_xbrl_query % values)
        cursor.execute(run_xbrl_query, values)
        result = cursor.fetchone()[0]
        logging.info(f"Run xbrl flag is {result}")
        return result
    except Exception as e:
        logging.info(f"Excpetion occured while getting run xbel status {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def update_bot_comments_empty(db_config, cin, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_comments_query = f"update orders set bot_comments = '' where cin = '{cin}' and id = {database_id}"
        cursor.execute(update_comments_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def update_main_table_fields_empty(db_config, cin, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_comments_query = f"update orders set bot_comments = '',retry_counter = '',exception_type = '' where cin = '{cin}' and id = {database_id}"
        cursor.execute(update_comments_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()