import mysql.connector

host = "162.241.123.123"
username = "classle3"
password = "0p=HJ^q~$pT-"
database = "classle3_mns_credit"

db_connection = mysql.connector.connect(
    host=host,
    user=username,
    password=password,
    database=database
)
db_connection.autocommit = True

db_cursor = db_connection.cursor()

