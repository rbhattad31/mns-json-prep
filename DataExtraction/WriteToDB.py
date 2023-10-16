import mysql.connector

host = ""
username = ""
password = ""
database = ""

db_connection = mysql.connector.connect(
    host=host,
    user=username,
    password=password,
    database=database
)

db_connection.autocommit = True
db_cursor = db_connection.cursor()

