import mysql.connector



db_connection = mysql.connector.connect(
    host=host,
    user=username,
    password=password,
    database=database
)
db_connection.autocommit = True

db_cursor = db_connection.cursor()

