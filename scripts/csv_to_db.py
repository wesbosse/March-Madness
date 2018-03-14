import os
import csv
import pymysql
host = input("Host: ")
user = input("Username: ")
password = input("Password: ")
db = input("Database: ")
connection = pymysql.connect(
    host=host,
    user=user,
    password=password,
    db=db
)


directory_str = "../data/"
for sub_dirs, dirs, files in os.walk(directory_str):
    for current_file in files:
        if current_file.endswith("csv"):
            file_path = directory_str + current_file
            table_name = current_file[:-4]

            with open(file_path, 'r') as raw_file:
                open_csv = csv.reader(raw_file)
                columns = next(open_csv)

                columns_and_types = []
                for i, column in enumerate(columns):
                    columns_and_types.append()

                print(columns_and_types)
                cursor = connection.cursor()
                for data in open_csv:
                    query = 'insert into {}({}) values ({})'.format(table_name, ','.join(columns), ','.join(data))
                    # cursor.execute(query)
                    print(query)
                connection.commit()
                print(query)
        else:
            continue


try:
    cursorObject = connectionObject.cursor()
    sqlQuery = "CREATE TABLE Employee(id int, LastName varchar(32), FirstName varchar(32), DepartmentCode int)"
    cursorObject.execute(sqlQuery)
    sqlQuery = "show tables"
    cursorObject.execute(sqlQuery)
    rows = cursorObject.fetchall()
    for row in rows:
        print(row)
except Exception as e:
    print("Exeception occured:{}".format(e))
finally:
    connectionObject.close()