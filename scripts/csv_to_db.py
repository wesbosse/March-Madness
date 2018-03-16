import os
import csv
import codecs
import pymysql

host = '198.71.225.53'
user = input("Username: ")
password = input("Password: ")
db = 'ncaa'
table_name = "Scraped"

connection = pymysql.connect(
    host=host,
    user=user,
    password=password,
    db=db
)
cursor = connection.cursor()

directory_str = "../data/"
for sub_dirs, dirs, files in os.walk(directory_str):
    for current_file in files:
        if current_file.endswith("csv"):
            file_path = directory_str + current_file
            table_name = current_file[:-4]

            if table_name == 'NCAATourneyCompactResults':
                header = ['year int', 'day int', 'winning_id int', 'winning_score int', 'losing_id int', 'losing_score int', 'winner_location varchar(32)', 'overtime int']
            if table_name == 'NCAATourneySeeds':
                header = ['year int', 'seed varchar(6)', 'team_id int']
            if table_name == 'NCAATourneySlots':
                header = ['year int', 'slot varchar(6)', 'strong_seed varchar(6)', 'weak_seed varchar(6)']
            if table_name == 'Teams':
                header = ['team_id int', 'table_name varchar(32)', 'FirstD1Season int', 'LastD1Season int']
            if table_name == 'TeamSpellings':
                header = ['TeamNameSpelling varchar(32)', 'TeamID int']

            cursor.execute("SHOW TABLES LIKE '{}'".format(table_name))
            result = cursor.fetchone()
            print(result)
            if result == None:
                cursor.execute("CREATE TABLE {} ({})".format(table_name, ','.join(header)))
                connection.commit()
            else:
                cursor.execute("DROP TABLE {}".format(table_name))
                cursor.execute("CREATE TABLE {} ({})".format(table_name, ','.join(header)))
                connection.commit()

            with codecs.open(file_path, 'r',  encoding='utf-8', errors='ignore') as raw_file:
                open_csv = csv.reader(raw_file)
                columns = next(open_csv)

                for data in open_csv:
                    formatted_row=[]
                    for item in data:
                        if type(item) == str:
                            formatted_row.append('"' + item + '"')
                        else:
                            formatted_row.append(item)
                    query = 'insert into {} values ({})'.format(table_name, ','.join(formatted_row))
                    cursor.execute(query)
                    print(query)
                # connection.commit()
                # print(query)
        else:
            continue