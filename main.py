import psycopg2
import json


connection = psycopg2.connect("dbname='postgres' user='postgres' password='postgres' host='localhost' port='5432'")
cursor = connection.cursor()


def create_table():
    cursor.execute("CREATE TABLE IF NOT EXISTS redo (id INTEGER, A INTEGER, B INTEGER)")
    connection.commit()


create_table()

with open('metadado.json') as f:
    data = json.load(f)

print(data['INITIAL'])

for i in range(len(data['INITIAL']['id'])):
    values = []
    for item in data['INITIAL']:
        values.append(data['INITIAL'][item][i])

    insert_query = "INSERT INTO redo VALUES (%s, %s, %s) "
    cursor.execute(insert_query, tuple(values))
    print(values)

connection.commit()
connection.close()
