import psycopg2
import json


connection = psycopg2.connect("dbname='postgres' user='postgres' password='postgres' host='localhost' port='5432'")
cursor = connection.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS redo (id INTEGER, A INTEGER, B INTEGER)")

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


log = []
arq = open('entradaLog.txt','r')
texto = arq.readlines()
chars = ['<','>','(',')','\n']
for linha in texto:
    for char in chars:
        linha = linha.replace(char, '')
    linha = linha.replace(' ', ',')
    linha = linha.split(',')
    log.append(linha)
log.pop(len(log)-1)
for i in log:
    print(i)
arq.close()
connection.close()
