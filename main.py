import psycopg2
import json


connection = psycopg2.connect("dbname='postgres' user='postgres' password='postgres' host='localhost' port='5432'")
cursor = connection.cursor()
cursor.execute("DROP TABLE redo; CREATE TABLE redo (id INTEGER, A INTEGER, B INTEGER)")

with open('metadado.json') as f:
    data = json.load(f)

for i in range(len(data['INITIAL']['id'])):
    values = [data['INITIAL'][item][i] for item in data['INITIAL']]
    query = "INSERT INTO redo VALUES (%s, %s, %s)"
    cursor.execute(query, tuple(values))
connection.commit()


log = []
arq = open('entradaLog','r')
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

transactions, commandQuery = [], []
for command in log:
    match command[0]:
        case "start":
            transactions.append(command[1])
        case "commit":
            pass
        case "CKPT":
            pass
        case "crash":
            pass
        case _:
            if (len(command)) == 5:
                pass

connection.close()
