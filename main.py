import psycopg2
import json

class Transaction:
    def __init__(self, nome):
        self.nome = nome
        self.comandos = []
        self.comitada = False

connection = psycopg2.connect("dbname='postgres' user='postgres' password='joaohenalves' host='localhost' port='5432'")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS redo; CREATE TABLE redo (id INTEGER, A INTEGER, B INTEGER)")

with open('metadado.json') as f:
    data = json.load(f)

for i in range(len(data['INITIAL']['id'])):
    values = [data['INITIAL'][item][i] for item in data['INITIAL']]
    cursor.execute("INSERT INTO redo VALUES (%s, %s, %s)", tuple(values))
connection.commit()

transactions = []
tAux = []
arq = open('entradaLog.txt','r')
texto = arq.readlines()
arq.close()
chars = ['<','>','(',')','\n']
for linha in texto:
    for char in chars:
        linha = linha.replace(char, '')
    linha = linha.replace(' ', ',')
    linha = linha.split(',')

    match linha[0]:
        case "start":
            transactions.append(Transaction(linha[1]))
        case "commit":
            for j in range(len(transactions)):
                if transactions[j].nome == linha[1]:      
                    transactions[j].comitada = True
        case "CKPT":
            del linha[0]
            for k in transactions:
                if k.nome in linha:
                    tAux.append(k)
            transactions = tAux
        case "abort":
            for l in range(len(transactions)):
                if transactions[l].nome == linha[1]:
                    del transactions[l]
                    break
        case "crash":
            pass
        case _:
            if (len(linha)) == 5:
                for m in range(len(transactions)):
                    if transactions[m].nome == linha[0]:
                        del linha[0]
                        transactions[m].comandos.append(linha)
                        break

connection.close()
