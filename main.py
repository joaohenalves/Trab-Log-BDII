import psycopg2
import json


class Transaction:
    def __init__(self, nome):
        self.nome = nome
        self.comandos = []
        self.comitada = False


connection = psycopg2.connect("dbname='postgres' user='postgres' password='postgres' host='localhost' port='5432'")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS redo; CREATE TABLE redo (id INTEGER, A INTEGER, B INTEGER)")

with open('metadado.json') as file:
    data = json.load(file)
for i in range(len(data['INITIAL']['id'])):
    cursor.execute("INSERT INTO redo VALUES (%s, %s, %s)", tuple(data['INITIAL'][item][i] for item in data['INITIAL']))

connection.commit()

transactions = []
tAux = []
with open('entradaLog.txt', 'r') as file:
    for linha in file:
        for char in ['<', '>', '(', ')', '\n']:
            linha = linha.replace(char, '')
        linha = linha.replace(' ', ',').split(',')

        match linha[0]:
            case "start":
                transactions.append(Transaction(linha[1]))
            case "commit":
                for j in range(len(transactions)):
                    if transactions[j].nome == linha[1]:
                        transactions[j].comitada = True
            case "CKPT":
                del linha[0]
                tAux.extend(k for k in transactions if k.nome in linha)
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
                    for transaction in transactions:
                        if transaction.nome == linha[0]:
                            del linha[0]
                            transaction.comandos.append(linha)
                            break

for t in transactions:
    print(t.nome, t.comandos)

connection.close()
