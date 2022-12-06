import psycopg2
import json

class Transaction:
    def __init__(self, nome):
        self.nome, self.comandos, self.comitada = nome, [], False

connection = psycopg2.connect("dbname='postgres' user='postgres' password='postgres' host='localhost' port='5432'")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS redo; CREATE TABLE redo (id INTEGER PRIMARY KEY, A INTEGER, B INTEGER)")

with open('metadado.json') as file:
    data = json.load(file)
for i in range(len(data['INITIAL']['id'])):
    cursor.execute("INSERT INTO redo VALUES (%s, %s, %s)", tuple(data['INITIAL'][item][i] for item in data['INITIAL']))
connection.commit()

transactions, tAux = [], []
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
                        break
            case "CKPT":
                tAux.extend(k for k in transactions if k.nome in linha[1:])
                transactions = tAux
            case "abort":
                for l in range(len(transactions)):
                    if transactions[l].nome == linha[1]:
                        del transactions[l]
                        break
            case _:
                if len(linha) == 5:
                    [t.comandos.append(linha[1:]) for t in transactions if t.nome == linha[0]]

for t in transactions:
    if t.comitada:
        for i in range(len(t.comandos)):
            cursor.execute(f"SELECT {t.comandos[i][1]} FROM redo WHERE id = {t.comandos[i][0]}")
            v = cursor.fetchone()

            if v[0] != (valor := t.comandos[i][3]):
                cursor.execute(f"UPDATE redo SET {t.comandos[i][1]} = {valor} WHERE id = {t.comandos[i][0]}")
                connection.commit()
                print(f"A transação {t.nome} alterou a coluna {t.comandos[i][1]} do id {t.comandos[i][0]} para o valor: {t.comandos[i][3]}")
    print(f"Transação {t.nome}{'' if t.comitada else ' não'} realizou REDO")

cursor.execute("SELECT * FROM redo ORDER BY id")
print(cursor.fetchall())
connection.close()
