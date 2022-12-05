import psycopg2
import json


class Transaction:
    nome = ''
    comandos = []
    comitada = False
    abortada = False


connection = psycopg2.connect("dbname='postgres' user='postgres' password='joaohenalves' host='localhost' port='5432'")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS redo; CREATE TABLE redo (id INTEGER, A INTEGER, B INTEGER)")

with open('metadado.json') as f:
    data = json.load(f)

for i in range(len(data['INITIAL']['id'])):
    values = [data['INITIAL'][item][i] for item in data['INITIAL']]
    query = "INSERT INTO redo VALUES (%s, %s, %s)"
    cursor.execute(query, tuple(values))
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
            print("entrou start")
            t = Transaction()
            t.nome = linha[1]
            transactions.append(t)
        case "commit":
            print("entrou commit")
            for j in range(len(transactions)):
                if transactions[j].nome == linha[1]:      
                    transactions[j].comitada = True
        case "CKPT":
            print("entrou ckpt")
            del linha[0]
            for k in transactions:
                if k.nome in linha:
                    tAux.append(k)
            transactions = tAux
        case "abort":
            print("entrou abort")
            for l in range(len(transactions)):
                if transactions[l].nome == linha[1]:
                    del transactions[l]
                    break
        case "crash":
            print("entrou crash")
            pass
        case _:
            print("entrou default")
            if (len(linha)) == 5:
                for m in range(len(transactions)):
                    print(1)
                    if transactions[m].nome == linha[0]:
                        del linha[0]
                        transactions[m].comandos.append(linha)
                        print(linha)
                        break


for l in transactions:
    print(l.nome, l.comitada)
    

print(len(transactions))
for z in range(len(transactions)):
    print(transactions[z].comandos)
    



# commandQuery = []
# for command in log:
#     match command[0]:
#         case "start":
#             transactions.append(command[1])
#         case "commit":
#             pass
#         case "CKPT":
#             pass
#         case "crash":
#             pass
#         case _:
#             if (len(command)) == 5:
#                 pass

connection.close()
