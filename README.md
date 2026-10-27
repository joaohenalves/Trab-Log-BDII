# 🗄️ Recuperação de Transações — Banco de Dados II

Projeto acadêmico desenvolvido para a disciplina de **Banco de Dados II**, com o objetivo de simular a recuperação de um banco de dados após uma falha, utilizando registros de log.

## 📌 Sobre o projeto

O programa implementa um mecanismo simplificado de recuperação baseado na operação **REDO**, responsável por reaplicar alterações de transações confirmadas (*committed*).

A aplicação interpreta um arquivo de log contendo operações de transações, identifica checkpoints e executa as alterações necessárias em um banco de dados PostgreSQL.

## 🛠️ Tecnologias utilizadas

- **Python** — Implementação da lógica de recuperação.
- **PostgreSQL** — Banco de dados relacional.
- **Psycopg2** — Comunicação entre Python e PostgreSQL.
- **JSON** — Armazenamento dos dados iniciais.

## 📁 Estrutura do repositório

- **`main.py`** — Implementação do mecanismo de recuperação REDO.
- **`entradaLog.txt`** — Registro das operações e transações.
- **`metadado.json`** — Estado inicial do banco de dados.

## 📚 Conceitos abordados

- Transações em bancos de dados
- Logs de transações
- Recuperação de falhas (REDO)
- Checkpoints
- Operações COMMIT e ABORT

## 🎓 Contexto acadêmico

Trabalho desenvolvido durante a graduação em **Ciência da Computação**, na disciplina de **Banco de Dados II**.