# 🗄️ Transaction Recovery — Database II

Academic project developed for the **Database II** course, focusing on simulating database recovery after a failure using transaction logs.

## Contributors

This project was collaboratively developed by:

- **[João Henrique Alves](https://github.com/joaohenalves)**
- **[Bernardo Facchi](https://github.com/Cataratas)**

## 📌 About the Project

The program implements a simplified **REDO-based recovery mechanism**, responsible for reapplying changes made by committed transactions.

It processes a transaction log, identifies checkpoints, and performs the necessary operations on a PostgreSQL database.

## 🛠️ Technologies Used

- **Python** — Recovery logic implementation.
- **PostgreSQL** — Relational database system.
- **Psycopg2** — Communication between Python and PostgreSQL.
- **JSON** — Initial database state storage.

## 📁 Repository Structure

- **`main.py`** — REDO recovery mechanism implementation.
- **`entradaLog.txt`** — Transaction log containing recorded operations.
- **`metadado.json`** — Initial database state.

## 📚 Topics Covered

- Database Transactions
- Transaction Logging
- Crash Recovery (REDO)
- Checkpoints
- COMMIT and ABORT Operations

## 🎓 Academic Context

Developed as part of the **Computer Science** undergraduate program, for the **Database II** course.
