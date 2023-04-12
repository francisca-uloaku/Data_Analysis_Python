import psycopg2
from decouple import config
import pandas as pd


# Transaction Service connection
conn = psycopg2.connect(
    host= config('host_tran'),
    database = config('database_tran'),
    user = config('user_tran'),
    password = config('password_tran'),
    port = config('port_tran'))
cursor = conn.cursor()


# Payment Service connection
conn2 = psycopg2.connect(
    host= config('host_pay'),
    database = config('database_pay'),
    user = config('user_pay'),
    password = config('password_pay'),
    port = config('port_pay')) 
cursor2 = conn2.cursor()

# Biz Reg Table
conn3 = psycopg2.connect(
    host= config('host_biz'),
    database = config('database_biz'),
    user = config('user_biz'),
    password = config('password_biz'),
    port = config('port_biz')) 
cursor3 = conn2.cursor()

all_transactions="""
SELECT * 
FROM transaction t
WHERE status ='00'
AND created_at::DATE <= (NOW() - interval '1 DAY')::DATE;
""" 

all_payments="""
SELECT client_reference as reference,
    client_reference,
    created_at,
    processor
FROM transaction t
WHERE status ='00'
AND created_at::DATE <= (NOW() - interval '1 DAY')::DATE;
"""

transactions= pd.read_sql(all_transactions, conn)
payments=pd.read_sql(all_payments, conn2)
result = pd.merge(transactions, payments, how="outer", on='reference',indicator=True)

transactions.to_csv('transactions.csv', index=False)
payments.to_csv('payments.csv', index=False)
result.to_csv('All_Details.csv', index=False)

