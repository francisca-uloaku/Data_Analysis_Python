import psycopg2
from decouple import config
import pandas as pd


# Mobile App Database
conn = psycopg2.connect(
    host= config('host'),
    database = config('database'),
    user = config('user'),
    password = config('password'),
    port = config('port')) 
cursor = conn.cursor()

# Transaction Service connection
conn2 = psycopg2.connect(
    host= config('host_tran'),
    database = config('database_tran'),
    user = config('user_tran'),
    password = config('password_tran'),
    port = config('port_tran')) 
cursor2 = conn2.cursor()

payment_sq="""
SELECT DISTINCT business 
FROM transaction t
WHERE status ='00'
AND is_reversed IS FALSE
AND is_reversed_action_response IS NULL
AND t.created_at::DATE BETWEEN date_trunc('MONTH',now())::DATE - interval '1 MONTH' AND
date_trunc('MONTH', NOW()) - interval '1 day'
;
""" 
payment_business= pd.read_sql(payment_sq, conn2)

business= tuple(payment_business['business'])

# Users that performed both book-keeping and payment transactions
transaction_sq="""
WITH TEMP AS(
SELECT  
    t.created_by as id, 
    t.created_at
FROM transaction t
WHERE t.created_at::DATE BETWEEN date_trunc('MONTH',now())::DATE - interval '1 MONTH' AND
date_trunc('MONTH', NOW()) - interval '1 day'
),

TEMP2 AS(
SELECT id, COUNT(id)
FROM TEMP
GROUP BY 1
HAVING COUNT(id)>=2)


SELECT t.id, t.count,
    p.phone_number--, max(t.created_at::DATE), min(t.created_at::DATE)
FROM TEMP2 t 
JOIN person p ON t.id=p.id
JOIN business b ON p.id=b.owner
WHERE b.id in {}
GROUP BY 1, 2, 3
ORDER BY 2 DESC
;
""".format(business)

payment_transactions= pd.read_sql(transaction_sq, conn)

payment_transactions.to_csv('Book-keeping_Payment.csv', index=False)

# Users that performed only Book-keeping
bk_transaction_sq="""
WITH TEMP AS(
SELECT  
    t.created_by as id, 
    t.created_at
FROM transaction t
WHERE t.created_at::DATE BETWEEN date_trunc('MONTH',now())::DATE - interval '1 MONTH' AND
date_trunc('MONTH', NOW()) - interval '1 day'),

TEMP2 AS(
SELECT id, COUNT(id)
FROM TEMP
GROUP BY 1
HAVING COUNT(id)>=2)


SELECT t.id, t.count,
    p.phone_number--, max(t.created_at::DATE), min(t.created_at::DATE)
FROM TEMP2 t 
JOIN person p ON t.id=p.id
JOIN business b ON p.id=b.owner
WHERE b.id NOT in {}
GROUP BY 1, 2, 3
ORDER BY 2 DESC
LIMIT 4000
;
""".format(business)

book_transactions= pd.read_sql(bk_transaction_sq, conn)

book_transactions.to_csv('Book-keeping.csv', index=False)