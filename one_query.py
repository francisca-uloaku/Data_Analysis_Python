from operator import truediv
import psycopg2
import csv
from decouple import config
import pandas as pd
import dateutil

def create_conn(host,db,user,password,port):
    
    conn= psycopg2.connect(
       host=config(host),
       database = config(db),
       user = config(user),
       password = config(password),
       port = config(port))
    # cursor = conn.cursor()
    return conn


def pull_data_from_db(query, connection):
    data = pd.read_sql(query, connection)
    return data

kippa_conn = create_conn(
    host='host',
    db='database',
    user='user',
    password='password',
    port='port')

txn_conn = create_conn(
    host='host_tran',
    db='database_tran',
    user='user_tran',
    password='password_tran',
    port='port_tran')

pay_conn = create_conn(
    host='host_pay',
    db='database_pay',
    user='user_pay',
    password='password_pay',
    port='port_pay')

wallet_conn = create_conn(
    host='host_wal',
    db='database_wal',
    user='user_wal',
    password='password_wal',
    port='port_wal')

sq="""
WITH TEMP AS(
SELECT created_at::DATE,
    t.created_by as id
FROM transaction t
UNION
SELECT i.created_at::DATE,
    --business as id,
    b.owner as id
FROM invoice i
RIGHT JOIN business b on b.id=i.business
UNION
SELECT p.created_at::DATE,  
    --created_by as id,
    b.owner as id
FROM product p
RIGHT JOIN business b on b.id=p.business
UNION
SELECT d.created_at::DATE,
    --business as id,
    b.owner as id
FROM debt d
RIGHT JOIN business b on d.business=b.id
UNION
SElECT c.created_at::DATE,
    --business as id,
    b.owner as id
FROM credit c
RIGHT JOIN business b on c.business=b.id),

TEMP2 AS (
SELECT * 
FROM TEMP
WHERE created_at::DATE BETWEEN '2022-04-01' AND (NOW() - interval '1 DAY')::DATE),


TEMP3 AS (
SELECT ID, COUNT(*)
FROM TEMP2
GROUP BY 1
ORDER BY 2 DESC)

SELECT t.id,
    t.count,
    p.first_name,
    p.last_name,
    p.phone_number
FROM person p
JOIN TEMP3 t ON p.id=t.id
ORDER BY 2 DESC;
"""

data= pull_data_from_db(query=sq, connection=kippa_conn)

data.to_csv('Generic.csv', index=False)



