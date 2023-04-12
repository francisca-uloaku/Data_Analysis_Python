import psycopg2
from decouple import config
import pandas as pd


# Mobile App Database
conn = psycopg2.connect(
    host= config('Host_name'),
    database = config('Database'),
    user = config('User'),
    password = config('Password'),
    port = config('Port')) 
cursor = conn.cursor()


# data = pd.read_csv('Fairmoney  - fairmoney.csv')
# id=tuple(data['id'])
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
WHERE created_at::DATE BETWEEN '2022-02-01' AND (NOW() - interval '1 DAY')::DATE),


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
ORDER BY 2 DESC
LIMIT 20000
""" #.format(id)


def read_db(conn, sq, filename):
    df = pd.read_sql(sq,conn)
    df.to_csv(filename +'.csv', index=False)
    return "Done", len(df)

print(read_db(conn, sq, filename='UsersDetails'))





# df.to_excel("BookKeeping_Details_March.xlsx",
#              sheet_name='All_transacting_User') 
