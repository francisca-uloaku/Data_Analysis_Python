import psycopg2
from decouple import config
import pandas as pd
from datetime import date, timedelta
import json

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

merchants= pd.read_csv("https://....../d/e/" +"...........true&"+".....put=csv")

merchants['Date'].fillna(method='ffill', inplace=True)
merchants['Date'] = pd.to_datetime(merchants['Date'])

numbers = tuple(merchants['Full Number'].unique())
merchants['ACCOUNT NUMBER'].fillna(method='ffill', inplace=True)
acc_num = tuple(merchants['ACCOUNT NUMBER'].unique())


sq="""
SELECT v.created_at::DATE AS "Date",
    CONCAT(v.first_name, ' ', v.last_name) AS "Merchant Name",
    v.account_name AS "Business Name",
    p.phone_number,
    v.account_number,
    b.id as business
FROM person p
JOIN business b ON p.id=b.owner
JOIN virtualaccount v ON b.id=v.business_id
WHERE p.phone_number IN {}
UNION
SELECT v.created_at::DATE AS "Date",
    CONCAT(v.first_name, ' ', v.last_name) AS "Merchant Name",
    v.account_name AS "Business Name",
    p.phone_number,
    v.account_number,
    b.id as business
FROM person p
JOIN business b ON p.id=b.owner
JOIN virtualaccount v ON b.id=v.business_id
AND v.account_number::BIGINT IN {}
""".format(numbers, acc_num)


details_found=pd.read_sql(sq, conn)

print(len(details_found))

details_found.to_csv('file.csv', index=False)
