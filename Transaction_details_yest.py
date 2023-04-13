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

# Transaction Service connection
conn2 = psycopg2.connect(
    host= config('host_tran'),
    database = config('database_tran'),
    user = config('User_tran'),
    password = config('password_tran'),
    port = config('port_tran')) 
cursor2 = conn2.cursor()


# Get business id from transaction service
sq="""
SELECT business, 
    count(*) 
FROM transaction 
GROUP BY 1 
ORDER BY 2 desc;"""
df = pd.read_sql(sq,conn2)
df.to_excel("Alltime_details.xlsx",
             sheet_name='All_transacting_User') 

data=tuple(df['business'])
sq="""
SELECT b.id, 
    p.first_name, 
    p.last_name, 
    p.phone_number 
FROM PERSON p 
JOIN business b ON p.id=b.owner 
WHERE b.id in {};""".format(data)
df = pd.read_sql(sq,conn)


df.to_excel("Details.xlsx",
             sheet_name='Details_User')  

# Get business id from transaction service yesterday
sq="""
SELECT business, 
    count(*) 
FROM transaction 
WHERE created_at::DATE =(NOW() - interval '1 DAY')::DATE
GROUP BY 1 
ORDER BY 2 desc;"""
df = pd.read_sql(sq,conn2)
df.to_excel("Alltime_details_yest.xlsx",
             sheet_name='All_transacting_User') 


# Business and type
sq= """
SELECT business, 
    type
FROM transaction;"""
df = pd.read_sql(sq,conn2)
df.to_excel("Alltime_details_type.xlsx",
             sheet_name='Business_type') 

# Business and type yesterday
sq= """
SELECT business, 
    type
FROM transaction
WHERE created_at::DATE =(NOW() - interval '1 DAY')::DATE;
"""
df = pd.read_sql(sq,conn2)
df.to_excel("Alltime_details_type_yest.xlsx",
             sheet_name='Business_type') 


data=tuple(df['business'])
sq="""
SELECT b.id, 
    p.first_name, 
    p.last_name, 
    p.phone_number 
FROM PERSON p 
JOIN business b ON p.id=b.owner 
WHERE b.id in {};""".format(data)
df = pd.read_sql(sq,conn)


df.to_excel("Details.xlsx",
             sheet_name='Details_User')  
print('DataFrame is written successfully to Excel File.')

df.to_csv('transaction_details.csv', index=False)
print('DataFrame is written successfully to CSV File.')
