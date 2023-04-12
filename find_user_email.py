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


email= 'udoka000@gmail.com'

sq="""
SELECT b.id,
    b.owner,
    p.id,
    v.account_number,
    v.account_name,	
    v.first_name,	
    v.last_name,
    v.business_id as business
FROM person p
JOIN business b ON p.id=b.owner
JOIN virtualaccount v ON b.id=v.business_id
WHERE p.email ILIKE '{}'
""".format(email)

userdetails = pull_data_from_db(query=sq, connection=kippa_conn)

business= userdetails['business']
business = str(business[0])

sq2="""
SELECT business,
    current_balance
FROM wallet 
WHERE business = '{}'
""".format(business)

balance = pull_data_from_db(query=sq2, connection=wallet_conn)

full_data = pd.merge(userdetails, balance, on='business', how='inner', indicator=True)

full_data=full_data[['account_number',
                    'account_name',
                    'first_name',
                    'last_name',
                    'current_balance']]

full_data.to_csv(str(email) + '.csv', index=False)

sq3="""
SELECT type,
    amount, 
    created_at, 
    reference, 
    status,
    gross, 
    business,
    "fundingId",
    "dvaId"
FROM transaction
Where business = '{}';
""".format(business) 

txn_record = pull_data_from_db(query=sq3, connection=txn_conn)

txn_record.to_csv(str(email) +'_record' + '.csv', index=False)

