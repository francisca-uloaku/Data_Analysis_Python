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


try_conn = create_conn(
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

reference='19718978'

sq = """
SELECT *
FROM transaction 
WHERE reference IN ('{}')
""".format(reference)

data = pull_data_from_db(query=sq, connection=pay_conn)

user = data['business']

sq2="""
SELECT CONCAT(first_name, ' ', last_name) as _full_name,
    account_number as _account_number,
    business_id as business
FROM 
virtualaccount 
WHERE business_id IN ('{}')
""".format(user[0])

user_details= pull_data_from_db(query=sq2, connection=try_conn)

sq3="""
SELECT p.phone_number, 
    p.email,
    b.id as business
FROM person p
INNER JOIN business b
ON p.id=b.owner
WHERE b.id IN ('{}');
""".format(user[0])

other_details = pull_data_from_db(query=sq3, connection=try_conn)

full_details = pd.merge(user_details, other_details, on='business', how='left', indicator=False)

full_details.to_csv(reference + '.csv', index=False)

sq4="""
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
Where business IN ('{}') 
""".format(user[0])

txn =pull_data_from_db(query=sq4, connection=txn_conn)

txn.to_csv(reference + '_Details'+ '.csv', index=False)
