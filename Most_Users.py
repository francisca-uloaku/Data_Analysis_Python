from operator import truediv
from tkinter.tix import tixCommand
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
SELECT business,
    MIN(created_at::DATE),
    type,
    count(*),
    SUM(Amount)
FROM  transaction 
WHERE status='00'
AND is_reversed IS FALSE
AND is_reversed_action_response IS NULL
GROUP BY 1,3
ORDER BY 2 DESC;
"""

all_time_payment_use = pull_data_from_db(query=sq, connection=txn_conn)

business= tuple(all_time_payment_use['business'])

sq2="""
SELECT DISTINCT p.id,
    p.first_name,
    p.last_name,
    p.email,
    bc.name AS industry,
    p.created_at::DATE,
    b.name as business_name,
    b.id as business
FROM person p
LEFT JOIN business b ON p.id=b.owner
LEFT JOIN "businessCategory" bc ON b.category=bc.id
WHERE b.id IN {}
""".format(business)

business_industry = pull_data_from_db(query=sq2, connection=kippa_conn)

all_details=pd.merge(business_industry, all_time_payment_use, on='business', how='outer', indicator=True)

all_details.to_csv('Industry_rank.csv', index=False)

