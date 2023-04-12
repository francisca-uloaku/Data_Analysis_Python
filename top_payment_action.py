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

sq1="""
SELECT DISTINCT Business,
    COUNT(*) AS Payment_count,
    SUM(Amount) AS Payment_volume
FROM transaction 
WHERE status='00'
AND is_reversed IS FALSE
AND is_reversed_action_response IS NULL
GROUP BY 1
ORDER BY 3 DESC
LIMIT 1000;
"""
payment_record = pull_data_from_db(query=sq1, connection=txn_conn)

payment_users=tuple(payment_record['business'])

sq2 ="""
SELECT DISTINCT p.id, 
    p.phone_number,
    p.first_name,
    p.last_name,
    p.email,
    b.name AS business_name,
    bc.name AS industry,
    b.state,
    b.city,
    b.id as business
FROM person p 
JOIN business b ON p.id=b.owner
JOIN virtualaccount v ON b.id=v.business_id
JOIN "businessCategory" bc ON b.category=bc.id
WHERE b.state='Lagos' 
AND b.id IN {}; 
""".format(payment_users)

payment_user_details=pull_data_from_db(query=sq2, connection=kippa_conn)

payment_details=pd.merge(payment_user_details, payment_record, on='business', how='left', indicator=True)

payment_details=payment_details.sort_values('payment_volume', ascending=False)

payment_details.to_csv("Top_Payment_User_Details.csv", index=False)

sq3="""
WITH TEMP AS(
SELECT created_at::DATE,
    t.created_by as id
FROM transaction t
UNION ALL
SELECT i.created_at::DATE,
    --business as id,
    b.owner as id
FROM invoice i
LEFT JOIN business b on b.id=i.business
UNION ALL
SELECT p.created_at::DATE,  
    --created_by as id,
    b.owner as id
FROM product p
LEFT JOIN business b on b.id=p.business
UNION ALL
SELECT d.created_at::DATE,
    --business as id,
    b.owner as id
FROM debt d
LEFT JOIN business b on d.business=b.id
UNION ALL
SElECT c.created_at::DATE,
    --business as id,
    b.owner as id
FROM credit c
LEFT JOIN business b on c.business=b.id
UNION ALL
SELECT v.created_at::DATE,
    b.owner as id
FROM virtualaccount v
LEFT JOIN business b on b.id=v.business_id)

SELECT id,
    COUNT(*) as Action_count
FROM TEMP
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1000
"""

action= pull_data_from_db(query=sq3, connection=kippa_conn)
action=action.dropna()
users=tuple(action['id'])

sq4="""
SELECT DISTINCT p.id, 
    p.phone_number,
    p.first_name,
    p.last_name,
    p.email,
    b.name AS business_name,
    bc.name AS industry,
    b.state,
    b.city,
    b.id as business
FROM person p 
JOIN business b ON p.id=b.owner
JOIN virtualaccount v ON b.id=v.business_id
JOIN "businessCategory" bc ON b.category=bc.id
WHERE b.state='Lagos' 
AND p.id IN {}; 
""".format(users)

action_details = pull_data_from_db(query=sq4, connection=kippa_conn)

action_users = pd.merge(action_details, action, on='id', how='left', indicator=True)

action_users=action_users.sort_values('action_count', ascending=False)

action_users.to_csv("Top_action_User_Details.csv", index=False)
