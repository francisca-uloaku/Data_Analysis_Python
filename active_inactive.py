from calendar import c
from operator import truediv
import psycopg2
import csv
from decouple import config
import pandas as pd
import dateutil


def create_conn(host, db, user, password, port):

    conn = psycopg2.connect(
        host=config(host),
        database=config(db),
        user=config(user),
        password=config(password),
        port=config(port),
        sslmode="disable",
    )
    # cursor = conn.cursor()
    return conn


def pull_data_from_db(query, connection):
    data = pd.read_sql(query, connection)
    return data


kippa_conn = create_conn(
    host="host", db="database", user="user", password="password", port="port"
)

txn_conn = create_conn(
    host="host_tran",
    db="database_tran",
    user="user_tran",
    password="password_tran",
    port="port_tran",
)


sq = """
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
RIGHT JOIN business b on c.business=b.id
UNION
SELECT v.created_at::DATE,
    b.owner as id
FROM virtualaccount v
RIGHT JOIN business b on b.id=v.business_id)

SELECT DISTINCT ID 
FROM TEMP t
WHERE t.created_at::DATE>='2021-06-01';
"""

All_active_users = pull_data_from_db(query=sq, connection=kippa_conn)

All_active_users = All_active_users.dropna()

id = tuple(All_active_users["id"])

sq2 = """
SELECT p.phone_number
FROM person p
WHERE p.id IN {}
""".format(
    id
)

# active_users_phone= pull_data_from_db(query=sq2, connection=kippa_conn)

active_users_phone = pd.read_sql(sq2, kippa_conn)

active_users_phone.to_csv("Active_users.csv", index=False)

sq3 = """
SELECT p.phone_number
FROM person p
EXCEPT
SELECT p.phone_number
FROM person p
WHERE p.id IN {}
""".format(
    id
)

# inactive_users_phone= pull_data_from_db(query=sq3, connection=kippa_conn)

inactive_users_phone = pd.read_sql(sq3, kippa_conn)

inactive_users_phone.to_csv("Inactive_users.csv", index=False)
