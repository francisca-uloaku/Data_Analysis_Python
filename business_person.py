import psycopg2
from decouple import config
import pandas as pd


def create_conn(host, db, user, password, port):

    conn = psycopg2.connect(
        host=config(host),
        database=config(db),
        user=config(user),
        password=config(password),
        port=config(port),
    )
    # cursor = conn.cursor()
    return conn


def pull_data_from_db(query, connection):
    data = pd.read_sql(query, connection)
    return data


try_conn = create_conn(
    host="host", db="database", user="user", password="password", port="port"
)

txn_conn = create_conn(
    host="host_tran",
    db="database_tran",
    user="user_tran",
    password="password_tran",
    port="port_tran",
)

pay_conn = create_conn(
    host="host_pay",
    db="database_pay",
    user="user_pay",
    password="password_pay",
    port="port_pay",
)

wallet_conn = create_conn(
    host="host_wal",
    db="database_wal",
    user="user_wal",
    password="password_wal",
    port="port_wal",
)

sq = """
SELECT b.id as business,
    p.id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone_number
FROM business b
JOIN person p ON b.owner=p.id
WHERE b.owner IN (
    SELECT b.owner
    FROM business b
    GROUP BY 1
    HAVING COUNT(b.id) > 3
    ORDER BY 1 DESC)
ORDER BY 2 DESC,1
"""

merchants_data = pull_data_from_db(query=sq, connection=try_conn)

business_ids = tuple(merchants_data["business"])

sq2 = """
SELECT business_id as business,
    account_number,
    account_name,
    CONCAT(first_name, ' ', last_name) as Merchant_name
FROM virtualaccount
WHERE business_id IN {}
""".format(
    business_ids
)

account_details = pull_data_from_db(query=sq2, connection=try_conn)

merchants_details = pd.merge(
    merchants_data, account_details, on="business", how="outer"
)

sq3 = """
SELECT business,
    current_balance,
    --lien,
    is_blacklisted AS "Blacklisted Tag"
FROM Wallet
WHERE business IN {}
""".format(
    business_ids
)

balance = pull_data_from_db(query=sq3, connection=wallet_conn)

all_details = pd.merge(merchants_details, balance, on="business", how="outer")

all_details.to_csv("All_details.csv", index=False)

sq4 = """
WITH TEMP AS (
SELECT p.id,
    COUNT(b.id) as No_of_business
FROM person p
JOIN business b
on p.id=b.owner
GROUP BY 1),

TEMP2 AS (
SELECT *,
    (CASE WHEN No_of_business > 5 THEN 'Greater Than 5'
        WHEN No_of_business BETWEEN 4 AND 5 THEN 'Between 4 and 5'
        WHEN No_of_business BETWEEN 2 AND 3 THEN 'Between 2 and 3'
        WHEN No_of_business = 1 THEN 'Just 1'
        ELSE 'Uncatigorized' END) AS Business_count_category
FROM TEMP),

TEMP3 AS (
SELECT DISTINCT id, 
    id as two 
FROM TEMP2 WHERE No_of_business>3)

SELECT t.id as business,
    p.id,
    p.first_name,
    p.last_name,
    p.email,
    p.phone_number
FROM person p 
JOIN TEMP3 t ON t.id=p.id
"""
business_with_3 = pull_data_from_db(query=sq4, connection=try_conn)

sq5 = """
WITH TEMP AS (
SELECT p.id,
    COUNT(b.id) as No_of_business
FROM person p
JOIN business b
on p.id=b.owner
GROUP BY 1),

TEMP2 AS (
SELECT *,
    (CASE WHEN No_of_business > 5 THEN 'Greater Than 5'
        WHEN No_of_business BETWEEN 4 AND 5 THEN 'Between 4 and 5'
        WHEN No_of_business BETWEEN 2 AND 3 THEN 'Between 2 and 3'
        WHEN No_of_business = 1 THEN 'Just 1'
        ELSE 'Uncatigorized' END) AS Business_count_category
FROM TEMP)

SELECT DISTINCT t.created_by as business,
    t.created_by
FROM transaction t
WHERE t.created_by IN
(SELECT id FROM TEMP2 WHERE No_of_business>3)
AND t.created_at::DATE BETWEEN '2022-02-01' AND '2022-05-31'
"""

active_merchants = pull_data_from_db(query=sq5, connection=try_conn)

active_status = pd.merge(business_with_3, active_merchants, on="business", how="outer")

active_status.to_csv("Active Status.csv", index=False)
