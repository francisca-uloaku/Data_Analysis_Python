from numpy import full
import psycopg2
from decouple import config
import pandas as pd



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

sq_1="""
SELECT business_id as business, 
    account_number, 
    account_name, 
    CONCAT(first_name, ' ', last_name) as Full_Name
FROM virtualaccount
WHERE account_number::BIGINT IN (
........,
........,
........
)
UNION 
SELECT business_id as business, 
    account_number, 
    account_name, 
    CONCAT(first_name, ' ', last_name) as Full_Name
FROM virtualaccount
WHERE account_name ILIKE 'dis%';
"""

staff= pd.read_sql(sq_1, conn)

remove = tuple(staff['business'])

sq="""
SELECT DISTINCT Business,
    COUNT(*),
    SUM(Amount)
FROM transaction 
WHERE status='00'
AND is_reversed IS FALSE
AND is_reversed_action_response IS NULL
AND business NOT IN {}
GROUP BY 1
ORDER BY 2 DESC
LIMIT 60; 
""".format(remove)

top_pay_users= pd.read_sql(sq, conn2)

top_pay_business = tuple(top_pay_users['business'])

sq2="""
SELECT 
    p.first_name,
    p.last_name,
    p.phone_number,
    b.name as business_name,
    b.id as business
FROM PERSON p 
LEFT JOIN business b ON p.id=b.owner
WHERE b.id IN {}
""".format(top_pay_business)

User_details = pd.read_sql(sq2, conn)

full_details=pd.merge(User_details, top_pay_users, how='outer', on='business', indicator=True)

full_details=full_details.sort_values('count', ascending=False)

full_details.to_csv("Payment_top_users.csv", index=False)
