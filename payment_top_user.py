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
8227436851,
8223687421,
8225752987,
8223365102,
8227316198,
7697228504,
8221502015,
8228135223,
8223066967,
8223593211,
8229716736,
7681660657,
8225855713,
8227779822,
8225259685,
8228817638,
8222314943,
7681357021,
8229507135,
7681014504,
8220760018,
7681844042,
8222076422,
7682449286,
7696798972,
8227251717,
7681449227,
7682050903,
8225520381,
8228987977,
8227467326,
8223871225,
8222624249,
8229660299,
8226976944,
8221095481,
8222418227,
8223182706,
8228035112,
8220451925,
8224541459,
8220672659,
7681844042
)
UNION 
SELECT business_id as business, 
    account_number, 
    account_name, 
    CONCAT(first_name, ' ', last_name) as Full_Name
FROM virtualaccount
WHERE account_name ILIKE 'KIPPA%';
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