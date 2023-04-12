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

def get_business_ids(start_date, end_date):

    sq="""
    SELECT b.id as business
    FROM business b
    JOIN person p on p.id=b.owner
    WHERE p.created_at::DATE BETWEEN '{}' AND '{}'
    """.format(start_date, end_date)

    business_ids=pull_data_from_db(query=sq, connection=kippa_conn)

    data = tuple(business_ids['business'].unique())

    return data

def registration_date(business_ids):

    sq = """
    SELECT p.created_at::DATE,
        count(*) as onbaording
    FROM business b
    JOIN person p on p.id=b.owner 
    WHERE b.id IN {}
    GROUP BY 1
    ORDER BY 1
    """.format(business_ids)

    onboarding = pull_data_from_db(query=sq, connection=kippa_conn)

    return onboarding

def account_creation(business_ids):

    sq="""
    SELECT created_at::DATE,
        count(*) as account_creation
    FROM virtualaccount
    WHERE business_id IN {}
    GROUP BY 1
    ORDER BY 1
    """.format(business_ids)

    account = pull_data_from_db(query=sq, connection=kippa_conn)

    return account 


def payment_details(business_ids):

    sq = """
    WITH TEMP AS(
    SELECT business,
        MIN(Created_at::DATE) as created_at
    FROM transaction
    WHERE business IN {}
    GROUP BY 1)
    
    SELECT created_at,
        COUNT(*) as payment
    FROM TEMP
    GROUP BY 1
    ORDER BY 1
    """.format(business_ids)

    payment = pull_data_from_db(query=sq, connection=txn_conn)

    return payment



business = get_business_ids(start_date='2022-07-01', end_date='2022-07-31')
onboarding = registration_date(business) 
account = account_creation(business)
payment = payment_details(business)

onboarding_account = pd.merge(onboarding, account, on='created_at', how='left')

onboarding_account_payment = pd.merge(onboarding_account, payment, on='created_at', how='left')

onboarding_account_payment.to_csv("conversation_data.csv", index=False)
