from operator import truediv
import psycopg2
import csv
from decouple import config
import pandas as pd
import dateutil
import json

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

def dict_to_json(dict):
    df = pd.DataFrame.from_dict(dict)
    data = df.to_json(orient='index')
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

stat=dict()

def onboarding_stat():

    global stat

    users = pd.read_csv("https://docs.google.com/spreadsheets/d/e/"+"2PACX-1vTj_c-MszPzJ0w6brVmRAADMhCywNzdoIqUVInWj3e3Hp0i3gnsuto9zvdnkKH9V1fulG_" +
    "rrXgwLrMg/pub?gid=2061214039&single=true&output=csv")

    stat['onboarded'] = len(users)
    stat['ambassadors'] = users['Ambassador'].nunique()

    user_per_ambassador = users['Ambassador'].value_counts()

    stat['users_per_ambassador']= user_per_ambassador.to_dict()

    phone_num = tuple(users['Full Number'])

    acc_num = tuple(users['KIPPA ACCOUNT NUMBER'])

    return users, phone_num, acc_num


def find_user(phone, account, users_details):

    users_details= users_details[['KIPPA ACCOUNT NUMBER', 'Ambassador']]

    users_details = users_details.apply(pd.to_numeric, errors='ignore')

    global stat

    sq="""
    SELECT v.created_at::DATE AS "Kippa_acct_Date",
        p.created_at::DATE AS "registered_date",
        CONCAT(v.first_name, ' ', v.last_name) AS "Merchant Name",
        v.account_name AS "Business Name",
        p.phone_number AS "Full Number",
        v.account_number AS "KIPPA ACCOUNT NUMBER",
        b.id as business
    FROM person p
    JOIN business b ON p.id=b.owner
    JOIN virtualaccount v ON b.id=v.business_id
    WHERE p.phone_number::BIGINT IN {}
    UNION
    SELECT v.created_at::DATE AS "Kippa_acct_Date",
        p.created_at::DATE AS "registered_date",
        CONCAT(v.first_name, ' ', v.last_name) AS "Merchant Name",
        v.account_name AS "Business Name",
        p.phone_number AS "Full Number",
        v.account_number AS "KIPPA ACCOUNT NUMBER",
        b.id as business
    FROM person p
    JOIN business b ON p.id=b.owner
    JOIN virtualaccount v ON b.id=v.business_id
    WHERE v.account_number::BIGINT IN {}
    """.format(phone, account)
    
    check_user= pull_data_from_db(query=sq, connection=kippa_conn)

    stat['users_found'] =len(check_user)

    business_id = tuple(check_user['business'])

    sq2="""
    SELECT business,
        COUNT(*) AS "Transaction_volume",
        SUM(amount) AS "Transaction_value"
    FROM transaction
    WHERE business IN {}
    AND status ='00'
    AND is_reversed IS FALSE
    AND is_reversed_action_response IS NULL
    GROUP BY 1
    ORDER BY 3 DESC,2 DESC;
    """.format(business_id)

    txn_details = pull_data_from_db(query=sq2, connection=txn_conn)

    stat['transacting_user']=txn_details['business'].nunique()

    total_details=pd.merge(check_user, txn_details, on='business', how='outer')

    total_details = total_details.sort_values('Transaction_value', ascending=False)

    total_details = total_details.apply(pd.to_numeric, errors='ignore')

    user_x_ambassador = pd.merge(total_details, users_details, on='KIPPA ACCOUNT NUMBER', how='outer')

    user_x_ambassador=user_x_ambassador.fillna(0)

    user_x_ambassador=user_x_ambassador.drop(columns=['business'])
    
    user_x_ambassador.to_csv('Ambassador_details.csv', index=False)

    stat=json.dumps(stat)

    return stat


users, phone_number, acc_number = onboarding_stat()


all = find_user(phone=phone_number, account=acc_number, users_details=users)

print(all)