import psycopg2
from decouple import config
import pandas as pd
from datetime import date, timedelta
import json

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


def dict_to_json(dict):
    df = pd.DataFrame.from_dict(dict)
    data = df.to_json(orient='index')
    return data

def now():
    return date.today()


def t_1():
    today=now()
    yesterday = today - timedelta(days=1)
    yesterday= '{}'.format(yesterday)
    return yesterday


def onboarding_stat():

    stat=dict()


    # Onyin's file
    merchants= pd.read_csv("https://docs.google.com/spreadsheets/d/e/"+"2PACX-1vRpQwnrOHf7oj6UF06uZUL-toAMZcahh6E5soHHJs9-0_1D6yngr5OZCZp"+
    "-YZufy-ndr9NQpTqTZt8Z/pub?gid=0&single=true&output=csv")

    merchants = merchants.drop_duplicates('Full Number', keep='first')

    # merchants['Date'].fillna(method='ffill', inplace=True)
    # merchants['Date'] = pd.to_datetime(merchants['Date'])
    # merchants['Kippa Account number'].fillna(method='ffill', inplace=True)
    merchants['Full Number'].fillna(method='ffill', inplace=True)
    merchants = merchants[merchants['Full Number'] != 'FALSE']

    # merchants.to_csv('merchants.csv', index=False)

    all_merchants= merchants['Full Number'].nunique()

    numbers = tuple(merchants['Full Number'].unique())

    # acc_num = tuple(merchants['Kippa Account Number'].unique())

    acc_num = tuple(merchants['Full Number'].unique())


    return all_merchants, numbers, acc_num, merchants


def get_merchant_details():

    numbers=onboarding_stat()[1]
    account_number=onboarding_stat()[2]

    acc_nu= (8225865782, 
        8229983528, 
        8225884217, 
        8225320741, 
        8220409733,
        8223350553,
        8221222437
        )

    sq="""
    SELECT v.created_at::DATE AS "Date",
        CONCAT(v.first_name, ' ', v.last_name) AS "Merchant Name",
        v.account_name AS "Business Name",
        p.phone_number AS "Full Number",
        v.account_number,
        b.id as business
    FROM person p
    JOIN business b ON p.id=b.owner
    JOIN virtualaccount v ON b.id=v.business_id
    WHERE p.phone_number::BIGINT IN {}
    UNION
    SELECT v.created_at::DATE AS "Date",
        CONCAT(v.first_name, ' ', v.last_name) AS "Merchant Name",
        v.account_name AS "Business Name",
        p.phone_number AS "Full Number",
        v.account_number,
        b.id as business
    FROM person p
    JOIN business b ON p.id=b.owner
    JOIN virtualaccount v ON b.id=v.business_id
    WHERE v.account_number::BIGINT IN {}
    """.format(numbers, acc_nu)

    account_details=pd.read_sql(sq, conn)

    business_id = tuple(account_details['business'])

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

    all_time=pd.read_sql(sq2, conn2)

    transacting_merchants=len(all_time)

    all_time = all_time.apply(pd.to_numeric, errors='ignore')
    
    # all_time=all_time.sort_values('Transaction_value', ascending=False)

    sq3="""
    SELECT created_at::DATE AS "Date",
        type,
        amount,
        business
    FROM transaction
    WHERE business IN {}
    AND status ='00'
    AND is_reversed IS FALSE
    AND is_reversed_action_response IS NULL
    ORDER BY 1;
    """.format(business_id)

    dataframe=pd.read_sql(sq3, conn2)

    _transaction= dict()

    payment_channel=dataframe.groupby(['type'])['amount'].agg(['sum', 'count']).reset_index().rename(columns={'sum':'Transaction Value','count' : 'Transaction Volume'})
    _transaction['payment_summary']=dataframe['amount'].sum()
    _transaction['transaction_count']=len(dataframe)
    _transaction['payment_breakdown']= dict_to_json(payment_channel)
    _transaction['transacting_user']=dataframe['business'].nunique()

    final=pd.merge(account_details, all_time, how='outer', on='business')
    final=final.fillna(0)
    
    data = final[["Merchant Name", 
    "Business Name", 
    "Full Number", 
    "account_number", 
    "Transaction_volume", 
    "Transaction_value"]]

    data = data.sort_values('Transaction_value', ascending=False)

    data.to_csv('car_dealers.csv', index=False)

    merchants_found=len(account_details)

    return data, merchants_found, transacting_merchants, _transaction

print("Unique Car Dealers Merchants: ", onboarding_stat()[0])
print(get_merchant_details())
