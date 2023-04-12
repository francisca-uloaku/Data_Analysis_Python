import psycopg2
from decouple import config
import pandas as pd
from datetime import date, timedelta
import json
import argparse
import math


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


def merchants_list(category):

    # First List
    # merchants= pd.read_csv("https://docs.google.com/spreadsheets/d/e/" +"2PACX-1vSdoLhKv_LWSw-eu1R_m-fA2Iws3hNHCMjqgfgaDz3hdA8HL9yrFjd38"+
    # "p2qnxzEj-9Mbe5QwukVzz6C/pub?gid=0&single=true&output=csv")

    # Updated List
    merchants= pd.read_csv("https://docs.google.com/spreadsheets/d/e/"+"2PACX-1vSdoLhKv_LWSw-eu1R_m-fA2Iws3hNHCMjqgfgaDz3hdA8HL9yrFjd38"+
    "p2qnxzEj-9Mbe5QwukVzz6C/pub?gid=2011518318&single=true&output=csv")

    
    merchants = merchants.drop_duplicates('Full Number', keep='first')
    
    merchants['DATE'].fillna(method='ffill', inplace=True)
    merchants['DATE'] = pd.to_datetime(merchants['DATE'])
    merchants['ACCOUNT NUMBER'].fillna(method='ffill', inplace=True)

    supermarket_merchant=merchants[merchants['CATEGORY'] == 'Super Market Vendors']
    car_merchants=merchants[merchants['CATEGORY'] == 'Car Dealers']
    offline_merchants=merchants[merchants['CATEGORY'] == 'Offline Activation']
    bdc_merchants=merchants[merchants['CATEGORY'] == 'BDC']
    specified_category=merchants[merchants['CATEGORY'] == category]
    specified_category = specified_category[specified_category['Full Number'] != 'FALSE']

    all_merchants= merchants['Full Number'].nunique()

    all_merchants_split=merchants['CATEGORY'].value_counts()

    all_merchants_split=dict_to_json(all_merchants_split)

    numbers = tuple(merchants['Full Number'].unique())

    acc_num = tuple(merchants['ACCOUNT NUMBER'].unique())

    # return (len(supermarket_merchant), len(car_merchants), len(offline_merchants), len(bdc_merchants), len(specified_category))

    return specified_category

def get_merchants_phone_account(dataframe):
    number, account = tuple(dataframe['Full Number'].unique()), tuple(dataframe['ACCOUNT NUMBER'].unique())
    return number, account


def get_merchant_transactions(numbers, account_number):

    if args.category=='Super Market Vendors':
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
        AND v.account_number::BIGINT IN {}
        """.format(numbers, account_number)
    else:
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
        """.format(numbers)

    account_details=pd.read_sql(sq, conn)

    business_id = tuple(account_details['business'])

    merchants_found= len(business_id)

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

    final=pd.merge(account_details, all_time, how='outer', on='business')

    final=final.fillna(0)
    
    data = final[["Merchant Name", 
    "Business Name", 
    "Full Number", 
    "account_number", 
    "Transaction_volume", 
    "Transaction_value"]]

    data = data.sort_values('Transaction_value', ascending=False)

    data.to_csv(args.category + " " +str(now()) + '.csv', index=False)

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

    return merchants_found, _transaction, transacting_merchants


parser = argparse.ArgumentParser()
parser.add_argument('--category', '-c', type=str, required=True)

if __name__ == '__main__':
    args = parser.parse_args()
    selected_category=args.category
    list_of_merchants = merchants_list(category=selected_category)
    details=get_merchants_phone_account(list_of_merchants)
    print(get_merchant_transactions(numbers=details[0], account_number=details[1]))
    