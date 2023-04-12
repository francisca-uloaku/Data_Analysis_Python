import psycopg2
from decouple import config
import pandas as pd
from datetime import date, timedelta
import json
import dateutil


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
    merchants = merchants[merchants['Full Number'] != 'FALSE']

    all_merchants= merchants['Full Number'].nunique()

    all_merchants_split=merchants['CATEGORY'].value_counts()

    all_merchants_split=dict_to_json(all_merchants_split)

    numbers = tuple(merchants['Full Number'].unique())

    acc_num = tuple(merchants['ACCOUNT NUMBER'].unique())

    # merchants=merchants.dropna()
    unique_merchants_Onboarded=all_merchants


    return unique_merchants_Onboarded, numbers, all_merchants_split, merchants, acc_num


# Use phone and account number to get merchants details
def get_merchant_details():

    numbers=onboarding_stat()[1]
    account_number=onboarding_stat()[4]

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
    WHERE p.phone_number IN {}
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

    account_details=pd.read_sql(sq, conn)

    # account_details= account_details.apply(pd.to_numeric, errors='ignore')

    account_details['Date']=account_details['Date'].astype('datetime64[ns]')

    full_details_stat=dict()

    yest=account_details[account_details['Date'] == t_1()]
    today=account_details[account_details['Date'] == now()]

    full_details_stat['all time']=len(account_details)
    full_details_stat['yesterday']=len(yest)
    full_details_stat['today']=len(today)



    account_details2=pd.merge(account_details, onboarding_stat()[3], how='left', on='Full Number')

    # account_details2 = account_details2.drop_duplicates('ACCOUNT NUMBER', keep='first')

    business=tuple(account_details['business'])

    return account_details, business, full_details_stat #, account_details2


def all_time_transaction_stat(business_id):

    sq="""
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

    all_time_transactions=pd.read_sql(sq, conn2)

    data = all_time_transactions.apply(pd.to_numeric, errors='ignore')

    data=data.fillna(0)

    data=data.sort_values('Transaction_value', ascending=False)

    data.to_csv("All_time_transaction_spend.csv", index=False)

    all_time_amount=data['Transaction_value'].sum()
    all_time_count= data['Transaction_volume'].sum()
    transacting_users= len(data['business'])


    return data, all_time_amount, all_time_count, transacting_users

def transaction_breakdown(dataframe):
    
    _transaction= dict()

    payment_channel=dataframe.groupby(['type'])['amount'].agg(['sum', 'count']).reset_index().rename(columns={'sum':'Transaction Value','count' : 'Transaction Volume'})
    _transaction['payment_summary']=dataframe['amount'].sum()
    _transaction['transaction_count']=len(dataframe)
    _transaction['payment_breakdown']= dict_to_json(payment_channel)
    _transaction['transacting_user']=dataframe['business'].nunique()

    return _transaction

def transaction_details(business_id):

    sq="""
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

    all_time_details= pd.read_sql(sq, conn2)
    
    all_time_details.to_csv('all_time_details.csv', index=False)

    today_date= pd.datetime.now().date()
    yesterday_date = today_date - pd.to_timedelta(1, unit='d')

    # yesterday=all_time_details[all_time_details['Date'] == t_1()]
    # today=all_time_details[all_time_details['Date'] == now()]

    yesterday=all_time_details[all_time_details['Date'] == yesterday_date]
    today=all_time_details[all_time_details['Date'] == today_date]


    all_time_breakdown=transaction_breakdown(all_time_details)
    yesterday_breakdown=transaction_breakdown(yesterday)
    today_breakdown=transaction_breakdown(today)

    return all_time_breakdown, yesterday_breakdown, today_breakdown, yesterday, today, all_time_details


merchant_onboarding_stat= onboarding_stat()
unique_merchants=  merchant_onboarding_stat[0]
Merchants_with_Phone_Numbers= merchant_onboarding_stat[1]
merchants_category= merchant_onboarding_stat[2]
Merchants_DF= merchant_onboarding_stat[3]
Merchants_Account_Numbers=merchant_onboarding_stat[4]


print("Total Number of merchants onboarded: ", unique_merchants)
print("Onboarded Merchants Split by Category: ", merchants_category)

merchant_details=get_merchant_details()
merchant_details[0].to_excel("Found_details.xlsx",
              sheet_name='User_Details', index=False)
print("Total Number of Merchants whose details were found: ", len(merchant_details[0]))
print("Total Number of merchants details breakdown: ", merchant_details[2])


transaction_summary=all_time_transaction_stat(merchant_details[1])
print("All time transaction sum:", transaction_summary[1])
print("All time transaction count:", transaction_summary[2])
print("All time transacting users:", transaction_summary[3])


final= transaction_details(business_id=merchant_details[1])
print("ALL time breakdown:", final[0])
print("Yesterday breakdown:", final[1])
print("Today breakdown:", final[2])

print(final[3])
print(final[4])
print(final[5])