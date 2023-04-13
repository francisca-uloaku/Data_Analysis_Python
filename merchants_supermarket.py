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
    merchants= pd.read_csv("https://docs.google.com/spreadsheets" +
    "2PA......" +
    "ut=csv", 
    header=[1])

    merchants = merchants.drop_duplicates('Full Number', keep='first')

    merchants['Date'].fillna(method='ffill', inplace=True)
    merchants['Date'] = pd.to_datetime(merchants['Date'])
    merchants['ACCOUNT NUMBER'].fillna(method='ffill', inplace=True)
    merchants['Full Number'].fillna(method='ffill', inplace=True)
    merchants = merchants[merchants['Full Number'] != 'FALSE']

    # merchants.to_csv('merchants.csv', index=False)

    all_merchants= merchants['Full Number'].nunique()

    numbers = tuple(merchants['Full Number'].unique())

    acc_num = tuple(merchants['ACCOUNT NUMBER'].unique())

    # stat['all_time'] = merchants['ACCOUNT NUMBER'].nunique()
    
    # onboarded_yest = merchants[merchants['Date'] == t_1()]
    # onboarded_today= merchants[merchants['Date'] == now()]

    # stat['yesterday'] = onboarded_yest['ACCOUNT NUMBER'].nunique()
    # stat['today'] = onboarded_today['ACCOUNT NUMBER'].nunique()
    
    # final=json.dumps(stat)

    # numbers = tuple(merchants['ACCOUNT NUMBER'].unique())

    return all_merchants, numbers, acc_num, merchants


def get_merchant_details():

    numbers=onboarding_stat()[1]
    account_number=onboarding_stat()[2]

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
    
    # all_time=all_time.sort_values('Transaction_value', ascending=False)

    final=pd.merge(account_details, all_time, how='outer', on='business')
    final=final.fillna(0)
    
    data = final[["Merchant Name", 
    "Business Name", 
    "Full Number", 
    "account_number", 
    "Transaction_volume", 
    "Transaction_value"]]

    data = data.sort_values('Transaction_value', ascending=False)

    data.to_csv('Supe.csv', index=False)

    merchants_found=len(account_details)

    return data, merchants_found, transacting_merchants, _transaction

print("Unique Supermarket Merchants: ", onboarding_stat()[0])
print(get_merchant_details())




# sq="""
# SELECT business_id AS business,
#     account_name AS "Merchant Business Name",
#     account_number AS "Account Number",
#     CONCAT(first_name, ' ', last_name) AS "Merchant Name"
# FROM virtualaccount
# WHERE account_number::BIGINT IN {};
# """.format(numbers)

# account_details=pd.read_sql(sq, conn)

# account_details= account_details.apply(pd.to_numeric, errors='ignore')

# business=tuple(account_details['business'])


# # sq2="""
# # SELECT b.id as business,
# #     p.phone_number
# # FROM person p
# # JOIN business b
# # ON p.id=owner
# # WHERE b.id IN {}
# # """.format(business)

# # phoneNumber=pd.read_sql(sq2, conn)

# # user_details = pd.merge(account_details, phoneNumber, how='outer', on='business')


# sq3="""
# SELECT business,
#     COUNT(*) AS "Transaction_volume",
#     SUM(amount) AS "Transaction_value"
# FROM transaction
# WHERE business IN {}
# AND status ='00'
# AND is_reversed IS FALSE
# AND is_reversed_action_response IS NULL
# GROUP BY 1
# ORDER BY 3 DESC,2 DESC;
# """.format(business)

# sq4="""
# SELECT *
# FROM transaction
# WHERE business IN {}
# AND status ='00'
# AND is_reversed IS FALSE
# AND is_reversed_action_response IS NULL;
# """.format(business)

# all_time_transactions=pd.read_sql(sq3, conn2)

# all_time_detailed_transactions=pd.read_sql(sq4, conn2)

# payment_channel_all_time=all_time_detailed_transactions.groupby(['type'])['amount'].agg(['sum', 'count']).reset_index().rename(columns={'sum':'Transaction Value','count' : 'Transaction Volume'})

# payment_summary_all_time=all_time_detailed_transactions['amount'].sum()
# transaction_count_all_time=len(all_time_detailed_transactions)
# payment_breakdown_all_time = dict_to_json(payment_channel_all_time)
# transacting_user_all_time=all_time_detailed_transactions['business'].nunique()



# yesterday_transactions= all_time_detailed_transactions[all_time_detailed_transactions['created_at'] == t_1()]
# payment_channel_yest=yesterday_transactions.groupby(['type'])['amount'].agg(['sum', 'count']).reset_index().rename(columns={'sum':'Transaction Value','count' : 'Transaction Volume'})
# payment_summary_yest=yesterday_transactions['amount'].sum()
# transaction_count_yest=len(yesterday_transactions)
# payment_breakdown_yest = dict_to_json(payment_channel_yest)
# transacting_user_yest=yesterday_transactions['business'].nunique()




# today_transactions= all_time_detailed_transactions[all_time_detailed_transactions['created_at'] == now()]
# payment_channel_today=yesterday_transactions.groupby(['type'])['amount'].agg(['sum', 'count']).reset_index().rename(columns={'sum':'Transaction Value','count' : 'Transaction Volume'})

# payment_summary_today=today_transactions['amount'].sum()
# transaction_count_today=len(today_transactions)
# payment_breakdown_today = dict_to_json(payment_channel_today)
# transacting_user_today=today_transactions['business'].nunique()




# print(payment_breakdown_all_time, payment_breakdown_yest, payment_breakdown_today)
# print(transacting_user_all_time, transacting_user_yest ,transacting_user_today)
# print(payment_summary_all_time, transaction_count_all_time) 
# print(payment_summary_yest, transaction_count_yest) 
# print(payment_summary_today, transaction_count_today) 



# # # final=pd.merge(user_details, all_time_transactions, how='outer', on='business')
# # final=pd.merge(account_details, all_time_transactions, how='outer', on='business')


# # data = final[["Merchant Name", 
# #     "Merchant Business Name", 
# #     # "phone_number", 
# #     "Account Number", 
# #     "Transaction_volume", 
# #     "Transaction_value"]]

# # data = data.apply(pd.to_numeric, errors='ignore')
# # data=data.fillna(0)

# # # # data=data.transform(np.sort)

# # data=data.sort_values('Transaction_value', ascending=False)


# # data.to_csv('cf.csv', index=False)

# # # data.to_excel("Merchant_transaction.xlsx",
# # #              sheet_name='Details_User',
# # #              index=False) 
