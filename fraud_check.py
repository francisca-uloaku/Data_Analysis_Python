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


try_conn = create_conn(
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

def get_business_id():

    data=pd.read_csv("https://docs.google.com/spreadsheets/"+"2................."+
    "hg...............=csv")

    acc_num = tuple(data['try_Account_Number'].unique())

    sq="""
    SELECT business_id, 
        account_number  
    FROM virtualaccount
    WHERE account_number::BIGINT IN {}
    """.format(acc_num)

    business=pull_data_from_db(query=sq, connection=try_conn)

    business_id=tuple(business['business_id'].unique())

    return business_id

business_ids = get_business_id()


sq="""
select business, 
    current_balance 
FROM wallet w
where lien > 1000000 or is_blacklisted = true
UNION
SELECT business, 
    current_balance  
FROM wallet
WHERE business IN {}
""".format(business_ids)

suspect= pull_data_from_db(query=sq, connection=wallet_conn)

suspect_business=tuple(suspect['business'].unique())


sq2="""
SELECT type,
    amount, 
    created_at, 
    reference, 
    status,
    gross, 
    business,
    "fundingId",
    "Id"
FROM transaction
Where business IN {}
--AND status = '00'
--AND is_reversed IS FALSE
--AND is_reversed_action_response IS NULL
AND type='bank_transfer';
""".format(suspect_business)

bank_transfer = pull_data_from_db(query=sq2, connection=txn_conn)

reference=tuple(bank_transfer['reference'])

# bank_transfer.to_csv('first_round.csv', index=False)

sq3="""
SELECT beneficiary_name AS "Beneficiary name",
    bank_name AS "Beneficiary bank",
    beneficiary_account_number AS account_number,
    reference
FROM funds_transfer
WHERE reference IN {}
""".format(reference)

ben_details = pull_data_from_db(query=sq3, connection=txn_conn)

user_business=tuple(bank_transfer['business'])

sq_3="""
SELECT CONCAT(first_name, ' ', last_name) as t_full_name,
    account_number as kippa_account_number,
    business_id as business
FROM virtualaccount
WHERE business_id IN {}
""".format(user_business)

bank_acct= pull_data_from_db(query=sq_3, connection=try_conn)

sq_2_="""
SELECT p.phone_number, 
    p.email,
    b.id as business
FROM person p
JOIN business b
ON p.id=b.owner
WHERE b.id IN {};
""".format(user_business)

other_details= pull_data_from_db(query=sq_2_, connection=try_conn)

bank_transfer_deeds = pd.merge(bank_transfer, ben_details, on='reference', how='outer')

user_full_details=pd.merge(bank_acct, other_details, on='business', how='outer')

bank_transfer_details=pd.merge(bank_transfer_deeds, user_full_details, on='business', how='inner')

all_details_bank = pd.merge(bank_transfer_details, suspect, on='business', how='inner')

all_details_bank=all_details_bank[['type',
                                            'amount',
                                            'created_at',
                                            'reference',
                                            'Beneficiary name',
                                            'Beneficiary bank',
                                            'account_number',
                                            't_full_name',
                                            't_account_number',
                                            'phone_number',
                                            'email',
                                            'status',
                                            'current_balance']]
                                            
all_details_bank=all_details_bank.sort_values('t_account_number', ascending=False)
all_details_bank.to_csv('bank_transfer_details.csv', index=False)

sq4="""
SELECT type,
    amount, 
    created_at, 
    reference , 
    status,
    gross, 
    business,
    "fundingId",
    "Id"
FROM transaction
Where business IN {}
--AND status = '00'
--AND is_reversed IS FALSE
--AND is_reversed_action_response IS NULL
AND type='dedicated_virtual_account'
;""".format(suspect_business)

dva_transaction= pull_data_from_db(query=sq4, connection=txn_conn)

dva_reference= tuple(ua_transaction['reference'])
dva_business=tuple(ua_transaction['business'])

sq5= """
SELECT
    client_reference as reference,
    --response,
    response::json -> 'originatorname' AS "Sender name",
    response::json -> 'bankname' AS "Sender bank",
    response::json -> 'originatoraccountnumber' as "Sender Account Number"
FROM transaction
WHERE client_reference IN {}
""".format(dva_reference)

dva_details=pull_data_from_db(query=sq5, connection=pay_conn)


sq6="""
SELECT CONCAT(first_name, ' ', last_name) as t_full_name,
    account_number as t_account_number,
    business_id as business
FROM virtualaccount
WHERE business_id IN {}
""".format(ua_business)

ua_bank_acct= pull_data_from_db(query=sq_3, connection=try_conn)

sq7="""
SELECT p.phone_number, 
    p.email,
    b.id as business
FROM person p
INNER JOIN business b
ON p.id=b.owner
WHERE b.id IN {};
""".format(ua_business)

dva_other_details= pull_data_from_db(query=sq7, connection=try_conn)
 
dva_trans_details= pd.merge(ua_transaction, ua_details, on='reference', how='outer')

dva_user_details =pd.merge(ua_bank_acct, ua_other_details, on='business', how='outer')

dva_full_details = pd.merge(ua_trans_details, ua_user_details, on='business', how='inner')

all_details_ua = pd.merge(ua_full_details, suspect, on='business', how='inner')


all_details_ua=all_details_dva[['type',
                                    'amount',
                                    'created_at',
                                    'reference',
                                    'Sender name',
                                    'Sender bank',
                                    'Sender Account Number',
                                    't_full_name',
                                    't_account_number',
                                    'phone_number',
                                    'email',
                                    'status',
                                    'current_balance']]

all_details_ua=all_details_dva.sort_values('t_account_number', ascending=False)

all_details_ua.to_csv("ua_full_details.csv", index=False)


