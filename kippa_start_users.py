import psycopg2
import csv
from decouple import config
import pandas as pd
import requests
import json
import time
import os
from os import listdir
from os.path import join


HOME=os.getcwd()
t0 = time.time()


def create_conn(host,db,user,password,port):

    conn= psycopg2.connect(
       host=config(host),
       database = config(db),
       user = config(user),
       password = config(password),
       port = config(port))
    return conn


def pull_data_from_db(query, connection):
    data = pd.read_sql(query, connection)
    return data

biz_conn = create_conn(
    host='host_biz',
    db='database_biz',
    user='user_biz',
    password='password_biz',
    port='port_biz')


sq = """
WITH TEMP AS (
SELECT *,
DATE_PART('day', submission_date - payment_date) * 24 + DATE_PART('hour', submission_date - payment_date) as diff_in_hr
FROM tat_table
)

SELECT * 
FROM TEMP
WHERE diff_in_hr <=100.0
"""

user_bucket = pull_data_from_db(query=sq, connection=biz_conn)

user_buscket_email = tuple(user_bucket['email'].unique())

sq2="""
SELECT first_name,
    last_name,
    email as recipients,
    phone
FROM all_bizreg_payments
WHERE email IN {}
""".format(user_buscket_email)

kippa_start = pull_data_from_db(query=sq2, connection=biz_conn)

input_file = open ('DB_scripts/application.json')
json_array = json.load(input_file)

data_file=json_array.get('data')
bydate=(data_file['value']['byDate'])

empty_list=list()
for value in bydate.values():
    empty_list.append(value)

full_list=list()
for element in empty_list:
    for item in element:
        full_list.append(item)

full_list=json.dumps(full_list)

df = pd.read_json(full_list, orient ='records')

df =df[['occupation',
        'surname',
        'firstName',
        'otherNames',
        'ownerState',
        'businessState',
        'recipients']]

# df = df[df['businessState']=='Lagos State']

all_detials = pd.merge(kippa_start, df,on='recipients', how='left')

all_detials.to_csv("Kippa_start_users.csv", index=False)

kippa_start.to_csv("Kippa_users.csv", index=False)