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

def clean_dict(d):
    # Account for partnership emails
    email = d['recipients']
    email = '|'.join(email) if isinstance(email, list) else str(email)
    # save changes and return
    d['recipients'] = email 
    return d

def get_submission_data():

    url = "https://m.kippa.cc/apps/bizreg/api/v1/fetch-submissions"
    payload={}
    Authorization = config('Authorization')
    headers = {
    'Authorization': Authorization
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    data=json.loads(response.text)
    data_file=data.get('data')
    bydate=(data_file['value']['byDate'])

    empty_list=list()
    for value in bydate.values():
        empty_list.append(value)

    full_list=list()
    for element in empty_list:
        for item in element:
            full_list.append(clean_dict(item))

    full_list=json.dumps(full_list)

    df = pd.read_json(full_list, orient ='records')
    df.rename(columns = {'recipients':'email', 'observed_at':'submission_date'}, inplace = True)
    df=df[['email', 'submission_date']]
    # df = df[df["email"].str.contains(",") == False]

    df.to_csv('data.csv', index=False)

    return df

def get_application_payment_data():

    sq="""
    SELECT DISTINCT a.email,
        a.created_at AS application_date,
        ab.created_at AS payment_date
    FROM applications a
    RIGHT JOIN all_bizreg_payments ab 
    ON a.email=ab.email
    """
    data=pull_data_from_db(query=sq, connection=biz_conn)

    return data

def merge_all():

    submission=get_submission_data()
    application_payment=get_application_payment_data()

    data = pd.merge(application_payment, submission, on='email', how='left')

    data= data[['email', 'payment_date', 'application_date', 'submission_date']]

    data = data.sort_values('payment_date', ascending=False)


    data=data.dropna()

    # data.to_csv('Merged.csv', index=False)


    # all_data= len(data)

    data = data[data['payment_date'] < data['application_date']]

    # after=len(data)

    # data.to_csv('after.csv', index=False)

    data=data[data['application_date'] < data['submission_date']]

    # later=len(data)

    data.to_csv('Merged.csv', index=False)

    later=len(data)

    return "Done", later

print(merge_all())

def create_schema_data():

    cursor=biz_conn.cursor()

    cursor.execute("""
        DROP TABLE IF EXISTS Tat_table;

        CREATE TABLE Tat_table (
            Email VARCHAR(255),
            Payment_date TIMESTAMP,
            Application_date TIMESTAMP,
            Submission_date TIMESTAMP);
        """)

    with open('Merged.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO Tat_table VALUES (%s, %s, %s, %s)",
        row
    )
 
    biz_conn.commit()

    return 'Done'

def delete_files(dir, ext):
  get_path = lambda f : join(dir, f)
  def delete(filepath):
    try:
      os.remove(filepath)
      return 1
    except:
      return 0
  return [delete(get_path(f)) for f in listdir(dir) if f.endswith(f'.{ext}')]

def cleanup():
  delete_files(dir=HOME, ext='csv')


if __name__ == '__main__':
    merge_all()
    create_schema_data()
    # time.sleep(20)
    cleanup()


t1 = time.time()

diff = t1-t0
print(t0, t1, diff)
