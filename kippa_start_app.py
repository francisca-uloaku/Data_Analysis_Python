import psycopg2
from decouple import config
import pandas as pd
import requests 
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


kippa_conn = create_conn(
    host='host',
    db='database',
    user='user',
    password='password',
    port='port')

biz_conn = create_conn(
    host='host_biz',
    db='database_biz',
    user='user_biz',
    password='password_biz',
    port='port_biz')

kippa_start= """

    SELECT DISTINCT email 
    FROM applications 
    WHERE phone_number NOT LIKE '%value%'
    """

kippa_app="""

    SELECT DISTINCT email 
    FROM person 
    WHERE email IS NOT NULL AND email != ''
    """

def get_user_email(sq, connection):

    data = pull_data_from_db(sq, connection)

    email_unique= set(list(data['email'].unique()))

    return email_unique

def get_non_kippa_user():

    start_user = get_user_email(sq=kippa_start, connection=biz_conn)
    
    app_user = get_user_email(sq=kippa_app, connection=kippa_conn)

    non_kippa_user = start_user - app_user

    return non_kippa_user


def get_application_data():

    url = "https://m.kippa.cc/apps/bizreg/api/v1/fetch-applications"
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
            full_list.append(item)

    full_list=json.dumps(full_list)

    df = pd.read_json(full_list, orient ='records')

    df.to_csv("Application_data.csv", index=False)

    return df

def filter_application_list():

    dataframe = get_application_data()

    filter_email = get_non_kippa_user()

    email_list = list(filter_email)

    filtered = dataframe[dataframe['email'].isin(email_list)]
    
    details = filtered[['surname', 
                        'firstName', 
                        'otherNames', 
                        'phoneNumber', 
                        'email', 
                        'gender']]
    
    details = details.drop_duplicates()

    details.to_csv("data.csv", index=False)

    return "done"

print(filter_application_list())