import pandas as pd
import dateutil
from datetime import date, timedelta
import logging
import argparse



logging.basicConfig(
    filename="paystack.log",
    level=logging.INFO,
    format="%(asctime)s: %(levelname)s:%(message)s"
    )


def t_1():
    today=date.today()
    yesterday = today - timedelta(days = 1)
    return yesterday


def read_data(file_dir):
    df=pd.read_csv(file_dir)
    df['datetime'] = df['Transaction Date'].apply(dateutil.parser.parse)
    df['date'] = pd.to_datetime(df['datetime']).dt.date
    df['Time'] = pd.to_datetime(df['datetime']).dt.time
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    df['day'] = df['datetime'].dt.day
    df.rename(columns= {'Channel':'Transaction_type'}, inplace = True)
    logging.info(file_dir)
    df=df[df['Status'] == 'success']
    df_yest = df[df['date'] == t_1()]
    return df_yest

def dict_to_json(dict):
    df = pd.DataFrame.from_dict(dict)
    data = df.to_json(orient='index')
    return data

def amount_transaction(dataframe):
    df=dataframe
    ks = df[df['Amount Paid'] == 15000]
    ks_value = ks['Amount Paid'].sum()
    ks_channel = ks.groupby(['Transaction_type'])['Amount Paid'].agg(['sum', 'count'])

    payment= df[df['Amount Paid'] != 15000]    
    transacting_users = payment['Customer (email)'].nunique()
    payment_value = payment['Amount Paid'].sum()
    payment_channel =payment.groupby(['Transaction_type'])['Amount Paid'].agg(['sum', 'count']).reset_index().rename(columns={'sum':'Transaction Value','count' : 'Transaction Volume'})


    data = [{'Item': 'Transaction Value', 'Number': int(payment_value)},
            {'Item': 'Transaction Volume',  'Number': int(len(payment))}
            ]

    transacting_details = [{'Item': 'Transacting User', 'Number': int(transacting_users)},
                            {'Item': 'Transaction Volume',  'Number': int(payment_value)}
                        ]
    
    channel_type = payment_channel.to_json(orient='index')

    
    payment_details = dict_to_json(data)

    uni_user_volume = dict_to_json(transacting_details)

    return payment_details, channel_type, uni_user_volume


parser = argparse.ArgumentParser()
parser.add_argument('--file', '-f', type=str, required=True)

if __name__ == '__main__':
    args = parser.parse_args()
    file_dir=args.file
    data = read_data(file_dir)
    All_details= amount_transaction(data)
    logging.info(All_details)
    print("Complete")  