import psycopg2
from decouple import config
import pandas as pd
from datetime import date, timedelta
import json
import dateutil


def create_conn(host, db, user, password, port):

    conn = psycopg2.connect(
        host=config(host),
        database=config(db),
        user=config(user),
        password=config(password),
        port=config(port),
    )
    return conn


def pull_data_from_db(query, connection):
    data = pd.read_sql(query, connection)
    return data


try_conn = create_conn(
    host="host", db="database",
    user="user", 
    password="password", 
    port="port"
)

query = """
SELECT first_name,
    last_name,
    country_code,
    phone_number,
    phone_number_verified, 
    image_url,
    email,
    email_verified,
    created_at
FROM person
WHERE source='GOOGLE'
AND created_at::DATE >= '2022-12-19'
ORDER BY 8 DESC;
"""

data = pull_data_from_db(query=query, connection=try_conn)

dta = pd.read_csv("/Users.......csv")


df = dta.rename(columns={"Customer (email)": "email"})

df["ref_length"] = df["Reference"].apply(lambda x: len(x))

df["first_five"] = df["Reference"].str[:5]

df = df[df["ref_length"] <= 23]

filter_values = ["GST"]

df = df[~df["first_five"].str.upper().isin(filter_values)]

df_check = df[["email", "Reference"]]

merged_df = pd.merge(data, df_check, on="email", how="left", indicator=True)

merged_df = merged_df[
    [
        "first_name",
        "last_name",
        "country_code",
        "phone_number",
        "phone_number_verified",
        "image_url",
        "email",
        "email_verified",
        "created_at",
    ]
]

merged_df.sort_values(by='created_at', ascending=False).to_csv("result.csv", index=False)
