import psycopg2
from decouple import config
import pandas as pd
from datetime import date, timedelta
import json
import pendulum

# Mobile App Database
conn = psycopg2.connect(
    host= config('host'),
    database = config('database'),
    user = config('user'),
    password = config('password'),
    port = config('port')) 
cursor = conn.cursor()



sq="""
SELECT t.created_by,
    t.created_at::DATE,
    (date(t.created_at) - '2022-01-01')/7 + 1 as week2
FROM transaction t
JOIN person p ON t.created_by=p.id
WHERE p.created_at::DATE BETWEEN'2022-01-01' AND '2022-05-10'
ORDER BY 1
"""
all_transactions=pd.read_sql(sq, conn)

cursor.close()

week=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]

weeks=[]
for i in week:
    data= ("Week "+ str(i))
    weeks.append(data)

print(tuple(weeks))
# weeks=list(all_transactions['week2'].unique())


weekly_users=dict()
for i in week:
    data=all_transactions[['created_by','week2']]
    data=data[data['week2']== i]
    weekly_users["Week "+str(i)]= set(data['created_by'].unique())
    # unique_users_per_week=data['created_by'].unique()
    # weekly_users.append(tuple(unique_users_per_week))

user_breakdown= dict()
for item, value in weekly_users.items():
    user_breakdown[item]=len(value)
    print(item, len(value))

data= pd.DataFrame(user_breakdown.items(), columns=['Week', 'NumberOfUsers'])

data.to_csv('Weekly_Users.csv', index=False)

week_1= weekly_users.get("Week 1")
week_2= weekly_users.get("Week 2")
week_3= weekly_users.get("Week 3")
week_4= weekly_users.get("Week 4")
week_5= weekly_users.get("Week 5")
week_6= weekly_users.get("Week 6")
week_7= weekly_users.get("Week 7")
week_8= weekly_users.get("Week 8")
week_9= weekly_users.get("Week 9")
week_10= weekly_users.get("Week 10")
week_11= weekly_users.get("Week 11")
week_12= weekly_users.get("Week 12")
week_13= weekly_users.get("Week 13")
week_14= weekly_users.get("Week 14")
week_15= weekly_users.get("Week 15")
week_16= weekly_users.get("Week 16")
week_17= weekly_users.get("Week 17")
week_18= weekly_users.get("Week 18")
week_19= weekly_users.get("Week 19")

# print(len(week_2.intersection(week_3)))
# print(len(week_5.intersection(week_5)))
# print(len(week_5.intersection(week_5)))
# print(len(week_7.intersection(week_7)))
# print(len(week_7.intersection(week_7)))
# print(len(week_7.intersection(week_7)))
# print(len(week_7.intersection(week_8)))
# print(len(week_12.intersection(week_12)))
# print(len(week_12.intersection(week_12)))
# print(len(week_12.intersection(week_12)))
# print(len(week_13.intersection(week_13)))
# print(len(week_13.intersection(week_15)))
# print(len(week_15.intersection(week_15)))
# print(len(week_15.intersection(week_16)))
# print(len(week_17.intersection(week_17)))
# print(len(week_17.intersection(week_17)))
print(len(week_18.intersection(week_18)))
print(len(week_18.intersection(week_19)))


