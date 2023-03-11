#!/usr/bin/env python
# coding: utf-8

import pandas as pd 
from sqlalchemy import create_engine
import argparse

# user 
# password
# host
# port
# database name
# table name
# url of the csv

def main(params):
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    database=params.database
    table_name=params.table_name
    url=params.url
    engine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    engine.connect()



#   CSV download
    csv_name='output.csv'
    



parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

parser.add_argument('user',help='user name for postgres')
parser.add_argument('password',help='password for postgres')
parser.add_argument('host',help='host for postgres')
parser.add_argument('port',help='port for postgresql')
parser.add_argument('database',help='database name for postgres')
parser.add_argument('table_name',help='name of the table where we will write data')
parser.add_argument('url',help='url of the csv file')


args = parser.parse_args()
print(args.accumulate(args.integers))





df=pd.read_csv('/home/bibek/Downloads/train.csv')


df.pickup_datetime=pd.to_datetime(df.pickup_datetime)
df.dropoff_datetime=pd.to_datetime(df.dropoff_datetime)



df_iter=pd.read_csv('/home/bibek/Downloads/train.csv',iterator=True,chunksize=20000)



df.pickup_datetime=pd.to_datetime(df.pickup_datetime)
df.dropoff_datetime=pd.to_datetime(df.dropoff_datetime)



df.head(n=0).to_sql(name='ny_taxi',con=engine,if_exists='replace')

df.to_sql(name='ny_taxi',con=engine,if_exists='append')
