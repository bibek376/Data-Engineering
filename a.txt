from sqlalchemy import create_engine
import pandas as pd 
from time import time
import argparse
import os
import logging

#user_name
#password
#host 
#port
#database
#table_name
#url_of_csv_file


def main(params):

    user_name=params.user_name
    password=params.password
    host=params.host
    port=params.port
    database=params.database
    table_name=params.table_name
#   table_name="ny_taxi_data"
    url_of_csv_file=params.url_of_csv_file

#https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
    
    csv_name='output.csv.gz'
    csv_name_file='output.csv'

    #CSV File download

    os.system(f"wget {url_of_csv_file} -O {csv_name}")
    os.system(f"gunzip {csv_name}")
    print("File download Successfully")

    engine=create_engine(f'postgresql://{user_name}:{password}@{host}:{port}/{database}')
    engine.connect()

    df_iter=pd.read_csv(csv_name_file,iterator=True,chunksize=100000)
    df=next(df_iter)

    df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)

    #Table Schema
    df.head(n=0).to_sql(name=f'{table_name}',con=engine,if_exists='replace')
    print(f"Table {table_name} has been created!!!")
    #Insert First 100000 data in table
    df.to_sql(name=f'{table_name}',con=engine,if_exists='append')
    #Upload all data in SQL table

    while True:
        try:
            t_start=time()

            df=next(df_iter)
            df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=f'{table_name}',con=engine,if_exists='append')

            t_end=time()

            print("Inserted another Chunk,took%.3f second" %(t_start-t_end))
        except StopIteration:
            print("All data has been processed.")
            break



if __name__=="__main__":

    parser = argparse.ArgumentParser(description='Ingest CSV data to PostgreSQL')

    parser.add_argument('--user_name',help='user name for Postgres')
    parser.add_argument('--password',help='password for Postgres')
    parser.add_argument('--host',help='host name for Postgres')
    parser.add_argument('--port',help='Port for Postgres')
    parser.add_argument('--database',help='Database name used for postgres')
    parser.add_argument('--table_name',help='table name in postgres')
    parser.add_argument('--url_of_csv_file',help='csv link for yellow trip data')

    args = parser.parse_args()

    main(args)
