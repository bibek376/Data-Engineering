#!/usr/bin/env python
# coding: utf-8

from sqlalchemy import create_engine
import pandas as pd 
from time import time

engine=create_engine('postgresql://postgres:password@localhost:5000/ny_taxi_db')
engine.connect()



df_iter=pd.read_csv('/home/bibek/Desktop/Data_Engineering/Data/yellow_tripdata_2021-01.csv',iterator=True,chunksize=100000)
df=next(df_iter)


df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)



df.head(n=0).to_sql(name='ny_taxi_data',con=engine,if_exists='replace')



df.to_sql(name='ny_taxi_data',con=engine,if_exists='append')


#Upload all data in SQL table

while True:
    try:
        t_start=time()

        df=next(df_iter)

        df.tpep_pickup_datetime=pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime=pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name='ny_taxi_data',con=engine,if_exists='append')

        t_end=time()

        print("Inserted another Chunk,took%.3f second" %(t_start-t_end))
    except StopIteration:
        print("All data has been processed.")
        break




