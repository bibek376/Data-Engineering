select * from zones z ;
select * from ny_taxi_data ntd ;
-- ALTER TABLE zones DROP COLUMN index;
-- ALTER TABLE ny_taxi_data DROP COLUMN index;


---Join 
SELECT
  t.tpep_pickup_datetime,
  t.tpep_dropoff_datetime,
  t.total_amount,
  CONCAT(zpu."Borough", ' / ', zpu."Zone") As "pickup_loc",
  CONCAT(zdo."Borough", ' / ' , zpu."Zone") As "dropoff_loc"
from ny_taxi_data t
join zones zpu on t."PULocationID" = zpu."LocationID"
join zones zdo on t."DOLocationID" = zdo."LocationID"
limit 100;


--check if pickup and dropoff location (foreign key)

select * from zones z ;
select * from ny_taxi_data ntd ;

SELECT
  t.tpep_pickup_datetime,
  t.tpep_dropoff_datetime,
  t.total_amount
from ny_taxi_data t
left join zones zpu on t."PULocationID" = zpu."LocationID"
left join zones zdo on t."DOLocationID" = zdo."LocationID"
where zpu."LocationID" is null
or zdo."LocationID" is null;


---delete record

select * from zones z ;

delete from zones where "LocationID" =10;

--number of trips per day
select date_trunc('DAY',ntd.tpep_dropoff_datetime),count(*)  
from ny_taxi_data ntd 
group by 1
order by 2 desc;

