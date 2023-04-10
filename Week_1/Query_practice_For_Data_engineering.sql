select * from zones z ;
select * from ny_taxi_data ntd ;
-- ALTER TABLE zones DROP COLUMN index;
-- ALTER TABLE ny_taxi_data DROP COLUMN index;

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










