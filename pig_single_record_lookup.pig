data = LOAD '$input' USING PigStorage(',') AS (VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount);

filtered = FILTER data BY VendorID == 2 AND tpep_pickup_datetime == '2017-10-01 00:15:30' AND tpep_dropoff_datetime == '2017-10-01 00:25:11' AND passenger_count == 1 AND trip_distance == 2.17;

STORE filtered into '$output';