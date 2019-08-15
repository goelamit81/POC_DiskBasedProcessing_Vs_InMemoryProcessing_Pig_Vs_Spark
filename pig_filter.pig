data = LOAD '$input' USING PigStorage(',') AS (VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount);

filtered = FILTER data BY RatecodeID == 4;

STORE filtered into '$output';