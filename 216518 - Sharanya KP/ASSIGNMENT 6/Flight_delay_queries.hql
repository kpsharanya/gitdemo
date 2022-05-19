use airline_delayDB;

--d)Average arrival delay caused by airlines
--select AIRLINE, avg(ARRIVAL_DELAY) as avg_delay from parquet_flights group by AIRLINE order by avg_delay;

--e)Days of months with respected to average of arrival delays
--select DAY,avg(ARRIVAL_DELAY) as avg_delay from parquet_flights group by DAY order by DAY;

--f)Arrange weekdays with respect to the average arrival delays caused
--select DAY_OF_WEEK,avg(ARRIVAL_DELAY) as avg_delay from parquet_flights group by DAY_OF_WEEK order by avg_delay;

--g)Arrange Days of month as per cancellations done in Descending
--select DAY,count(CANCELLED) as cancelled from parquet_flights group by DAY order by DAY desc; 

--h)Finding busiest airports with respect to day of week
--select DAY_OF_WEEK, count(ORIGIN_AIRPORT) as origin_count,count(DESTINATION_AIRPORT) as des_count from parquet_flights group by DAY_OF_WEEK order by origin_count desc,des_count desc;

--i)Finding airlines that make the maximum number of cancellations
--select AIRLINE, sum(CANCELLED) as cancellation from parquet_flights group by AIRLINE order by cancellation desc limit 1;

--j)Find and order airlines in descending that make the most number of diversions
--select AIRLINE,count(*) as max_num_DIVERTED from parquet_flights where DIVERTED = 1 group by AIRLINE order by max_num_DIVERTED desc limit 1;

--k)Finding days of month that see the most number of diversion
--select DAY,count(*) as max_num_DIVERTED from parquet_flights where DIVERTED = 1 group by DAY order by max_num_DIVERTED desc;

--l)Calculating mean and standard deviation of departure delay for all flights in minutes
--select stddev_pop(DEPARTURE_DELAY),avg(DEPARTURE_DELAY) from parquet_flights;

--m)Calculating mean and standard deviation of arrival delay for all flights in minutes
--select stddev_pop(ARRIVAL_DELAY),avg(ARRIVAL_DELAY) from parquet_flights;

--n)Create a partitioning table “flights_partition” using partitioned by schema “CANCELLED”

--set hive.exec.dynamic.partition=true;
--set hive.exec.dynamic.partition.mode=nonstrict;

--create external table if not exists flights_partition(ID int,YEAR int,MONTH int,DAY int,DAY_OF_WEEK int,AIRLINE varchar(10),FLIGHT_NUMBER int,TAIL_NUMBER varchar(10),ORIGIN_AIRPORT varchar(10),DESTINATION_AIRPORT varchar(10),SCHEDULED_DEPARTURE int,DEPARTURE_TIME int,DEPARTURE_DELAY int,TAXI_OUT int,WHEELS_OFF int,SCHEDULED_TIME int,ELAPSED_TIME int,AIR_TIME int,DISTANCE int,WHEELS_ON int,TAXI_IN int,SCHEDULED_ARRIVAL int,ARRIVAL_TIME int,ARRIVAL_DELAY int,DIVERTED int,CANCELLATION_REASON int,AIR_SYSTEM_DELAY int,SECURITY_DELAY int,AIRLINE_DELAY int,LATE_AIRCRAFT_DELAY int,WEATHER_DELAY int)
--partitioned by (CANCELLED int)
--row format delimited
--fields terminated by ','
--lines terminated by '\n';

describe formatted flights_partition;

--Insert the Data into Partitioned Table
--insert overwrite table flights_partition
--partition(CANCELLED)
--select * from flights;

--o)Create Bucketing table “Flights_Bucket” using clustered by MONTH into 3 Buckets Note: No partitioning, only bucketing of table.

--create external table if not exists Flights_Bucket(ID int,YEAR int,MONTH int,DAY int,DAY_OF_WEEK int,AIRLINE varchar(10),FLIGHT_NUMBER int,TAIL_NUMBER varchar(10),ORIGIN_AIRPORT varchar(10),DESTINATION_AIRPORT varchar(10),SCHEDULED_DEPARTURE int,DEPARTURE_TIME int,DEPARTURE_DELAY int,TAXI_OUT int,WHEELS_OFF int,SCHEDULED_TIME int,ELAPSED_TIME int,AIR_TIME int,DISTANCE int,WHEELS_ON int,TAXI_IN int,SCHEDULED_ARRIVAL int,ARRIVAL_TIME int,ARRIVAL_DELAY int,DIVERTED int,CANCELLED int,CANCELLATION_REASON int,AIR_SYSTEM_DELAY int,SECURITY_DELAY int,AIRLINE_DELAY int,LATE_AIRCRAFT_DELAY int,WEATHER_DELAY int)
--clustered by (MONTH) into 3 buckets
--row format delimited
--fields terminated by ','
--lines terminated by '\n'
--stored as textfile;

describe formatted Flights_Bucket;

--Insert Bucket Data
--insert overwrite table Flight_Bucket
--select * from flights;

select * from Flights_Bucket limit 5;

--p)Get count of data of each bucket.
select count(*) from Flight_Bucket;

--q)finding all diverted Route from a source to destination Airport & which route is the most diverted
select ORIGIN_AIRPORT, DESTINATION_AIRPORT, count(DIVERTED) as diversion from Flights_Bucket 
group by ORIGIN_AIRPORT,DESTINATION_AIRPORT order by Diversion;


