--creating database named airline_delayDB
--create database airline_delayDB;

-- Use Database
use airline_delayDB;

--a) Create external table flights
--drop table if exists flights;

--create external table if not exists flights(ID int,YEAR int,MONTH int,DAY int,DAY_OF_WEEK int,AIRLINE varchar(10),FLIGHT_NUMBER int,TAIL_NUM
BER varchar(10),ORIGIN_AIRPORT varchar(10),DESTINATION_AIRPORT varchar(10),SCHEDULED_DEPARTURE int,DEPARTURE_TIME int,DEPARTURE_DELAY int,TAXI
_OUT int,WHEELS_OFF int,SCHEDULED_TIME int,ELAPSED_TIME int,AIR_TIME int,DISTANCE int,WHEELS_ON int,TAXI_IN int,SCHEDULED_ARRIVAL int,ARRIVAL_
TIME int,ARRIVAL_DELAY int,DIVERTED int,CANCELLED int,CANCELLATION_REASON int,AIR_SYSTEM_DELAY int,SECURITY_DELAY int,AIRLINE_DELAY int,LATE_A
IRCRAFT_DELAY int,WEATHER_DELAY int)
--row format delimited
--fields terminated by ','
--lines terminated by '\n'
--stored as textfile
--location '/flight_delay';

--Display first five rows of table
select * from flights limit 5;

--b) Create external table with parquet file format
--drop table if exists parquet_flights;

--create external table if not exists parquet_flights(ID int,YEAR int,MONTH int,DAY int,DAY_OF_WEEK int,AIRLINE varchar(10),FLIGHT_NUMBER int,TAIL_NUMBER varchar(10),ORIGIN_AIRPORT varchar(10),DESTINATION_AIRPORT varchar(10),SCHEDULED_DEPARTURE int,DEPARTURE_TIME int,DEPARTURE_DELAY int,TAXI_OUT int,WHEELS_OFF int,SCHEDULED_TIME int,ELAPSED_TIME int,AIR_TIME int,DISTANCE int,WHEELS_ON int,TAXI_IN int,SCHEDULED_ARRIVAL int,ARRIVAL_TIME int,ARRIVAL_DELAY int,DIVERTED int,CANCELLED int,CANCELLATION_REASON int,AIR_SYSTEM_DELAY int,SECURITY_DELAY int,AIRLINE_DELAY int,LATE_AIRCRAFT_DELAY int,WEATHER_DELAY int)
--row format delimited
--fields terminated by ','
--lines terminated by '\n'
--stored as parquetfile;


--4. Insert data from external table to partquet table
--insert into table parquet_flights
--select * from flights;

--c)Describe the table schema & show top 10 rows of Dataset
select * from parquet_flights limit 10;

