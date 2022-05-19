create database if not exists flight_delayDB;

use flight_delayDB;

drop table if exists Flights;

-- 1.Create a Table Flights with schemas of Table
create table if not exists Flights (ID int,YEAR int,
MONTH int,DAY int,DAY_OF_WEEK int,AIRLINE varchar(20),
FLIGHT_NUMBER int,TAIL_NUMBER varchar(20),ORIGIN_AIRPORT varchar(20),
DESTINATION_AIRPORT varchar(20),SCHEDULED_DEPARTURE int,
DEPARTURE_TIME int,DEPARTURE_DELAY int,TAXI_OUT int,
WHEELS_OFF int,SCHEDULED_TIME int,ELAPSED_TIME int,
AIR_TIME int,DISTANCE int,WHEELS_ON int,TAXI_IN int,
SCHEDULED_ARRIVAL int,ARRIVAL_TIME int,ARRIVAL_DELAY int,
DIVERTED int,CANCELLED int,CANCELLATION_REASON varchar(20),
AIR_SYSTEM_DELAY int,SECURITY_DELAY int,AIRLINE_DELAY int,
LATE_AIRCRAFT_DELAY int,WEATHER_DELAY int,primary key(id));

SET GLOBAL local_infile = true;

-- 2.Insert all records into flights table. Use dataset Flights_Delay.csv
LOAD DATA LOCAL INFILE 'E:/MySQL/Flights_Delay.csv' INTO TABLE Flights
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- 3.Average Arrival delay caused by airlines
select AIRLINE, avg(ARRIVAL_DELAY) as avg_delay from Flights group by AIRLINE;

-- 4.Display the Day of Month with AVG Delay [Hint: Add Count() of Arrival & Departure Delay]
select MONTH, DAY, (DEPARTURE_DELAY + ARRIVAL_DELAY)/2 as AVERAGE_DELAY from Flights
group by MONTH , DAY order by MONTH;

-- 5.Analysis for each month with total number of cancellations.
select MONTH,count(CANCELLED) as cancelled from Flights group by MONTH order by MONTH; 

-- 6.Find the airlines that make maximum number of cancellations
select AIRLINE, sum(CANCELLED) as cancellation from Flights group by AIRLINE order by cancellation desc limit 1;

-- 7.Finding the Busiest Airport [Hint: Find Count() of origin airport and destination airport]
-- for table of flight operations
-- select DESTINATION_AIRPORT , count(ORIGIN_AIRPORT) as SRC, count(DESTINATION_AIRPORT) as DST from Flights group by DESTINATION_AIRPORT order by DST desc ;

select * from (select DESTINATION_AIRPORT , 2*count(DESTINATION_AIRPORT)  as No_of_flight_operations from Flights
 group by DESTINATION_AIRPORT order by No_of_flight_operations desc) as most_busy_airport limit 1 ;

-- 8.Find the airlines that make maximum number of Diversions [Hint: Diverted = 1 indicate Diversion]
select AIRLINE,count(*) as max_num_DIVERTED from Flights where DIVERTED = 1 group by AIRLINE 
order by max_num_DIVERTED desc limit 1; 
-- select AIRLINE,sum(DIVERTED) as diversions FROM Flights group by  AIRLINE order by diversions desc; 

-- 9.Finding all diverted Route from a source to destination Airport & which route is the most diverted route.
select ORIGIN_AIRPORT, DESTINATION_AIRPORT, count(DIVERTED) as diversion from Flights 
group by ORIGIN_AIRPORT,DESTINATION_AIRPORT order by Diversion;

-- 10.Finding all Route from origin to destination Airport & which route got delayed.
select ORIGIN_AIRPORT,DESTINATION_AIRPORT,(DEPARTURE_DELAY + ARRIVAL_DELAY)/2 as AVERAGE_DELAY 
from Flights where (DEPARTURE_DELAY + ARRIVAL_DELAY)/2 > 0 order by AVERAGE_DELAY desc;



-- 11.Finding the Route which Got Delayed the Most 
-- [ Hint: Route include Origin Airport and Destination Airport, Group By Both ]
select * from (select ORIGIN_AIRPORT,DESTINATION_AIRPORT, WEATHER_DELAY + LATE_AIRCRAFT_DELAY + 
AIRLINE_DELAY + SECURITY_DELAY + AIR_SYSTEM_DELAY + (DEPARTURE_DELAY + ARRIVAL_DELAY)/2 as total_delay from Flights group by 
ORIGIN_AIRPORT,DESTINATION_AIRPORT order by total_delay desc) as most_delayed_route limit 1;

-- 12.Finding AIRLINES with its total flight count, total number of flights arrival delayed by more than 30 Minutes, 
-- % of such flights delayed by more than 30 minutes when it is not Weekends with minimum count of flights from Airlines 
-- by more than 10. Also Exclude some of Airlines 'AK', 'HI', 'PR', 'VI' and arrange output in descending order by % of such count of flights.
-- select AIRLINE,count(*) as total_count from Flights where ARRIVAL_DELAY > 30;

-- Airline and Flight_number
select AIRLINE, FLIGHT_NUMBER from Flights group by FLIGHT_NUMBER order by FLIGHT_NUMBER desc ;

-- Total Flight per AIRLINE
select AIRLINE, count(total_operational_flight) as FLIGHT_COUNT from (select AIRLINE, FLIGHT_NUMBER as total_operational_flight , ARRIVAL_DELAY
from Flights group by FLIGHT_NUMBER order by AIRLINE ) as Airline_FLIGHT_data group by AIRLINE;

-- -- Total Flight per AIRLINE with operational delay > 30
select AIRLINE, count(total_operational_flight) as FLIGHT_COUNT from (select AIRLINE, FLIGHT_NUMBER as total_operational_flight ,
 ARRIVAL_DELAY from Flights group by FLIGHT_NUMBER order by AIRLINE ) as Airline_FLIGHT_data where ARRIVAL_DELAY > 30 group by AIRLINE ;
 
 -- -- Total Flight per AIRLINE with operational delay > 30 && Total_operational_flight >= 10 not AK HI PR VI Full week data
 select AIRLINE, count(total_operational_flight) as FLIGHT_COUNT from (select AIRLINE, FLIGHT_NUMBER as total_operational_flight ,
 ARRIVAL_DELAY from Flights group by FLIGHT_NUMBER order by AIRLINE ) as Airline_FLIGHT_data where ARRIVAL_DELAY > 30 and AIRLINE 
 not in ( 'AK', 'HI', 'PR', 'VI') group by AIRLINE having FLIGHT_COUNT >=10;
 
 -- -- Total Flight per AIRLINE with operational delay > 30 && Total_operational_flight >= 10 not AK HI PR VI week data excluding weekends
 select AIRLINE, count(total_operational_flight) as FLIGHT_COUNT from (select AIRLINE, FLIGHT_NUMBER as total_operational_flight ,
 ARRIVAL_DELAY , DAY_OF_WEEK from Flights group by FLIGHT_NUMBER order by AIRLINE ) as Airline_FLIGHT_data where ARRIVAL_DELAY > 30 and DAY_OF_WEEK < 6 and AIRLINE 
 not in ( 'AK', 'HI', 'PR', 'VI') group by AIRLINE having FLIGHT_COUNT >=10;
 

 
 