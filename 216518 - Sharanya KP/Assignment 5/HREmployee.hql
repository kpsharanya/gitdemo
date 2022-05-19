-- Use Database
use hivedb;

--2. Create external hr_employee table
drop table if exists hr_employee;

create external table if not exists hr_employee(EmployeeID int,Department varchar(60),JobRole varchar(60),Attrition varchar(20),Gender varchar(10),Age int,MaritalStatus varchar(10),Education varchar(20),EducationField varchar(60),BusinessTravel varchar(60),JobInvolvement varchar(20),JobLevel int,JobSatisfaction varchar(20),Hourlyrate int,Income int,Salaryhike int,OverTime varchar(20),Workex int,YearsSinceLastPromotion int,EmpSatisfaction varchar(20),TrainingTimesLastYear int,WorkLifeBalance varchar(20),Performance_Rating varchar(20))
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile
location '/hr_employees';

--3. Display first five rows of table
select * from hr_employee limit 5;

--3. Create external table with parquet file format
drop table if exists parquet_hr_employee;

create external table if not exists parquet_hr_employee(EmployeeID int,Department varchar(60),JobRole varchar(60),Attrition varchar(20),Gender varchar(10),Age int,MaritalStatus varchar(10),Education varchar(20),EducationField varchar(60),BusinessTravel varchar(60),JobInvolvement varchar(20),JobLevel int,JobSatisfaction varchar(20),Hourlyrate int,Income int,Salaryhike int,OverTime varchar(20),Workex int,YearsSinceLastPromotion int,EmpSatisfaction varchar(20),TrainingTimesLastYear int,WorkLifeBalance varchar(20),Performance_Rating varchar(20))
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as parquetfile;


--4. Insert data from external table to partquet table
insert into table parquet_hr_employee
select * from hr_employee;

-- Display first 5 Rows
select * from parquet_hr_employee limit 5;
