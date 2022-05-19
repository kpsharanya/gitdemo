create database if not exists hremployeeDB;
use hremployeeDB;

drop table if exists hremployee;

create table if not exists hremployee(EmployeeID serial, Department varchar(255), JobRole varchar(255), Attrition varchar(10), Gender varchar(10), Age int,
MaritalStatus varchar(20), Education varchar(255),
EducationField varchar(255), BusinessTravel varchar(255), 
JobInvolvement varchar(255), JobLevel int, JobSatisfaction varchar(255),
Hourlyrate int, Income int,	Salaryhike int,	OverTime varchar(10),
Workex int,	YearsSinceLastPromotion int, EmpSatisfaction varchar(20),
TrainingTimesLastYear int, WorkLifeBalance varchar(20), 
Performance_Rating varchar(20), primary key(EmployeeID));

desc hremployee;

SET GLOBAL local_infile = true;

-- Load Data into HR_Employee
LOAD DATA LOCAL INFILE 'E:/MySQL/HR_Employee.csv' INTO TABLE hremployee
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- 1. Shape of table 
-- Returns Number of Rows
select count(EmployeeID) from hremployee;

-- Returns Number of Columns      
select count(*) from information_schema.columns where table_name = 'hremployee';

-- 2. Show the count of Employee & percentage Workforce in each Department.
select Department, count(*) as Count_EMP, count(*)*100/(select count(*) from hremployee) 
as percentage_dept from hremployee group by Department order by Count_EMP desc;


-- 3. Which gender have higher strength as workforce in each department ?
select Department, Gender, count(*) as COUNT_EMP from hremployee group by Department, Gender ORDER BY COUNT_EMP;

-- 4. Show the workforce in each JobRole
select JobRole, count(*) as Count_EMP from hremployee group by JobRole order by Count_EMP desc;

-- 5. Show Distribution of Employee's Age Group
-- ALTER TABLE HR_Employee DROP age_group;
 alter table hremployee add column age_group varchar(20);
 
 SET SQL_SAFE_UPDATES = 0;
 
 -- Assign Values to age_group 
 update hremployee 
 SET age_group = if ( age <= 25, '<25', if (age > 40, '40+', '25-40'));
 
 select age_group, COUNT(*) as emp_num from hremployee GROUP BY age_group;

 -- 6. Comapre all marital status of employee and find the most frequent marital status.
select MaritalStatus, count(*) as marital_count from hremployee group by MaritalStatus order by marital_count DESC; 

-- 7. What is Job satisfaction level of employee?
select JobSatisfaction, count(*) as Count_JobSatisfy, count(*)*100/(select count(*) from hremployee) 
as Per_JobSatisfy from hremployee group by JobSatisfaction order by Per_JobSatisfy desc;

-- 8. How frequently employee are going on Business Trip
select BusinessTravel, count(*) as Count_Trip, count(*)*100/(select count(*) from hremployee) 
as Percent_Trip from hremployee group by BusinessTravel order by Percent_Trip desc;

-- 9. Show the Department with Highest Attrition Rate (Percentage)
-- Observation : Research & Development Has Highest Attrition Rate
select Department,Attrition, count(*) as Count_EMP, count(*)*100/(select count(*) from hremployee) 
as Percent_EMP from hremployee group by Department,Attrition order by Percent_EMP desc;

select Department, sum(case when Attrition = 'Yes' then 1 else 0 end) as count_attr,
round(sum(case when attrition = 'Yes' then 1 else 0 end)*100/(select count(*) from hremployee), 2) as attr_rate from hremployee group by
Department;

-- 10. Show the Job Role with Highest Attrition Rate (Percentage)
-- Observation : Laboratory Technition Has Highest Attrition Rate
select JobRole, sum(case when Attrition = 'Yes' then 1 else 0 end) as count_attr,
round(sum(case when Attrition = 'Yes' then 1 else 0 end)*100/(select count(*) from hremployee), 2) as attr_rate from hremployee group by
JobRole order by attr_rate desc;

-- 11. Show Distribution of Employee's Promotion, Find the maximum chances of employee
-- getting Promoted
-- YearsSinceLastPromotion
select DISTINCT(YearsSinceLastPromotion) from hremployee;

alter table hremployee add column promotion_group varchar(30);

SET SQL_SAFE_UPDATES = 0;

update hremployee 
SET promotion_group = if ( YearsSinceLastPromotion <= 5, '<=5', 
if (YearsSinceLastPromotion > 10, '10+', '6-10')); 
select promotion_group, COUNT(*) as count_num from hremployee GROUP BY promotion_group;

-- 12. Find the Atrrition Rate for Marital Status
-- Observation : Highest Attrition is in Singles 
select MaritalStatus, Attrition, count(*) as Count_EMP, count(*)*100/(select count(*) from hremployee) 
as Percent_EMP from hremployee group by MaritalStatus,Attrition order by Percent_EMP desc;select MaritalStatus, sum(case when Attrition = 'Yes' then 1 else 0 end) as count_attr,
round(sum(case when Attrition = 'Yes' then 1 else 0 end)*100/(select count(*) from hremployee), 2) as attr_rate from hremployee group by
MaritalStatus order by attr_rate desc;

-- 13. Find the Attrition Count & Percentage for Different Education Levels
-- Observation: Higher Education have Lower Attrition Rate
select Education, Attrition, count(*) as Count_EMP, count(*)*100/(select count(*) from hremployee) 
as Percent_EMP from hremployee group by Education,Attrition order by Percent_EMP desc;

-- 14. Find the Attrition Count & Percentage for BusinessTravel
-- Observation: Attrition is High for Employees Travelling Frquently
select BusinessTravel, sum(case when attrition = 'Yes' then 1 else 0 end) as count_attr, 
round(SUM(if(attrition = 'yes', 1, 0))/count(*), 2) as attr_rate from hremployee group by BusinessTravel;

-- 15. Find the Attrition & Percentage Attrition for Various JobInvolvement
-- Observation: Low Job Involvement Leads to High Attrition Rate
select JobInvolvement, sum(case when attrition = 'Yes' then 1 else 0 end) as count_attr, 
round(SUM(if(attrition = 'yes', 1, 0))/count(*), 2) as attr_rate from hremployee 
group by JobInvolvement ORDER BY attr_rate;

-- 16. Show Attrition Rate for Different JobSatisfaction
-- Observation: Low Job Satisafaction leads to High Attrition Rate
select JobSatisfaction, sum(case when attrition = 'Yes' then 1 else 0 end) as count_attr, 
round(SUM(if(attrition = 'yes', 1, 0))/count(*), 2) as attr_rate from hremployee 
group by JobSatisfaction ORDER BY attr_rate;





