use hivedb;

-- 2.Most employee is working in which department
-- select Department,count(Department) as emp_count from parquet_hr_employee group by Department order by emp_count desc; 

--3.Highest number of job roles
--select JobRole,count(JobRole) as emp_count from parquet_hr_employee group by JobRole order by emp_count desc;

--4.Which gender have higher strength as workforce?
--select Gender,count(*) as emp_count from parquet_hr_employee group by Gender order by emp_count desc;

--5.Compare the marital status of employee and find the most frequent status.
--select MaritalStatus,count(*) as mar_count from parquet_hr_employee group by MaritalStatus order by mar_count desc;

--6.Mostly hired employee have qualification
--select Education,count(Education) as count from parquet_hr_employee group by Education order by count desc;

--7.Find the count of employee from which education fields
--select EducationField,count(EducationField) as count from parquet_hr_employee group by EducationField order by count desc;

--8.What is the job satisfaction level of employee?
--select JobSatisfaction, count(*) as Count_JobSatisfy from parquet_hr_employee group by JobSatisfaction order by Count_JobSatisfy desc;

--9.Does most of employee do overtime: Yes or No?
--select OverTime,count(*) as emp_count from parquet_hr_employee group by overtime order by emp_count desc;


--10.Find Min & Max Salaried employees.
select EmployeeID,min(Income),max(Income) from parquet_hr_employee group by EmployeeID;

--11.Does most of the employee do business travel? Find of the employees counts for each category
--select BusinessTravel,count(*) as count_emp from parquet_hr_employee group by BusinessTravel order by count_emp desc; 

--12.Find the AVG Income of graduate employee.
--select AVG(Income) from parquet_hr_employee where Education NOT IN('Below College','Master');
--select hr.Education,count(*) as count from parquet_EMP hr ,( select avg(income) avg_sal from 
--parquet_EMP) hr1 where hr.income > hr1.avg_sal group by hr.Education;
 

--13.Find the employee qualification receiving salary lower than equal to avg. salary of all employee. 
--select distinct(Education) from parquet_hr_employee as p1,(select avg(Income) as avg_sly from parquet__hr_employee) p2 where p1.Income < p2.avg_sly;

--14.When does the employee have highest chance of promotion in terms of working year?

--15.Highest attrition is in which department? Display this in % percentage as well.
--select Attrition,Department,count(Attrition) as count_emp from parquet_hr_employee group by Attrition,Department order by count_emp desc;

--16.Show marital status of Person having highest attrition rate.
select MaritalStatus,Attrition,count(Attrition) as count_emp from parquet_hr_employee group by Attrition,MaritalStatus order by count_emp desc; 
