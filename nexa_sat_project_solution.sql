   -- create table in the schema
create table nexa_sat(  
  customer_id varchar(50),
            gender varchar(10),
            partner varchar(3),
            dependants varchar(3),
            senior_citizen int,
            call_duration float,
            data_usage float,
            plan_type varchar(20),
            plan_level varchar(20),
            monthly_bill_amount float,
            tenure_months int,
            multiple_lines varchar(3),
            tech_support varchar(3),
            churn int);
            
	select*
    from nexa_sat;
 
   -- data cleaning checking for duplicates

select *,
row_number() over(
partition by customer_id, gender, partner, dependants, senior_citizen, call_duration, data_usage, plan_type, plan_level, monthly_bill_amount, 
tenure_months, multiple_lines, tech_support, churn) as row_num
from nexa_sat;

with duplicate_cte as
(
select *,
row_number() over(
partition by customer_id, gender, partner, dependants, senior_citizen, call_duration, data_usage, plan_type, plan_level, monthly_bill_amount, 
tenure_months, multiple_lines, tech_support, churn) as row_num
)
select*
from duplicate_cte
where row_num > 1;


 -- checking for null values
 
 select *
 from nexa_sat
 where customer_id is null
 or gender is null
 or partner is null 
 or dependants is null
 or senior_citizen is null 
 or call_duration is null
 or data_usage is null
 or plan_type is null
 or plan_level is null 
 or monthly_bill_amount is null
 or tenure_months is null 
 or multiple_lines is null 
 or tech_support is null
 or churn is null;
 
 -- EDA
-- total users 

select count(customer_id) as current_users
from nexa_sat
where churn = 0;

-- total number of users by plan level

select plan_level, count(customer_id) as total_users 
from nexa_sat
where churn = 0
group by 1;

-- total revenue

select round(sum(monthly_bill_amount ),2) as revenue
from nexa_sat;

-- revenue by plan level

select plan_level,round(sum(monthly_bill_amount ),2) as revenue
from nexa_sat
group by 1;

-- churn count by plan type and plan level

select plan_level,
  plan_type,
count(*)  as  total_customers,
sum(churn) as churn_count
from nexa_sat
group by 1,2
order by 1;

-- avg tenure by plan level

select plan_level, round(avg(tenure_months),2) as avg_tenure
from nexa_sat
group by 1;

-- marketing segments
-- createtables of existing users

create table existing_users
select*
from nexa_sat
where churn = 0;

select*
from existing_users;

-- calculate avg rev per existing user

select round(avg(monthly_bill_amount),2) as ARPU
from existing_users;

-- cal CLV and add column

alter table existing_users
add column clv float;

update  existing_users
set clv =  monthly_bill_amount * tenure_months;

select customer_id, clv
from existing_users;

--  clv score alter
-- monthly bill = 40%, tenure = 30%, call durstion = 10%, data usage 10%, premium = 10%

alter table existing_users
add column clv_score numeric(10,2);

describe existing_users;

alter table existing_users
modify clv_score numeric(12,4);

update existing_users
set clv_score = round(
				(0.4 * monthly_bill_amount) +
			   (0.3 * tenure_months) +
               (0.1 * call_duration) +
               (0.1 * data_usage) +
               (0.1 * case when plan_level = 'premium'
			  then 1 else 0 end),
              4 );

select customer_id, clv_score
from existing_users;     
			
  -- group users into segments based on clv_score
  
alter table existing_users
add column clv_segments varchar(50);

alter table existing_users
modify clv_segments varchar(50);

UPDATE existing_users
SET clv_segments = 
    CASE 
        WHEN clv_score > 85 THEN 'High Value'
        WHEN clv_score >= 50 THEN 'Moderate Value'
        WHEN clv_score >= 25 THEN 'Low Value'
        ELSE 'Churn Risk'
    END;
    
    
    select customer_id, clv, clv_score, clv_segments
    from existing_users;
    
-- analyzing segments
-- avg bill and tenure per segments

select clv_segments,
      round(avg(monthly_bill_amount), 2) as avg_monthly_charges,
     round(avg(tenure_months), 2) as avg_tenure
      from existing_users
      group by 1;
      
      -- tech support and multiple lines percent
      
      select clv_segments,
      round(avg(case when tech_support = 'yes' then 1 else 0 end ), 2) as tech_support_pct,
      round(avg(case when multiple_lines = 'yes' then 1 else 0 end), 2) as multiple_lines_pct
      from existing_users
      group by 1;
      
      -- revenue per segment 
      
      select clv_segments, count(customer_id),
		sum(monthly_bill_amount * tenure_months) as total_revenue
             from existing_users
             group  by 1;
             
-- cross selling and up selling 
-- cross selling tech support to snr citizens


SELECT customer_id
FROM existing_users
WHERE senior_citizen = 1 -- senior citizens
AND dependents = 'No' -- no children or tech savvy helpers
AND tech_support = 'No' -- do not already have this service
AND (clv_segments = 'Churn Risk' OR clv_segments = 'Low Value');


-- cross-selling: multiple lines for partners and dependants

SELECT customer_id
FROM existing_users
WHERE multiple_lines = 'No'
AND (dependents = 'Yes' OR partner = 'Yes')
AND plan_level = 'Basic';

-- up selling premuim discount for basic users with churn risk

select customer_id
from existing_users
where clv_segments = 'churn risk'
and plan_level = 'basic';

-- up selling basic to premium for longer lock in period and higher ARPU

select plan_level, round(avg(monthly_bill_amount), 2) as avg_bill, round(avg(tenure_months), 2) as avg_tenure
from existing_users
where clv_score = 'high value'
or clv_segments = 'modrate value'
group by 1;

-- select customer

select customer_id, monthly_bill_amount
from existing_users
where plan_level = 'basic'
and (clv_segments = 'high value' or clv_segments = 'modrate value')
and monthly_bill_amount > 150;

-- create stored procedure
-- snr citizens who will be offered tech support

DELIMITER //

CREATE PROCEDURE tech_support_snr_citizens()
BEGIN
    -- Select query to get the desired result
    SELECT eu.customer_id
    FROM existing_users eu
    WHERE eu.senior_citizen = 1  -- senior citizens
    AND eu.dependents = 'No'     -- no children or tech savvy helpers
    AND eu.tech_support = 'No'   -- do not already have this service
    AND (eu.clv_segments = 'Churn Risk' OR eu.clv_segments = 'Low Value');
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE churn_risk_discount()
BEGIN
    -- Select query to get the desired result
    SELECT eu.customer_id
    FROM existing_users eu
    WHERE eu.clv_segments = 'Churn Risk'
    AND eu.plan_level = 'Basic';
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE high_usage_basic()
BEGIN
    -- Select query to get the desired result
    SELECT eu.customer_id
    FROM existing_users eu
    WHERE eu.plan_level = 'Basic'
    AND eu.clv_segments = 'High Value'
    AND eu.monthly_bill_amount > 150;
END //

DELIMITER ;-- use procedures
-- churn_risk_discount
select*
from churn_risk_discount();

-- high usage basic
select*
from high_usage_basic();
