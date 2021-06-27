Find the d which is just greater than a and then find the 5th element in Arithimetic progression. a is starting number while d is difference in arithimetic progression

with test as (
    select a,min(d - a) as min from first join diff on 1=1 where (d-a)>0 group by a)
select a,d,(a + (d*4)) as term from first join diff on 1=1 where (d - a) = (select distinct min from test where first.a=test.a);

-----------------------------------------------------------------------------

select distinct Name, Address, COUNT(Name) OVER (PARTITION BY Name,Address) as Number_Of_Entries, LIST_AGG(Resources, ',') within group (order by Name,Address) "Resources" from Entries;

-------------------------------------------------------------------------------

DECLARE before_replace_timestamp TIMESTAMP;

-- Create table books.
CREATE TABLE `books` AS
SELECT 'Hamlet' title, 'William Shakespeare' author;

-- Get current timestamp before table replacement.
SET before_replace_timestamp = CURRENT_TIMESTAMP();

-- Replace table with different schema(title and release_date).
/*CREATE OR REPLACE TABLE books AS
SELECT 'Hamlet' title, DATE '1603-01-01' release_date;

-- This query returns Hamlet, William Shakespeare as result.
SELECT * FROM books FOR SYSTEM_TIME AS OF before_replace_timestamp;*/

-------------------------------------------------------------------------------
PIVOT

create table `produce` as
WITH Produce AS (
  SELECT 'Kale' as product, 51 as sales, 'Q1' as quarter UNION ALL
  SELECT 'Kale', 23, 'Q2' UNION ALL
  SELECT 'Kale', 45, 'Q3' UNION ALL
  SELECT 'Kale', 3, 'Q4' UNION ALL
  SELECT 'Apple', 77, 'Q1' UNION ALL
  SELECT 'Apple', 0, 'Q2' UNION ALL
  SELECT 'Apple', 25, 'Q3' UNION ALL
  SELECT 'Apple', 2, 'Q4')
SELECT * FROM Produce;

SELECT * FROM
  (SELECT * FROM `produce`)
  PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
  
----------------------------------------------------------------------------------
Array and Struct

WITH locations AS
  (SELECT 'I am identifier' as identifier,ARRAY<STRUCT<city STRING, state STRING>>[("Seattle", "Washington"),
    ("Phoenix", "Arizona")] AS location,
    ARRAY<STRUCT<country STRING, continent STRING>>[("US", "NA"),
    ("Phoenix", "Arizona")] AS geography)
SELECT identifier,l.LOCATION,l.geography
FROM locations l;

----------------------------------------------------------------------------------
Netflix Watchtime

with a as(select start_time,end_time,max(end_time) over() max_time,lag(end_time,1,0) over(order by end_time) as end_tm from `netflix` order by start_time asc),
b as (select case when end_tm=0 then (0-start_time) when start_time>end_tm then (end_tm - start_time) else 0 end as diff,max_time from a),
c as (select sum(diff) as diff, max_time from b group by max_time)
select diff + max_time as total_watched from c;

----------------------------------------------------------------------------------
Remove transitive duplicates

Input Table
Col1 Col2
A      B
A      B
B      A
A      C

Output Table - A B and B A are considered to be transitive duplicates.
Col1 Col2
A      B
A      C

select distinct least(col1,col2),greatest(col1,col2) from `tab`;

------------------------------------------------------------------------------------
--Find min and max of continuous sequence along with group
--Input Table
/*
Group Seq
A       1
A		2
A		3
A		5
A		6
A		8
A		9
B		11
C		1
C		2
C		3
*/
--Desired Output

--SQL
select group, max(seq) as max_seq, min(seq) as min_seq from

(select group, sequence, sequence - row_number() over(partition by group order by sequence) as split from tab) A
group by group,split order by group asc;

/*Here sequence - row_number() will return split values of same number for a group for continuous sequence, like for A for 1,2 and 3 sequence we will have difference of 0 while for 5 and 6, we will have difference of 1 as row_number will be 4 for seq 5 record.*/

---------------------------------------------------------------------------------------
--Student Table has three columns Student_Name, Total_Marks and Year. User has to write a SQL query to display Student_Name, Total_Marks, Year,  Prev_Yr_Marks for those whose Total_Marks are greater than or equal to the previous year.

with a as(
select lag(Total_Marks,1,0) over(partition by Student_Name order by year desc) as Prev_Yr_Marks, Student_Name, Year from Student
)

select tab.Student_Name, tab.Total_Marks, tab.Year, a.Prev_Yr_Marks from Student tab inner join a on tab.Student_Name=a.Student_Name and tab.Year=a.Year where Total_Marks>=Prev_Yr_Marks;

----------------------------------------------------------------------------------------
--Emp_Details  Table has four columns EmpID, Gender, EmailID and DeptID. User has to write a SQL query to derive another column called Email_List to display all Emailid concatenated with semicolon associated with a each DEPT_ID  as shown below in output Table.

select DeptID, STRING_AGG(EmailID,';' order by EmailID) FROM Emp_Details group by DeptID;

----------------------------------------------------------------------------------------
--Order_Tbl has four columns namely ORDER_DAY, ORDER_ID, PRODUCT_ID, QUANTITY and PRICE
--(a) Write a SQL to get all the products that got sold on both the days and the number of times the product is sold.
--(b) Write a SQL to get products that was ordered on 02-May-2015 but not on 01-May-2015
--(a)
with prev_day_sale as(
select ORDER_ID, PRODUCT_ID, lag(ORDER_DAY,1,'') over(Partition by ORDER_ID,PRODUCT_ID order by ORDER_DAY) as prev_order_day from Order_Tbl
)

select a.Product_ID, count(*) as num_times_prod_sold from Order_Tbl a left join prev_day_sale b on a.ORDER_ID=b.ORDER_ID and a.PRODUCT_ID=b.PRODUCT_ID where b.prev_order_day<>'' and order_day in(select max(order_day) fromm Order_tbl);;

--Solution 2
select distinct PRODUCT_ID, count(Product_ID) from Order_Tbl group by Product_ID having count(distinct ORDER_DAY)>1;

--(b)
with prev_day_sale as(
select ORDER_ID, PRODUCT_ID, lag(ORDER_DAY,1,'') over(Partition by ORDER_ID,PRODUCT_ID order by ORDER_DAY) as prev_order_day from Order_Tbl)
)

select a.Product_ID, count(*) as num_times_prod_sold from Order_Tbl a left join prev_day_sale b on a.ORDER_ID=b.ORDER_ID and a.PRODUCT_ID=b.PRODUCT_ID where b.prev_order_day='' and order_day in(select max(order_day) fromm Order_tbl);

--Solution 2
select distinct product_id, count(product_id) from Order_Tbl where order_day= '2015-12-24' and product_id not in (select distinct product_id from Order_Tbl where order_day='2015-12-23')

--Solution 3
with a as (select distinct product_id, count(product_id) as count from Order_Tbl where order_day= '2015-12-24'),
b as (select distinct product_id from Order_Tbl where order_day= '2015-12-23')
select a.product_id,a.count from a left join b on a.product_id=b.product_id where b.product_id is null;

--(c) get highest sold product on both days
with a as (
select product_id,order_day, sum(quantity*price) as total_sell_price from Order_Tbl group by product_id,order_day
)
b as(
select order_day, max(total_sell_price) as max_quantity_sold from a group by order_day
)
select a.order_day,a.product_id,b.max_quantity_sold from a inner join b on a.order_day=b.order_day and a.total_sell_price=b.max_quantity_sold;

--(d) Create two columns showing cost from two days adjacent to each other

with a as (
select product_id,extract(day from order_day) as day, sum(quantity*price) as total_sell_price from Order_Tbl group by product_id,order_day
)

(select product_id from a)
PIVOT(SUM(total_sell_price) as Total_Sales FOR day IN ('24','23'));

--------------------------------------------------------------------------------------
INPUT :-  Order_Tbl has four columns namely ORDER_ID, PRODUCT_ID, QUANTITY and PRICE
Problem Statements :-
Write a SQL which will explode the above data into single unit level records as shown below

I/P Table ->
elem   Seq
A      3
B      2

o/p -> Repeat elements for the number of times it is mentioned in seq
A 1
A 1
A 1
B 1
B 1

with cte as (
select order_id,product_id,1 as Qunatity, 1 as cnt from Order_Tbl

UNION ALL

select A.Order_ID,A.Product_ID,B.Quantity, B.cnt+1 from Order_Tbl inner join cte on A.Product_ID=B.Product_ID where B.cnt+1 <= A.Quantity
)
Select Order_ID, Product_ID, Quantity from cte order by Product_ID, Order_ID;


-----------------------------------------------------------------------------------------
/*
Find employees whose salary is greater than average salary across department.

I/p Table
Row	EmpID	EmpName	Salary	DeptID	
1	1004     Peter   20000  1
2	1003     Andrew  15000  1
3	1002     Antony  40000  2
4	1001     Mark    60000  2


O/P table
Row	EmpID	EmpName	DeptID	
1	1004    Peter     1
2	1001    Mark      2
*/

--SQL
with avg as (
select DeptID, avg(salary) as avg from `Employee` group by DeptID
)

select a.EmpID, a.EmpName, a.DeptID, a.Salary from `Employee` A inner join avg on a.DeptID=avg.DeptID where a.Salary>avg.avg;

-----------------------------------------------------------------------------------------------
/*
Input Table: -
ID team_name
-------------
1	India
2	Australia
3	England
4	New Zealand

Output Table: -

matches
---------------------
India vs Australia
India vs England
India vs New Zealand
Australia vs England
Australia vs New Zealand
England vs New Zealand

*/

select concat(t1.team_name,'vs',t2.team_name) as matches
from
(select team_name,id from team) t1
inner join
(select team_name,id from team) t2
on t1.id<t2.id
order by t1.id

--Solution 2
with a as (select id,team_name from team),
b as (select id,team_name from team)
select concat(a.team_name,'vs',a.team_name) as matches from a inner join b on a.id < b.id order by 1;

--------------------------------------------------------------------------------------------------
/*
Input Table -> Match_Result

Team1     	Team2	       Result
---------------------------------------
India	  	Australia		India
India	  	England			England
SA		  	India			India
Australia 	England			Draw
England	  	SA				SA
Australia 	India			Australia

Output Table ->

Team		Match_Played	Match_Won	Match_Tie	Match_Lost
---------------------------------------------------------------
Australia	3				1			1			1
England		3				1			1			1
India		4				2			0			2
SA			2				1			0			1
*/

/*CREATE TABLE
  `gcp-essentials-saket.COVID.Match_Result` (Team1 STRING,
    Team2 STRING,
    Result STRING);

INSERT INTO
  `gcp-essentials-saket.COVID.Match_Result`
VALUES
  ('India', 'Australia','India'),
  ('India','England','England'),
  ('SA','India','India'),
  ('Australia','England','Draw'),
  ('England','SA','SA'),
  ('Australia','India','Australia');*/
  
With match_won as (select sum(Match_Won) as Match_Won,Team from(
select count(Result) as Match_Won, Result as Team from `gcp-essentials-saket.COVID.Match_Result` where result<>'Draw' group by Team) group by team),
match_lost as (select sum(Match_Lost) as Match_Lost,Team from(
select if(Result<>Team1,count(Team1),count(Team2)) as Match_Lost, if(Result=Team1,Team2,Team1) as Team from `gcp-essentials-saket.COVID.Match_Result` where result<>'Draw' group by Team1, Team2,Result) group by Team),
match_drawn as (
select sum(Match_Tie) as Match_Tie,Team from(
select count(Result) as Match_Tie, team1 as Team from `gcp-essentials-saket.COVID.Match_Result` where result='Draw' group by team1
union all
select count(Result) as Match_Tie, team2 as Team from `gcp-essentials-saket.COVID.Match_Result` where result='Draw' group by team2) group by Team),
match_plays as (
select Team,sum(Match_Played) as Match_Played from(
select count(team1) as Match_Played,team1 as Team from `gcp-essentials-saket.COVID.Match_Result` group by team1
Union ALL
select count(team2) as Match_Played,team2 as Team from `gcp-essentials-saket.COVID.Match_Result` group by team2) group by team)

select distinct match_plays.team, coalesce(match_plays.Match_Played,0) as Match_Played, coalesce(match_won.Match_Won,0) as Match_Won, coalesce(match_drawn.Match_Tie,0) as Match_Tie, coalesce(match_lost.Match_Lost,0) as Match_Lost from match_plays left join match_won on match_plays.team=match_won.team left join match_drawn on match_plays.team=match_drawn.team left join match_lost on match_plays.team=match_lost.team order by match_plays.team;

----------------------------------------------------------------------------------------
/*
Input :- Transaction_Table has four columns  namely  AccountNumber, TransactionTime, TransactionID and Balance

Problem Statements :- Write SQL to get the most recent / latest balance, and TransactionID for each AccountNumber
*/

--Solution 1
with latest_transc_time as (
select rank() (partition by AccountNumber order by TransactionTime desc) as acct_rank,AccountNumber from Transaction)

select a.AccountNumber, a.TransactionTime, a.TransactionId, a.Balance from Transaction a inner join latest_transc_time b on a.AccountNumber = b.AccountNumber where b.rank=1;

--Solution 2
select a.AccountNumber, a.TransactionTime, a.TransactionId, a.Balance from Transaction a where a.TransactionTime in(select max(TransactionTime) from Transaction on a.AccountNumber=Transaction.AccountNumber);

--Solution 3
with latest_transc_time as (
select distinct AccountNumber,last_value(TransactionTime) (partition by AccountNumber order by TransactionTime desc) as max_transc_time from Transaction)
select a.AccountNumber, a.TransactionTime, a.TransactionId, a.Balance from Transaction a inner join latest_transc_time b on a.AccountNumber = b.AccountNumber where a.TransactionTime=b.max_transc_time;

------------------------------------------------------------------------------------------
/*
Input :- SalesTable has four columns  namely  ID, Product , SalesYear and QuantitySold

Problem Statements :- Write SQL to get the total Sales in year 1998,1999 and 2000 for all the products as shown below.
*/

--Solution 1
select 'TotalSales' as TotalSales, case when year=1998 then sum(quantitysold) end as 1998, case when year=1999 then sum(quantitysold) end as 1999,case when year=2000 then sum(quantitysold) end as 2000 from SalesTable group by year;

--Solution 2
SELECT * FROM
  (SELECT 'TotalSales' as TotalSales FROM SalesTable)
  PIVOT(SUM(quantitysold) FOR quarter IN (1998,1999,2000));
  
-------------------------------------------------------------------------------------------
/*
Calculate Running Total
*/

select ProdName, ProductCode, Quantity, InventoryDate, sum(Quantity) over(partition by ProdName,ProdCde order by InventoryDate range between unbounded preceding and current row) as Running_Total from Inventory;

-------------------------------------------------------------------------------------------
/*
Print alphabet A-Z
*/

DECLARE @Start int  
set @Start=65  
while(@Start<=90)  
begin  
print char(@Start)  
set @Start=@Start+1  
end 

--Solution 2
with Alphabet as(
select char(ASCII('A')) letter,
union all
select char(ASCII(letter) + 1) from Alphabet where letter<>'Z')
select * from Alphabet;

-----------------------------------------------------------------------------------------------
/*
Print Fibonacci series
*/

with fib as (
select 0 as first,1 as second, 1 as step
union all
select second,first+second,step+1 from fib where step<10)

select * from fib;

-----------------------------------------------------------------------------------------------
Calculate Net Balance
/*
create table Account (TransDate timestamp, TranID string,TranType string, Amount int);

Insert into Account values('2020-05-12 05:29:44', 'A10001','Credit', 50000),
('2020-05-13 10:30:20', 'B10001','Debit', 10000),
('2020-05-13 11:27:50', 'B10002','Credit', 20000),
('2020-05-14 08:35:30', 'C10001','Debit', 5000),
('2020-05-14 09:43:51', 'C10002','Debit', 5000),
('2020-05-15 05:51:11', 'D10001','Credit', 30000);
*/



with net_bal as (
select TranID,TransDate, case when TranType='Credit' then Amount else Amount*(-1) end as net_amt from Account
)
select a.TransDate,a.TranID,TranType,Amount,sum(net_amt) over(Order by a.TransDate) as Net_Balance from Account a inner join net_bal b 
on a.TranID=b.TranID and a.TransDate=b.TransDate;

--------------------------------------------------------------------------------------------------
/*
Write SQL to turn the columns English, Maths and Science into rows. It should display Marks for each student for each subjects.
Transpose columns into rows
*/

--Solution 1
select * from StudentInfo
unpivot(Marks for Subjects in('English','Maths','Science'))
order by StudentName;

--Solution 2
select StudentName, 'English' as Subjects, English as Marks from StudentInfo
Union All
select StudentName, 'Maths' as Subjects, Maths as Marks from StudentInfo
Union All
select StudentName, 'Science' as Subjects, Science as Marks from StudentInfo
order by StudentName;
--------------------------------------------------------------------------------------------------
/*
Input :- Trade_Tbl has five columns  namely  Trade_ID, Trade_Timestamp, Trade_Stock, Quantity and Price

Problem Statements :- Write SQL to find all couples of trade for same stock that happened in the range of 10 seconds and having price difference by more than 10 %. Output result should also list the percentage of price difference between the 2 trade.
*/

with Trade_CTE as (
select Trade_id, Trade_Timestamp, Price from Trade_Tbl
)

select a.trade_id, b.trade_id, floor(abs(b.price-a.price)) as price_diff from Trade_CTE a inner join Trade_CTE b on a.Trade_id < b.trade_id where DATEDIFF(SECOND, a.Trade_Timestamp, b.Trade_Timestamp) <= 10 and abs(((b.price-a.price)/a.price)*100) >= 10
order by 1;

--------------------------------------------------------------------------------------------------
/*
Input Table ->

Balances  Dates
26000     2020-01-01
26000     2020-01-02
26000     2020-01-03
30000     2020-01-04
30000     2020-01-05
26000     2020-01-06
26000     2020-01-07
32000     2020-01-08
31000     2020-01-09

Output Table ->
Balance		Start_Date		End_Date
26000		2020-01-01		2020-01-03
30000		2020-01-04		2020-01-05
26000		2020-01-06		2020-01-07
32000		2020-01-08		2020-01-08
31000		2020-01-09		2020-01-09
*/

with table as (
select Dates, Balances, case when lag(Balance) over(order by Dates) = Balance then 0 else 1 as prev_flag from Balance),
sequence as(
select Balances, Dates, sum(prev_flag) over(order by dates) as seq from Balance
)
select Balances, min(dates) as Start_Date, max(dates) as End_Date from sequence group by seq,Balances;

---------------------------------------------------------------------------------------------------------
/*
Input :- There are two table. First table name is Sales_Table. Second Table name is ExchangeRate_Table. As and when exchange rate changes, a new row is inserted in the ExchangeRate table with a new effective start date.

Problem Statements :- Write SQL to get Total  sales amount in USD for each sales date

Input Table ->
create or replace table Exchange_Rate (Source_Currency STRING, Target_Currency STRING, Exchange_Rate decimal(10,3), 
Effective_Start_Date date);

Insert into Exchange_Rate values('INR','USD', 0.014,'2019-12-31'),('INR','USD', 0.015,'2020-01-02'), ('GBP','USD', 1.32, '2019-12-20'),
('GBP','USD', 1.3, '2020-01-01' ),('GBP','USD', 1.35, '2020-01-16');
*/

with eff_date as(
select sales_date,max(effective_start_date) as effective_start_date,Source_Currency from sales a left join Exchange_Rate b 
on a.currency=b.source_currency and b.effective_start_date<=a.sales_date group by sales_date,Source_Currency)
select a.sales_date,round(sum(a.Sales_Amount*b.Exchange_Rate)) as Total_Sales_Amount_in_USD from sales a inner join Exchange_Rate b 
on a.currency=b.source_currency
inner join eff_date c on a.Sales_Date=c.Sales_Date and b.effective_start_date=c.effective_start_date
and a.Currency=c.Source_Currency group by a.sales_date;

---------------------------------------------------------------------------------------------------------
--
/*
create or replace table travel (Sources string, Destination string, Distance int);
insert into travel values('Delhi','Pune',1400), ('Pune','Delhi',1400),('Bangalore','Chennai',350),('Mumbai','Ahmedabad',500),
('Chennai','Bangalore',350),('Patna','Ranchi',300);

Input: -

Delhi Pune 1400
Pune Delhi 1400
Bangalore Chennai 350
Mumbai Ahmedabad 500
Chennai Bangalore 350
Patna Ranchi 300

Output
Bangalore Chennai 350
Delhi Pune 1400
Mumbai Ahmedabad 500
Patna Ranchi 300

*/

select distinct least(Sources,Destination) as sources,greatest(Sources,Destination) as Destination,distance from travel;

select * from travel where sources<destination
union
select * from travel a where not exists (select 1 from travel b where a.sources=b.destination) order by sources;

---------------------------------------------------------------------------------------------------------
/*
Find the missing id's
Input ->
id
1
4
7
10
14
18
20

Output ->
id
----
2
3
5
6
8
9
11
12
13
15
16
17
19

*/
--Need to recursive cte to store number from 1 to 20 and then find out missing id's

with cte_a as(
select max(id) as max from table
),
recursive cte_b as (
select min(id) as id from table
union all
select b.id + 1 from cte_a a inner join cte_b b on a.id=b.id where b.id<a.max)

select b.id from cte_b b left join table tab on b.id=tab.id where tab.id is null;

------------------------------------------------------------------------------------------------------
/*
Write a SQL to display the Source_Phone_Nbr and a flag where the flag needs to be set to ‘Y’ if first called number and last called number are the same and ‘N’ if first called number and last called number are different

Input :- Phone_Log Table has three columns namely Source_Phone_Nbr,  Destination_Phone_Nbr and Call_Start_DateTime. This table records all phone numbers that we dial in a given day.
*/

--Solution 1 (Subquery)
selet source_phone_number, case when firstcall=rankcall then 'Y' else 'N' end as Is_Match from
(
select Source_Phone_Nbr,max(case when firstrank=1 then Destination_Phone_Nbr end) as firstcall,
max(case when secondrank=1 then Destination_Phone_Nbr end) as secondcall from(
select source_phone_number, destination_phone_number, call_start_datetime,
row_number() over(partition by source_phone_number order by call_start_datetime) as firstrank,
row_number() over(partition by source_phone_number order by call_start_datetime desc) as sceondrank
 from phone_log));
 
--Solution 2 (First_Value and Last_Value Analytical Window functions)
with call_details as
(select distinct source_phone_number,first_value(destination_phone_number) over(partition by source_phone_number order by call_start_datetime) as first_call, last_value(destination_phone_number) over(partition by source_phone_number order by call_start_datetime rows between unbounded preceding and unbounded following) as last_call from phone_log)

select source_phone_number,if(first_call=last_call,'Y','N') as Is_Match from call_details;

-------------------------------------------------------------------------------------------------------


with cte_a as(
select max(End_Range) as max from table
),
recursive cte_b as (
select min(Start_Range) as id from table
union all
select id + 1 from cte_b where id<(SELECT max from cte_a))
select id from cte_b,table where id>='Start_Range' and id<='End_Range';

-------------------------------------------------------------------------------------------------------

with a as(
select X,Y,Z,count() over(partition by X,Y) as record_count from SampleTable
)
select X,Y,Z from a where record_count>1;