use test;

/*1.calcuate the following fields from the Year	Month (#)	Day  fields ( First Create a Date Field from Year , Month , Day fields)
   A.Year
   B.Monthno
   C.Monthfullname
   D.Quarter(Q1,Q2,Q3,Q4)
   E. YearMonth ( YYYY-MMM)
   F. Weekdayno
   G.Weekdayname
   H.FinancialMOnth
   I. Financial Quarter */
   
alter table maindata add column Full_Date date;
update maindata set Full_Date = str_to_date(concat(year_num,'-',month_num,'-',day_num),'%Y-%m-%d');

alter table maindata 
add column Month_Name varchar(15), 
add column Quarter_num varchar(15),
add column Weekday_num int, 
add column Weekday_name varchar(15),
add column Weekend_Weekday varchar(15),  
add column Financial_Month_num int, 
add column Financial_Quarter varchar(15);
UPDATE maindata 
SET 
    Month_Name = MONTHNAME(full_date),
    Quarter_num = CASE
        WHEN Month IN (1 , 2, 3) THEN 'Q1'
        WHEN Month IN (4 , 5, 6) THEN 'Q2'
        WHEN Month IN (7 , 8, 9) THEN 'Q3'
        WHEN Month IN (10 , 11, 12) THEN 'Q4'
    END,
    Weekday_num = WEEKDAY(full_date) + 1,
    Weekday_name = DAYNAME(full_date),
    Financial_Month_num = CASE
        WHEN Month >= 4 THEN month_num - 3
        WHEN Month < 4 THEN month_num + 9
    END,
    Weekend_Weekday = CASE
        WHEN weekday_num IN (1 , 2, 3, 4, 5) THEN 'Weekday'
        ELSE 'Weekend'
    END,
    Financial_Quarter = CASE
        WHEN Month IN (1 , 2, 3) THEN 'Q4'
        WHEN Month IN (4 , 5, 6) THEN 'Q1'
        WHEN Month IN (7 , 8, 9) THEN 'Q2'
        WHEN Month IN (10 , 11, 12) THEN 'Q3'
    END;
    
 #ANSWER_1
SELECT 
    Full_Date,
    Year,
    Month,
    Month_name,
    Quarter_num,
    CONCAT(year, '-', month) AS YearMonth,
    Weekday_num,
    Weekday_name,
    Weekend_Weekday,
    Financial_Month_num,
    Financial_Quarter
FROM
    maindata;
    
    
    
#2. Find the load Factor percentage on a yearly , Quarterly , Monthly basis ( Transported passengers / Available seats)
alter table maindata add column Load_Factor decimal(5,2);
update maindata set Load_Factor = case when available_seats = 0 then 0 else round(transported_passengers*100/available_seats,2) end;

select Year, round(avg(Load_Factor),2) as Load_Factor_Percentage from maindata group by year; 
select Month, Month_name, round(avg(Load_Factor),2) as Load_Factor_Percentage from maindata group by Month, Month_name order by month desc;
select Quarter_num, round(avg(Load_Factor),2) as Load_Factor_Percentage from maindata group by Quarter_num order by Quarter_num;



#3. Find the load Factor percentage on a Carrier Name basis ( Transported passengers / Available seats)
SELECT 
    Carrier_Name,
    round(avg(Load_Factor),2) AS Load_Factor_Percentage
FROM
    maindata
GROUP BY carrier_name
ORDER BY round(avg(Load_Factor),2) DESC;


        
#4. Identify Top 10 Carrier Names based passengers preference 
SELECT 
    Carrier_Name,
    round(avg(Load_Factor),2) AS Load_Factor_Percentage
FROM
    maindata
GROUP BY carrier_name
ORDER BY round(avg(Load_Factor),2) DESC
LIMIT 10;


#5. Display top Routes ( from-to City) based on Number of Flights 
SELECT 
    From_To_City, COUNT(Airline_id) AS Num_of_Flights
FROM
    maindata
GROUP BY From_To_City
ORDER BY COUNT(Airline_ID) DESC
LIMIT 10;


#6. Identify the how much load factor is occupied on Weekend vs Weekdays.
SELECT 
    Weekend_Weekday,
    round(avg(Load_Factor),2) AS Load_Factor_Percentage
FROM
    maindata
GROUP BY Weekend_weekday;


#7. Identify number of flights based on Distance group
SELECT 
    Distance_Group_ID,
    CONCAT(MIN(distance),'-',MAX(distance),' miles') AS Distance_Group,
    COUNT(airline_id) AS Num_of_Flights
FROM
    maindata
GROUP BY Distance_group_id;
