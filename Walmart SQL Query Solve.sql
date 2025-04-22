# Solve The Query
Use Walmart;

# Modify the Table Name 
Rename Table Sales To Walmart ;
select date ,(date,'dd/mm/yy') as formated_date from walmart;

select * from Walmart ;

select count(*) from walmart;

select payment_method, count(*) from walmart 
group by payment_method;

select count(distinct branch) from walmart;

select max(quantity) from walmart;

-- Business problem 
-- Q1. Find different payment_method and number of transactions ,number of qty sold

SELECT 
    payment_method,
    COUNT(*) AS no_payments,
    SUM(quantity) AS no_qty_sold
FROM
    walmart
GROUP BY payment_method;

-- Q2. Which category received the highest average rating in each branch?
SELECT *
FROM (
    SELECT 
        Branch, 
        Category, 
        AVG(Rating) AS Highest_Rating, 
        RANK() OVER (PARTITION BY Branch ORDER BY AVG(Rating) DESC) AS Ranks
    FROM Walmart
    GROUP BY Branch, Category
) AS RankedCategories
WHERE Ranks = 1;

-- Q3: Identify the busiest day of the week for each branch based on transaction volume?

WITH daily_counts AS (
  SELECT 
    branch,
    DATE_FORMAT(STR_TO_DATE(`date`, '%d/%m/%y'), '%W') AS day_name,
    COUNT(*) AS transaction_count
  FROM walmart
  GROUP BY branch, day_name
),
ranked_days AS (
  SELECT *,
         RANK() OVER (PARTITION BY branch ORDER BY transaction_count DESC) AS rnk
  FROM daily_counts
)
SELECT branch, day_name, transaction_count
FROM ranked_days
WHERE rnk = 1;

-- Q4  Calculate Total Quantity Sold by Payment Method

SELECT 
    payment_method, SUM(quantity) AS no_qty_sold
FROM
    walmart
GROUP BY payment_method;

-- Q5: Determine the average, minimum, and maximum ratings for each category in each city?

select category , city
,round(avg(rating),2) as Avg_Rating , min(rating) as Min_Rating , max(Rating) As Max_Rating
from walmart
group by category , city;

-- Q6:- Calculate the total profit for each category, ranked from highest to lowest?

select category , round(sum(total),2) As Total_Revenue ,round(sum(total * profit_margin),2) as Profit from walmart
group by category;

-- Q7:- Determine the Most Common Payment Method per Branch?
with cte as(
select Branch , payment_method , count(*) as Total_Trans ,
rank() over(partition by branch order by count(*) desc) as rankk
from walmart 
group by branch , payment_method)
select * from cte where rankk = 1;

-- Q8:- Categorize sales into 3 group (Morning, Afternoon, Evening) 
-- Find out which of the shift and number of invoice 

SELECT 
  branch,
  CASE 
    WHEN HOUR(TIME(`time`)) < 12 THEN 'Morning'
    WHEN HOUR(TIME(`time`)) BETWEEN 12 AND 17 THEN 'Afternoon'
    ELSE 'Evening'
  END AS time_of_day,
  COUNT(*) AS transaction_count
FROM walmart
GROUP BY branch, time_of_day
order by branch;

-- . Identify Branches with Highest Revenue Decline Year-Over-Year
--  ● Question: Which branches experienced the largest decrease in revenue compared to
--  the previous year?

WITH revenue_2022 AS 
( 
  SELECT branch, SUM(total) AS revenue
  FROM walmart
  WHERE DATE_FORMAT(STR_TO_DATE(`date`, '%d/%m/%y'), '%Y') = '2022'
  GROUP BY branch
),
revenue_2023 AS
(
  SELECT branch, SUM(total) AS revenue
  FROM walmart
  WHERE DATE_FORMAT(STR_TO_DATE(`date`, '%d/%m/%y'), '%Y') = '2023'
  GROUP BY branch
)
SELECT 
  ls.branch, 
  ls.revenue AS last_year_revenue,
  cs.revenue AS current_year_revenue,
  ROUND(((ls.revenue - cs.revenue) / ls.revenue) * 100, 2) AS rev_dec_ratio
FROM revenue_2022 AS ls
JOIN revenue_2023 AS cs ON ls.branch = cs.branch
WHERE ls.revenue > cs.revenue
ORDER BY rev_dec_ratio DESC
LIMIT 15;













