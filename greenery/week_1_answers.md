Question 1
How many users do we have?

30

Query:

SELECT
COUNT(DISTINCT user_id) AS total_users

FROM dbt_gabriele_k.stg_users

-----------------------------------------------------------------
Question 2
On average, how many orders do we receive per hour?

7.68

Query:

WITH orders_per_hour AS (

  SELECT
  COUNT(DISTINCT order_id) AS total_orders,
  DATE_PART('day', MAX(created_at) - MIN(created_at))*24 + DATE_PART('hour', MAX(created_at) - MIN(created_at) ) AS total_hours
  
  FROM dbt_gabriele_k.stg_orders
)

SELECT
total_orders/total_hours AS orders_per_hour

FROM orders_per_hour

-----------------------------------------------------------------
Question 3
On average, how long does an order take from being placed to being delivered?

3.89 days

Query:

SELECT
AVG(DATE_PART('day', delivered_at - created_at)) AS average_delivery_days
  
FROM dbt_gabriele_k.stg_orders

-----------------------------------------------------------------
Question 4
How many users have only made
- One purchase? 
124

- Two purchases? 
99

- Three+ purchases?
71

Query:

WITH purchases_per_user AS (

  SELECT
  user_id
  , COUNT(DISTINCT order_id) AS orders
    
  FROM dbt_gabriele_k.stg_orders
  
  GROUP BY 1
  
)

SELECT
COUNT(DISTINCT CASE WHEN orders > 0 THEN user_id END) AS users_with_1_order
, COUNT(DISTINCT CASE WHEN orders > 1 THEN user_id END) AS users_with_2_orders
, COUNT(DISTINCT CASE WHEN orders > 2 THEN user_id END) AS users_with_3_or_more_orders

FROM purchases_per_user

-----------------------------------------------------------------
Question 5
On average, how many unique sessions do we have per hour?

16.61

Query:

WITH sessions_per_hour AS (

  SELECT
  DATE_TRUNC('hour', created_at) AS date_hour
  , COUNT(DISTINCT session_id) AS total_sessions
  
  FROM dbt_gabriele_k.stg_events
  
  GROUP BY 1
  
)

, total_hours AS (

  SELECT
  DATE_PART('day', MAX(created_at) - MIN(created_at))*24 + DATE_PART('hour', MAX(created_at) - MIN(created_at) ) AS total_hours
  
  FROM dbt_gabriele_k.stg_events
  
)

SELECT 
SUM(total_sessions)/AVG(total_hours) AS sessions_per_hour

FROM 
sessions_per_hour

CROSS JOIN 
total_hours









