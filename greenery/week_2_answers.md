------------------------------------------------------
Part 1
------------------------------------------------------

-> What is our user repeat rate?

79.84%

Query:

WITH users AS(

  SELECT
  COUNT(DISTINCT CASE WHEN total_orders>1 THEN user_id END) AS repeat_users,
  COUNT(DISTINCT user_id) AS total_users
  
  FROM dbt_gabriele_k.fct_users_orders

)

SELECT
SUM(repeat_users)/SUM(total_users) AS repeat_user_rate

FROM users

------------------------------------------------------
-> What are good indicators of a user who will likely purchase again? 

Some factors that we could look at:
- Total lifetime spend
- Total lifetime orders
- Total lifetime items
- Past user experience (was the order late, how long did delivery take)
- Did user receive a discount
- How long user spent on our website

-> What about indicators of users who are likely NOT to purchase again?

Some factors that we could look at:
- Was the last order late
- Was the last order delivered

-> If you had more data, what features would you want to look into to answer this question?
- Customer service data would be interesting to have to see if users had any complaints about the items

------------------------------------------------------
-> Explain the marts models you added. Why did you organize the models in the way you did?

Intermediate models folder:

I kept intermediate models in their own folder, as those can be used accross different departments.

- int_orders -> contains orders table with promotional and address data added. Some small manipulations on values were also applied as well as some additional columns were calculated.

- int_users -> contains users table with some address data added.

Product folder:

- fct_events -> contains all events information with additional user and product data. This can be used for further event analysis on how users are behaving on the website (order of events, what pages they viewed, etc.)

- fct_sessions -> contains session level data with additional user data, including information for total page views, flags for different events within the session, etc. This is useful when overall session data is of interest rather than individual events.

Marketing folder:

- fct_orders_products -> contains product level data with additional information about orders and users. Can be used for promotional analysis as well as general sales data (how much users spent, which products are the most popular, which states are the most popular for which products, etc.)

Core folder:

- fct_orders -> contains order level data with some additional information about total items. This can be used to analyse overall sales data, delivery data and promotions.

- fct_users_orders -> contains user level data on orders and total items. This can be used to analyse user behaviour patterns (lifetime spent, order frequency, average order value) and how that relates to various factors.

Additional notes: I noticed that there are some data quality issues with amounts stored in the database. order_cost and shipping_cost columns don't always add up to the order_total. I was also trying to find the correct value by adding up value of individual items in the order after discount, but there were some cases when those didn't equal neither of the totals. Therefore, further investigation with table owners would be needed to determine which values are correct.

------------------------------------------------------
Part 2
------------------------------------------------------

- I added some tests to staging models, checking for unique, not null and positive values.

- I did not find any issues with the data from these tests.

- To ensure the quality of data regularly, I would run these tests every time after models are run (for example, each morning). In case of failures, I would check if there are issues with source data or SQL logic. In case of source data errors, I would let the table owners know and fix the data quality issues in staging model as short-term solution.


