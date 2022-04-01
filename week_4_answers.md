------------------------------------------------------
Part 1
------------------------------------------------------

I created a snapshot sql file, then ran dbt snapshot, updated orders table and ran dbt snapshot again. I can see two rows per each changed order now.

------------------------------------------------------
Part 2
------------------------------------------------------

CONVERSION FUNNEL

I already had a model fct_sessions which contains all information on session level including event types. I added new columns to flag 1/0 for each event instead as I previously had the total count of events.
Query to get conversion funnel can be found below. This can be adjusted to use the session_created_at_utc field to graph conversion funnel over time.
I did not create a dbt model for the funnel itself, because I believe there is a need for flexiblity in reporting the funnel and so the logic could live in a BI tool rather than dbt model. 
However, dbt model could be created using the logic below.

Session conversion funnel by day:

|  session_date  | total_page_view_sessions | total_add_to_cart_sessions | total_checkout_sessions | percentage_total_sessions | percentage_add_to_cart_sessions | percentage_checkout_sessions | total_add_to_cart_drop_off | total_checkout_drop_off |
|--------------------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
2021-02-09T00:00:00Z |  2  |  2  |  2  | 1.0000 | 1.0000 | 1.0000 |  0  |  0
2021-02-10T00:00:00Z | 175 | 175 | 175 | 1.0000 | 1.0000 | 1.0000 |  0  |  0
2021-02-11T00:00:00Z | 401 | 290 | 184 | 1.0000 | 0.7232 | 0.4589 | 111 | 106

Query:

WITH sessions_funnel AS (
  
  SELECT
  date_trunc('day', session_created_at_utc) AS session_date,
  COUNT(DISTINCT session_id) AS total_page_view_sessions,
  COUNT(DISTINCT CASE WHEN had_add_to_cart= 1 THEN session_id END) AS total_add_to_cart_sessions,
  COUNT(DISTINCT CASE WHEN had_checkout = 1 THEN session_id END) AS total_checkout_sessions
  
  FROM dbt_gabriele_k.fct_sessions
  
  GROUP BY 1

)

SELECT
sf.*,
ROUND(CAST(1 AS NUMERIC),4) AS percentage_total_sessions,
ROUND(CAST(total_add_to_cart_sessions AS NUMERIC)/CAST(total_page_view_sessions AS NUMERIC),4) AS percentage_add_to_cart_sessions,
ROUND(CAST(total_checkout_sessions AS NUMERIC)/CAST(total_page_view_sessions AS NUMERIC),4) AS percentage_checkout_sessions,
total_page_view_sessions-total_add_to_cart_sessions AS total_add_to_cart_drop_off,
total_add_to_cart_sessions-total_checkout_sessions AS total_checkout_drop_off

FROM sessions_funnel AS sf

ORDER BY session_date ASC

---

EXPOSURES

I created an exposure file in greenery/models/exposures.yml

------------------------------------------------------
Part 3
------------------------------------------------------

3A

My organization has recently got dbt and we are currently in the process of migrating all of our existing data modelling to dbt. This course was extremely helpful to get familiar with dbt basics, best practises and most importantly available functionalities.

I strongly believe that getting to know fundamentals on Macros/JINJA, Snapshots, Exposures, Packages and other things we have learned, will help us improve efficiency and reliability of our data transformation process. It is really good to get to know these from the beginning, so that we can already consider how we would like to use these functionalities and at which stage we would like to implement it.

3B

Currently our scheduled dbt run has the following steps:

1. dbt freshness
2. dbt run
3. dbt test

Our freshness is currently set up to only warn us if there are any issues, so that it does not block the run process. This set up will allow us to know if there are any data freshness issues as well as if there are are failures in the run command or if there are any failing tests.

Once we have basics set up, we will consider expanding these commands further.

This will be scheduled to run each morning to mirrow ingestion of our data process and our chosen tool is Airflow.

In the near future, I think we will consider storing some of the dbt metadata in BigQuery tables (such as freshness results or test results), so that we can then use it in our BI tool Looker to notify of any process failures.