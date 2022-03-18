WITH events AS (
    SELECT * FROM {{ ref('fct_events') }}
),

sessions AS (

    SELECT
    events.session_id,
    events.user_id,
    events.user_created_at_utc,
    events.user_state,
    events.user_country,
    MIN(events.created_at_utc) AS session_created_at_utc,
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS total_page_views,
    MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS had_add_to_cart,
    MAX(CASE WHEN event_type = 'checkout' THEN 1 ELSE 0 END) AS had_visit_to_checkout_page,
    MAX(CASE WHEN event_type = 'package_shipped' THEN 1 ELSE 0 END) AS had_package_shipped

    FROM events

    GROUP BY 1,2,3,4,5

)

SELECT
session_id,
user_id,
user_created_at_utc,
user_state,
user_country,
session_created_at_utc,
total_page_views,
CASE WHEN had_add_to_cart = 1 THEN TRUE ELSE FALSE END AS had_add_to_cart,
CASE WHEN had_visit_to_checkout_page = 1 THEN TRUE ELSE FALSE END AS had_visit_to_checkout_page,
CASE WHEN had_package_shipped = 1 THEN TRUE ELSE FALSE END AS had_package_shipped

FROM sessions