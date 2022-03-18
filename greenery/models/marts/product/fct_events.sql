WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
)

, users AS (
    SELECT * FROM {{ ref('int_users') }}
)

, products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
events.event_id,
events.session_id,
events.user_id,
events.page_url,
events.created_at_utc,
events.event_type,
events.order_id,
events.product_id,
users.user_created_at_utc,
users.user_state,
users.user_country,
products.product_name,
products.product_price,
ROW_NUMBER() OVER(PARTITION BY events.session_id ORDER BY events.created_at_utc ASC) AS event_sequence

FROM events

LEFT JOIN users
    ON events.user_id = users.user_id

LEFT JOIN products
    ON events.product_id = products.product_id