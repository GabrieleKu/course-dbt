WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

, promos AS (
    SELECT * FROM {{ ref('stg_promos') }}
)

, addresses AS (
    SELECT * FROM {{ ref('stg_addresses') }}
)

SELECT
orders.order_id,
orders.user_id,
orders.promo_id,
orders.address_id,
orders.created_at_utc,
orders.order_cost,
orders.shipping_cost,
orders.order_total,
orders.tracking_id,
orders.shipping_service,
orders.estimated_delivery_at_utc,
orders.delivered_at_utc,
orders.status,
ROUND(COALESCE(promos.discount_percentage,0),2) AS discount_percentage,
promos.status AS promotion_status,
addresses.state,
addresses.country,
CASE WHEN estimated_delivery_at_utc < delivered_at_utc THEN TRUE ELSE FALSE END AS is_order_late,
DATE_PART('day', delivered_at_utc - created_at_utc) AS delivery_days,
ROW_NUMBER() OVER(PARTITION BY orders.user_id ORDER BY orders.created_at_utc ASC) AS order_sequence


FROM orders

LEFT JOIN promos
    ON orders.promo_id = promos.promo_id

LEFT JOIN addresses
    ON orders.address_id = addresses.address_id
