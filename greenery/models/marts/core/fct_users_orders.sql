WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
)

, users AS (
    SELECT * FROM {{ ref('int_users') }}
)

SELECT
users.user_id,
users.user_created_at_utc,
users.address_id,
users.user_state,
users.user_country,
MIN(orders.created_at_utc) AS first_order_date,
MAX(orders.created_at_utc) AS last_order_date,
COUNT(DISTINCT orders.order_id) AS total_orders,
COUNT(DISTINCT CASE WHEN orders.status = 'delivered' THEN orders.order_id END) AS total_delivered_orders,
COUNT(DISTINCT CASE WHEN orders.status = 'preparing' THEN orders.order_id END) AS total_prepared_orders,
COUNT(DISTINCT CASE WHEN orders.status = 'shipped' THEN orders.order_id END) AS total_shipped_orders,
COUNT(DISTINCT CASE WHEN orders.is_order_late = TRUE THEN orders.order_id END) AS total_late_orders,
SUM(CASE WHEN orders.promo_id IS NOT NULL THEN 1 ELSE 0 END) AS total_promotional_orders,
SUM(orders.order_cost) AS total_order_cost,
SUM(orders.shipping_cost) AS total_shipping_cost,
SUM(orders.order_total) AS total_lifetime_spend,
SUM(orders.total_order_items) AS total_items,
ROUND(SUM(orders.order_total)/COUNT(DISTINCT orders.order_id),2) AS average_order_value,
ROUND(SUM(orders.order_total)/SUM(orders.total_order_items),2) AS average_item_value

FROM
orders

LEFT JOIN users
    ON orders.user_id = users.user_id

GROUP BY 1,2,3,4,5