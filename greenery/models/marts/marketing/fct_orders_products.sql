-- this model contains product level data and includes additional order information

WITH orders AS (
    SELECT * FROM {{ ref('int_orders') }}
)

, order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
)

, products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
orders.order_id,
orders.user_id,
orders.promo_id,
orders.address_id,
orders.created_at_utc,
orders.tracking_id,
orders.shipping_service,
orders.estimated_delivery_at_utc,
orders.delivered_at_utc,
orders.status,
orders.discount_percentage,
orders.promotion_status,
orders.state,
orders.country,
orders.is_order_late,
orders.delivery_days,
orders.order_sequence,
order_items.product_id,
order_items.quantity,
products.product_name,
products.product_price AS product_full_price,
ROUND(CAST(products.product_price*(1-orders.discount_percentage) AS NUMERIC),2) AS product_actual_price,
ROUND(CAST(products.product_price*(1-orders.discount_percentage) AS NUMERIC)*order_items.quantity,2) AS total_product_spend

FROM orders

LEFT JOIN order_items
    ON orders.order_id = order_items.order_id

LEFT JOIN products
    ON order_items.product_id = products.product_id