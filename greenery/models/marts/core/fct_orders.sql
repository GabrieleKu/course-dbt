WITH orders AS (
    SELECT * FROM {{ ref('int_orders') }}
)

, order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
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
orders.discount_percentage,
orders.promotion_status,
orders.state,
orders.country,
orders.is_order_late,
orders.delivery_days,
orders.order_sequence,
SUM(order_items.quantity) AS total_order_items

FROM
orders

LEFT JOIN order_items
    ON orders.order_id = order_items.order_id

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
