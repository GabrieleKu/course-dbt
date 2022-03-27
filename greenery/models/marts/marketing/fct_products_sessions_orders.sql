--{% set event_types = get_event_types() %}
{% set event_types = dbt_utils.get_column_values(table=ref('stg_events'), column='event_type') %}

WITH orders_products AS (
    SELECT * FROM {{ ref('fct_orders_products') }}
)

, events AS (
    SELECT * FROM {{ ref('fct_events') }}
)

, products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

-- gets product id and name

, product_names AS (
    SELECT
        product_id,
        product_name

    FROM products
)

-- gets distinct session and order combinations for those sessions with orders

, sessions_orders AS (

    SELECT
        session_id,
        order_id

    FROM events

    WHERE order_id IS NOT NULL

    GROUP BY 1,2
)


-- finds ordered items for each session

, sessions_ordered_products AS (

    SELECT
        sessions_orders.session_id,
        orders_products.product_id,
        SUM(orders_products.quantity) AS product_ordered_quantity

    FROM orders_products

    JOIN sessions_orders
        ON orders_products.order_id = sessions_orders.order_id
    
    GROUP BY 1,2

)

-- finds visited product pages for each session with product page views

, sessions_product_visits AS (

    SELECT
        session_id,
        product_id,
        {% for event_type in event_types %}
        SUM(CASE WHEN event_type = '{{event_type}}' THEN 1 ELSE 0 END) AS total_{{event_type}}_events
        {% if not loop.last %},{% endif %}  
        {% endfor %}

    FROM events

    WHERE product_id IS NOT NULL

    GROUP BY 1,2

)

, products_sessions_orders AS (

    SELECT
        sessions_product_visits.product_id,
        product_names.product_name,
        COUNT(DISTINCT sessions_product_visits.session_id) AS product_sessions,
        SUM(sessions_product_visits.total_page_view_events) As product_page_views,
        SUM(sessions_product_visits.total_add_to_cart_events) As product_times_added_to_cart,
        COUNT(DISTINCT sessions_ordered_products.session_id) AS product_orders,
        SUM(sessions_ordered_products.product_ordered_quantity) AS product_ordered_quantity

    FROM sessions_product_visits

    JOIN product_names
        ON sessions_product_visits.product_id = product_names.product_id

    LEFT JOIN sessions_ordered_products
        ON sessions_product_visits.session_id = sessions_ordered_products.session_id
        AND sessions_product_visits.product_id = sessions_ordered_products.product_id

    GROUP BY 1,2

)

SELECT
*,
ROUND(CAST(product_orders AS NUMERIC)/CAST(product_sessions AS NUMERIC),4) AS product_conversion_rate

FROM products_sessions_orders
