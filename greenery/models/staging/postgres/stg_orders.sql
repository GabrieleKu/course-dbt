WITH orders AS (

    SELECT
        order_id
        , user_id
        , promo_id
        , address_id
        , created_at AS created_at_utc
        , ROUND(CAST(order_cost AS NUMERIC),2) AS order_cost
        , ROUND(CAST(shipping_cost AS NUMERIC),2) AS shipping_cost
        , ROUND(CAST(order_total AS NUMERIC),2) AS order_total
        , tracking_id
        , shipping_service
        , estimated_delivery_at AS estimated_delivery_at_utc
        , delivered_at AS delivered_at_utc
        , status

    FROM {{ source('postgres','orders')}}

)

SELECT * FROM orders