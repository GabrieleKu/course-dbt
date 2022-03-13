WITH order_items AS (

    SELECT
        order_id
        , product_id
        , quantity

    FROM {{ source('postgres','order_items')}}

)

SELECT * FROM order_items