WITH products AS (

    SELECT
        product_id
        , name AS product_name
        , ROUND(CAST(price AS NUMERIC),2) AS product_price
        , inventory

    FROM {{ source('postgres','products')}}

)

SELECT * FROM products