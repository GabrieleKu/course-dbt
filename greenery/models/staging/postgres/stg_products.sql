WITH products AS (

    SELECT
        product_id
        , name
        , price
        , inventory

    FROM {{ source('postgres','products')}}

)

SELECT * FROM products