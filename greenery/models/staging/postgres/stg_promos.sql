WITH promos AS (

    SELECT
        promo_id
        , ROUND(CAST(discount AS NUMERIC)/100,2) AS discount_percentage
        , status

    FROM {{ source('postgres','promos')}}

)

SELECT * FROM promos