WITH promos AS (

    SELECT
        promo_id
        , discount
        , status

    FROM {{ source('postgres','promos')}}

)

SELECT * FROM promos