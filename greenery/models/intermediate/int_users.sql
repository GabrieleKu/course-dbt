WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
)

, addresses AS (
    SELECT * FROM {{ ref('stg_addresses') }}
)

SELECT
users.user_id,
users.created_at_utc AS user_created_at_utc,
addresses.address_id,
addresses.state AS user_state,
addresses.country AS user_country


FROM users

LEFT JOIN addresses
    ON users.address_id = addresses.address_id