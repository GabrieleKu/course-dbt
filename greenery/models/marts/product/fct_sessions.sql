
--{% set event_types = get_event_types() %}
{% set event_types = dbt_utils.get_column_values(table=ref('stg_events'), column='event_type') %}

WITH events AS (
    SELECT * FROM {{ ref('fct_events') }}
),

sessions AS (

    SELECT
    events.session_id,
    events.user_id,
    events.user_created_at_utc,
    events.user_state,
    events.user_country,
    MIN(events.created_at_utc) AS session_created_at_utc,
    {% for event_type in event_types %}
    SUM(CASE WHEN event_type = '{{event_type}}' THEN 1 ELSE 0 END) AS total_{{event_type}}_events
    {% if not loop.last %},{% endif %}
    {% endfor %} ,
    {% for event_type in event_types %}
    MAX(CASE WHEN event_type = '{{event_type}}' THEN 1 ELSE 0 END) AS had_{{event_type}}
    {% if not loop.last %},{% endif %}  
    {% endfor %}

    FROM events

    GROUP BY 1,2,3,4,5

)

SELECT
*

FROM sessions 