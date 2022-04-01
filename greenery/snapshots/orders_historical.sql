{% snapshot orders_historical %}

  {{
    config(
      target_schema='dbt_gabriele_k',
      unique_key='order_id',
      strategy = 'check',
      check_cols = ['status']
    )
  }}

  SELECT * FROM {{ source('postgres','orders')}}

{% endsnapshot %}