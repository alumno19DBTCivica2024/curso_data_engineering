{{
  config(
    materialized='incremental',
    unique_key = 'order_id',
    on_schema_change='fail'
  )
}}
WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__orders') }}
),
stg_order_items AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__order_items') }}
),
stg_products AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__products') }}
),
stg_promos AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__promos') }}
),
fct_order_lines AS (
    SELECT
        o.order_id,
        oi.product_id,
        o.user_id,
        o.address_id,
        o.promo_id,
        o.created_at_utc,
        o.delivered_at_utc,
        oi.quantity,
        p.price AS unit_price_usd,
        (oi.quantity * p.price) AS line_total_price,
        (oi.quantity * p.price * (pr.discount / 100)) AS discount_applied,
        ROUND((o.shipping_cost_usd / COUNT(oi.product_id) OVER (PARTITION BY o.order_id)),2) AS shipping_cost,
        DATEDIFF('DAY', o.created_at_utc, o.delivered_at_utc) AS delivery_time_days,
        o.status AS order_status,
        o.DATE_LOAD
    FROM stg_orders o 
    LEFT JOIN stg_order_items oi ON o.order_id = oi.order_id
    LEFT JOIN stg_products p ON oi.product_id = p.product_id
    LEFT JOIN stg_promos pr ON o.promo_id = pr.promo_id
    {% if is_incremental() %}
	    WHERE o.DATE_LOAD > (   SELECT MAX(o.DATE_LOAD) FROM {{ this }} o)
    {% endif %}    
    )

SELECT * FROM fct_order_lines