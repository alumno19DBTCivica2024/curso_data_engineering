{{
  config(
    materialized='incremental',
    unique_key = 'promo_id',
    on_schema_change='fail'
  )
}}
    WITH stg_promos AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__promos')}}
    ),

    stg_orders AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__orders')}}
    ),


dim_promos_gold AS (
    SELECT
        p.promo_id,
        p.status,
        p.discount,
        COUNT(o.order_id) AS total_orders,
        ROUND(SUM(o.item_order_cost_usd * (p.discount / 100)), 2) as total_discount_given,
        p.DATE_LOAD
    FROM stg_promos p
    LEFT JOIN 
        stg_orders o ON p.promo_id = o.promo_id
    {% if is_incremental() %}
        WHERE p.DATE_LOAD > (   SELECT MAX(p.DATE_LOAD) FROM {{ this }} p)
    {% endif %}
    GROUP BY 
       p.promo_id, p.status, p.discount, p.DATE_LOAD
)

SELECT * FROM dim_promos_gold