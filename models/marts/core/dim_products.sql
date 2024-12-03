{{
  config(
    materialized='incremental',
    unique_key = 'product_id',
    on_schema_change='fail'
  )
}}
    WITH stg_products AS (
        SELECT *
        FROM {{ref('stg_sql_server_dbo__products')}}
    ),
    dim_products_gold AS (
        SELECT
            p.product_id,
            p.name,
            p.price,
            p.inventory,
            p.stock_status,
            p.DATE_LOAD
        FROM stg_products p
        {% if is_incremental() %}
	        WHERE p.DATE_LOAD > ( SELECT MAX(p.DATE_LOAD) FROM {{ this }} p)
        {% endif %}
        
    )

SELECT * FROM dim_products_gold