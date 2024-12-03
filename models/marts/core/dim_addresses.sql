{{
  config(
    materialized='incremental',
    unique_key = 'address_id'
  )
}}
WITH stg_address AS (
    SELECT *
    FROM {{ref('stg_sql_server_dbo__addresses')}}
),

stg_orders AS (
    SELECT *
    FROM {{ref('stg_sql_server_dbo__orders')}}
),

dim_address_gold AS (
    SELECT
        a.address_id,
        a.address,
        a.zipcode_id,
        a.zipcode_desc,
        a.country_id,
        a.country_desc,
        a.state_id,
        a.state_desc,
        COUNT(o.order_id) AS total_orders,
        MAX(o.created_at_utc) AS last_order_date,
        a.DATE_LOAD as DATE_LOAD
    FROM stg_address a
    LEFT JOIN 
        stg_orders o ON a.address_id = o.address_id
    {% if is_incremental() %}
	    WHERE a.DATE_LOAD > (   SELECT MAX(a.DATE_LOAD) FROM {{ this }} a)
    {% endif %}
    GROUP BY 
        a.address_id, a.address, a.zipcode_id,a.zipcode_desc,a.country_desc, a.state_id,a.state_desc, a.country_id, a.DATE_LOAD
)

SELECT * FROM dim_address_gold