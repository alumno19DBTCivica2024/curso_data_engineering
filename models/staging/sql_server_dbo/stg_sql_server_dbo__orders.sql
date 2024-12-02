{{
  config(
    materialized='incremental',
    unique_key = 'order_id',
    on_schema_change='fail'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),
renamed_casted AS (
    SELECT 
        -- Incluir el resto de las columnas tal cual
        ORDER_ID,
        {{ dbt_utils.generate_surrogate_key(['SHIPPING_SERVICE']) }} AS SHIPPING_SERVICE_ID,
        CASE 
            WHEN TRIM(SHIPPING_SERVICE) = '' THEN UPPER('Not set yet')
            ELSE UPPER(SHIPPING_SERVICE)
        END AS SHIPPING_SERVICE_DESC,
        SHIPPING_COST AS SHIPPING_COST_USD,
        ADDRESS_ID,
        --CREATED_AT,
        CONVERT_TIMEZONE('UTC', CREATED_AT) AS CREATED_AT_UTC,
        CASE 
            WHEN TRIM(PROMO_ID) = '' THEN {{ dbt_utils.generate_surrogate_key(['null']) }}
            ELSE {{ dbt_utils.generate_surrogate_key(['PROMO_ID']) }}
        END AS PROMO_ID,
        --ESTIMATED_DELIVERY_AT,
        CONVERT_TIMEZONE('UTC', ESTIMATED_DELIVERY_AT) AS ESTIMATED_DELIVERY_AT_UTC,
        ORDER_COST AS ITEM_ORDER_COST_USD,
        USER_ID,
        ORDER_TOTAL as TOTAL_ORDER_COST_USD,
        --DELIVERED_AT,
        CONVERT_TIMEZONE('UTC', DELIVERED_AT) AS DELIVERED_AT_UTC,
        TRACKING_ID,
        UPPER(STATUS) as STATUS,
        _FIVETRAN_SYNCED AS DATE_LOAD
    FROM src_orders
    {% if is_incremental() %}
	    WHERE DATE_LOAD > (   SELECT MAX(DATE_LOAD) FROM {{ this }})
    {% endif %}
)

SELECT * FROM renamed_casted