{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT 
        -- Incluir el resto de las columnas tal cual
        {{ dbt_utils.generate_surrogate_key(['SHIPPING_SERVICE']) }} AS SHIPPING_SERVICE_ID,
        CASE 
            WHEN TRIM(SHIPPING_SERVICE) = '' THEN UPPER('Not set yet')
            ELSE UPPER(SHIPPING_SERVICE)
        END AS SHIPPING_SERVICE_DESC,
        SHIPPING_COST,
        ADDRESS_ID,
        CREATED_AT,
        CASE 
            WHEN TRIM(PROMO_ID) = '' THEN {{ dbt_utils.generate_surrogate_key(['null']) }}
            ELSE {{ dbt_utils.generate_surrogate_key(['PROMO_ID']) }}
        END AS PROMO_ID,
        ESTIMATED_DELIVERY_AT,
        ORDER_COST,
        USER_ID,
        ORDER_TOTAL,
        DELIVERED_AT,
        TRACKING_ID,
        UPPER(STATUS) as STATUS,
        UPPER(COALESCE(_FIVETRAN_DELETED, 'false')) as IS_DELETED,
        _FIVETRAN_SYNCED AS DATE_LOAD
    FROM src_orders
)

SELECT * FROM renamed_casted