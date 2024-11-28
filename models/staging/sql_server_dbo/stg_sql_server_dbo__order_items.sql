/*
Resumen del proceso

    1. Limpieza: eliminar duplicados y registros con valores inválidos
    2. Validación: Verificar relación entre order_id y product_id en
    las tables de orders y products.
    3. Filtrar las filas que no cumplan ambas validaciones.


*/
{{
  config(
    materialized='view'
  )
}}

WITH src_order_items AS (
    SELECT DISTINCT 
        order_id,
        product_id,
        quantity,
        _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED
    FROM {{ source('sql_server_dbo', 'order_items') }}
    WHERE quantity > 0 -- Eliminamos valores negativos o no válidos
    ),

renamed_casted AS (
    SELECT 
        c.order_id AS ORDER_ID,
        c.product_id AS PRODUCT_ID,
        c.quantity AS QUANTITY,
        CASE 
            WHEN EXISTS(
                SELECT 1 FROM {{ ref('stg_sql_server_dbo__orders') }} o
                WHERE o.order_id = c.order_id
            ) THEN TRUE
            ELSE FALSE 
        END AS VALID_ORDER,
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM {{ref('stg_sql_server_dbo__products')}} p
                WHERE p.product_id = c.product_id 
            ) THEN TRUE 
            ELSE FALSE 
        END AS VALID_PRODUCT,
        _FIVETRAN_SYNCED AS DATE_LOAD
    FROM src_order_items c
)

SELECT * FROM renamed_casted