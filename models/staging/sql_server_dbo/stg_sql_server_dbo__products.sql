{{
  config(
    materialized='view'
  )
}}

with src_products as (
    select
        PRODUCT_ID,
        PRICE,
        NAME,
        INVENTORY,
        _FIVETRAN_DELETED AS IS_DELETED,
        _FIVETRAN_SYNCED  AS DATE_LOAD
    from {{ source('sql_server_dbo', 'products') }}
),

products_transformado as (
    select
        PRODUCT_ID,
        UPPER(COALESCE(NAME,'Producto Desconocido')) AS NAME,
        CASE 
            WHEN PRICE < 0 THEN 0
            ELSE PRICE
        END AS PRICE, -- Aseguramos precios válidos
        CASE
            WHEN INVENTORY < 0 THEN 0
            ELSE INVENTORY
        END AS INVENTORY, -- Inventario corregido
        CASE
            WHEN INVENTORY < 0 THEN UPPER('Invalid')
            WHEN INVENTORY = 0 THEN UPPER('Out of Stock')
            ELSE UPPER('In Stock')
        END AS STOCK_STATUS, -- Estado del inventario
        UPPER(COALESCE(IS_DELETED, 'FALSE')) AS IS_DELETED,
        DATE_LOAD,
        CASE 
            WHEN IS_DELETED = TRUE THEN 'DELETED'
            ELSE 'ACTIVE'
        END AS STATUS -- Marca de registros eliminados
    from src_products
)

select * from products_transformado