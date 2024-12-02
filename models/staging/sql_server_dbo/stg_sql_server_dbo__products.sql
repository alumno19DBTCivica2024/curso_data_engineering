{{
  config(
    materialized='incremental',
    unique_key = 'product_id',
    on_schema_change='fail'
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
    {% if is_incremental() %}
	  WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )
    {% endif %}
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
        DATE_LOAD,
        CASE 
            WHEN IS_DELETED = TRUE THEN 'DELETED'
            ELSE 'ACTIVE'
        END AS STATUS -- Marca de registros eliminados
    from src_products

    union all

    -- Fila adicional con PROMO_NAME = 'sin promo' y PROMO_ID generado aleatoriamente
    select
        --md5(random()) as PROMO_ID,   -- Genera un PROMO_ID aleatorio
        {{ dbt_utils.generate_surrogate_key(['null']) }} as PRODUCT_ID,
        UPPER('sin producto') as NAME,
        '0.0' AS PRICE,
        '0' AS INVENTORY,
        'NO STOCK' AS STOCK_STATUS,
        convert_timezone('Europe/Berlin', current_timestamp()) as DATE_LOAD,
        'INACTIVE' AS STATUS
)

select * from products_transformado