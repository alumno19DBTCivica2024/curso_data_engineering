{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
),

src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
),

src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
),

-- Calcular el costo total de los productos por pedido
item_order_cost AS (
    SELECT 
        oi.ORDER_ID,
        SUM(oi.QUANTITY * p.PRICE) AS ITEM_ORDER_COST_USD
    FROM src_order_items oi
    LEFT JOIN src_products p
        ON oi.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY oi.ORDER_ID
)

SELECT 
    o.ORDER_ID,
    o.SHIPPING_SERVICE,
    o.SHIPPING_COST,
    o.ADDRESS_ID,
    o.CREATED_AT,
    o.PROMO_ID,
    o.ESTIMATED_DELIVERY_AT,
    o.ORDER_COST,
    o.USER_ID,
    o.ORDER_TOTAL,
    o.DELIVERED_AT,
    o.TRACKING_ID,
    o.STATUS,
    o._FIVETRAN_DELETED,
    o._FIVETRAN_SYNCED,
    ioc.ITEM_ORDER_COST_USD
FROM src_orders o
LEFT JOIN item_order_cost ioc
    ON o.ORDER_ID = ioc.ORDER_ID
