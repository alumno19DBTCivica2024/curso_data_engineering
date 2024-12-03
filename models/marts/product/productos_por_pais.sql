{{
  config(
    materialized='table'
  )
}}

WITH product_sales_by_country AS (
    SELECT
        da.state_desc AS STATE,          -- Nombre del país
        dp.name AS PRODUCT,             -- Nombre del producto
        SUM(fol.quantity) AS TOTAL_SOLD      -- Total de unidades vendidas
    FROM 
        {{ ref('fct_order_lines') }} fol     -- Hecho: Líneas de pedido
    INNER JOIN 
        {{ ref('dim_addresses') }} da        -- Dimensión: Direcciones
        ON fol.address_id = da.address_id
    INNER JOIN 
        {{ ref('dim_products') }} dp         -- Dimensión: Productos
        ON fol.product_id = dp.product_id
    GROUP BY
        da.state_desc, dp.name
    ORDER BY
        da.state_desc, total_sold DESC     -- Ordena por país y ventas descendentes
)

SELECT *
FROM product_sales_by_country