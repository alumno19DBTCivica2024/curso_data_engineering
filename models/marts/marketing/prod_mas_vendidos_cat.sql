{{
  config(
    materialized='table'
  )
}}

WITH product_sales AS (
    SELECT
        p.name AS product_name,        -- Nombre del producto
        SUM(ol.quantity) AS total_sold -- Cantidad total vendida
    FROM 
        {{ ref('fct_order_lines') }} ol   -- Hecho: Líneas de pedido
    INNER JOIN 
        {{ ref('dim_products') }} p       -- Dimensión: Productos
        ON ol.product_id = p.product_id
    GROUP BY
        p.name
    ORDER BY
        total_sold DESC
)

SELECT *
FROM product_sales