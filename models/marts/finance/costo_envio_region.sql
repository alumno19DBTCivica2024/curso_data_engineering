{{
  config(
    materialized='table'
  )
}}

WITH shipping_cost_by_region AS (
    SELECT
        da.state_desc AS region,          -- Región (estado o país)
        SUM(fol.shipping_cost) AS total_shipping_cost_usd, -- Costos totales de envío
        COUNT(DISTINCT fol.order_id) AS total_orders, -- Total de pedidos
        ROUND(AVG(fol.shipping_cost),2) AS avg_shipping_cost_usd -- Costo promedio de envío por pedido
    FROM 
        {{ ref('fct_order_lines') }} fol
    INNER JOIN 
        {{ ref('dim_addresses') }} da
        ON fol.address_id = da.address_id
    GROUP BY 
        da.state_desc
    ORDER BY 
        total_shipping_cost_usd DESC -- Ordenar por costos totales de envío
)

SELECT *
FROM shipping_cost_by_region