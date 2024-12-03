{{
  config(
    materialized='table'
  )
}}

WITH dim_users AS (
    SELECT *
    FROM {{ ref('dim_users') }} -- Información del usuario
),
dim_addresses AS (
    SELECT *
    FROM {{ ref('dim_addresses') }} -- Información de direcciones de pedidos y/o usuarios
),
order_lines_aggregated AS (
    -- Agrupamos por usuario para calcular métricas consolidadas
    SELECT
        ol.user_id,
        SUM(ol.line_total_price + ol.shipping_cost) AS total_order_cost_usd, -- Total gastado en pedidos
        SUM(ol.shipping_cost) AS total_shipping_cost_usd, -- Total de gastos de envío
        COUNT(DISTINCT ol.order_id) AS total_number_orders, -- Total de pedidos realizados
        SUM(ol.discount_applied) AS total_discount_usd,
        SUM(ol.quantity) AS total_quantity_product, -- Total de productos comprados
        COUNT(DISTINCT ol.product_id) AS total_different_products -- Total de productos diferentes comprados
    FROM 
        {{ ref('fct_order_lines') }} ol
    GROUP BY 
        ol.user_id
),
user_orders AS (
    -- Combinar métricas con información de usuarios y direcciones
    SELECT
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        u.phone_number,
        u.created_at_utc,
        u.updated_at_utc,
        a.address,
        a.zipcode_desc,
        a.state_desc,
        a.country_desc,
        COALESCE(ol.total_number_orders, 0) AS total_number_orders, -- Total de pedidos realizados
        ROUND(COALESCE(ol.total_order_cost_usd, 0), 2) AS total_order_cost_usd, -- Total gastado
        ROUND(COALESCE(ol.total_shipping_cost_usd, 0), 2) AS total_shipping_cost_usd, -- Total de gastos de envío
        COALESCE(ol.total_quantity_product, 0) AS total_quantity_product, -- Total de productos comprados
        COALESCE(ol.total_different_products, 0) AS total_different_products, -- Total de productos diferentes comprados
        ROUND(COALESCE(ol.total_discount_usd, 0),2) AS total_discount_usd -- Total descuento en el pedido
    FROM 
        dim_users u
    LEFT JOIN 
        dim_addresses a ON u.address_id = a.address_id
    LEFT JOIN 
        order_lines_aggregated ol ON u.user_id = ol.user_id
)

SELECT *
FROM user_orders