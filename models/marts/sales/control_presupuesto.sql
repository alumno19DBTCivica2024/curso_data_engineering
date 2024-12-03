
{{
  config(
    materialized='table'
  )
}}

WITH ventas_prod_mes AS (
    SELECT 
        ol.product_id,
        p.name,
        CAST(DATE_TRUNC('month', ol.created_at_utc) AS DATE) AS month, -- Convertir TIMESTAMPZ a DATE
        SUM(ol.quantity) AS sold_quantity,
        SUM(ol.line_total_price) AS revenue_real
    FROM {{ ref('fct_order_lines') }} ol
        INNER JOIN {{ref('dim_products')}} p ON ol.product_id = p.product_id
    GROUP BY ol.product_id, CAST(DATE_TRUNC('month', ol.created_at_utc) AS DATE), p.name
),

presupuesto_prod_mes AS (
    SELECT 
        b.product_id,
        b.month,
        SUM(b.budget_quantity) AS budget_quantity -- Cantidad presupuestada
    FROM {{ ref('fct_budget') }} b
    GROUP BY b.product_id, b.month
),
unit_price_prod AS (
    SELECT 
        p.product_id,
        AVG(p.price) AS unit_price -- Precio unitario promedio por producto
    FROM {{ ref('dim_products') }} p
    GROUP BY p.product_id
),

comparacion_presupuesto AS (
    SELECT 
        p.product_id,
        v.name,
        p.month AS P_month,
        v.month AS V_month,
        p.budget_quantity,
        COALESCE(v.sold_quantity, 0) AS sold_quantity,
        COALESCE(v.revenue_real, 0) AS revenue_real,
        ROUND(p.budget_quantity * COALESCE(up.unit_price, 0), 2) AS revenue_predicted,
        (p.budget_quantity - COALESCE(v.sold_quantity, 0)) AS difference_quantity,
        ROUND((COALESCE(v.sold_quantity, 0) / NULLIF(p.budget_quantity, 0)) * 100, 2) AS fulfillment_percentage -- Porcentaje de cumplimiento respecto al presupuesto.
    FROM 
        presupuesto_prod_mes p
    LEFT JOIN ventas_prod_mes v 
        ON p.product_id = v.product_id 
        AND p.month = v.month
    LEFT JOIN unit_price_prod up
        ON p.product_id = up.product_id
)
SELECT * FROM comparacion_presupuesto