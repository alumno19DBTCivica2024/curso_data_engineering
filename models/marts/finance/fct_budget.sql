/*
    Tabla de hechos: fct_budget
    Granularidad: producto por mes
    Contiene: presupuesto asignado para cada producto en un mes
*/
/*
{{
  config(
    materialized='table'
  )
}}
*/
{{ config(
    materialized='incremental',
    unique_key = 'product_id'
    ) 
    }}
WITH budget_aggregated AS (
    SELECT 
        b.product_id,
        DATE_TRUNC('month', b.month) AS month,  -- Aseguramos granularidad mensual
        SUM(b.quantity) AS budget_quantity,    -- Cantidad presupuestada
        CURRENT_DATE AS date_load              -- Fecha de carga
    FROM {{ ref('stg_google_sheets__budget') }} b
    {% if is_incremental() %}
        WHERE DATE_LOAD > (   SELECT MAX(DATE_LOAD) FROM {{ this }})
    {% endif %}
    GROUP BY b.product_id, DATE_TRUNC('month', b.month)
)

SELECT 
    product_id,
    month,
    budget_quantity,
    date_load
FROM budget_aggregated