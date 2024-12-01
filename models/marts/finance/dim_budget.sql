WITH stg_budget AS (
    SELECT *
    FROM {{ ref('stg_google_sheets__budget') }}
),

dim_budget AS (
    SELECT
        b.product_id,                              -- Identificador del producto
        TO_DATE(b.month) AS month,                 -- Convertimos el mes en formato fecha (YYYY-MM-01)
        b.quantity AS budget_quantity,             -- Cantidad presupuestada para el producto
        b.date_load                                -- Fecha en que se cargaron los datos
    FROM stg_budget b
)

SELECT * FROM dim_budget