{{ config(
    materialized='incremental',
    unique_key = '_row'
    ) 
    }}

WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
),
/*
max_synced AS (
    SELECT MAX(DATE_LOAD) AS max_fivetran_synced
    FROM {{ this }}
),
*/
renamed_casted AS (
    SELECT
          _row,
          product_id,
          month,
          quantity,
          _fivetran_synced AS DATE_LOAD -- Renombramos _fivetran_synced a date_load
    FROM stg_budget_products
    {% if is_incremental() %}
        WHERE DATE_LOAD > (   SELECT MAX(DATE_LOAD) FROM {{ this }})
    {% endif %}
)

SELECT *
FROM renamed_casted