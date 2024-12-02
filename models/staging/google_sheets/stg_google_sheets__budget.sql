{{ config(
    materialized='incremental',
    unique_key = '_row'
    ) 
    }}

WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
),

renamed_casted AS (
    SELECT
          _row,
          product_id,
          month,
          quantity,
          _fivetran_synced AS date_load -- Renombramos _fivetran_synced a date_load
    FROM stg_budget_products
    {% if is_incremental() %}
	WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )
    {% endif %}
)

SELECT *
FROM renamed_casted