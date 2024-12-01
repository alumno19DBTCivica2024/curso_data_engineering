{{ config(
    materialized='view'
    ) 
}}

WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets','budget') }}
    ),

renamed_casted AS (
    SELECT
          product_id
        , month
        , quantity 
        , _fivetran_synced as date_load
    FROM stg_budget_products
    )

SELECT * FROM renamed_casted