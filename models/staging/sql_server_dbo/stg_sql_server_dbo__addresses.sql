{{
  config(
    materialized='incremental',
    unique_key = 'address_id',
    on_schema_change='fail'
  )
}}

WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),
renamed_addresses_casted AS (
    SELECT
          ADDRESS_ID,
          md5(cast(coalesce(cast(ZIPCODE as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as ZIPCODE_ID, -- Genera un hash basado en el valor de ZIPCODE original
          ZIPCODE AS ZIPCODE_DESC,
          md5(cast(coalesce(cast(COUNTRY as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as COUNTRY_ID, -- Genera un hash basado en el valor de COUNTRY original
          UPPER(COUNTRY) AS COUNTRY_DESC,
          UPPER(ADDRESS) AS ADDRESS,
          md5(cast(coalesce(cast(STATE as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as STATE_ID, -- Genera un hash basado en el valor de STATE original
          UPPER(STATE) AS STATE_DESC,
          _FIVETRAN_SYNCED  as DATE_LOAD
    FROM src_addresses
    {% if is_incremental() %}
	    WHERE DATE_LOAD > (   SELECT MAX(DATE_LOAD) FROM {{ this }})
    {% endif %}
    )

SELECT * FROM renamed_addresses_casted