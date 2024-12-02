{{
  config(
    materialized='incremental',
    unique_key = 'user_id'
  )
}}
WITH src_users as (
    select
        USER_ID,
        UPDATED_AT,
        ADDRESS_ID,
        UPPER(LAST_NAME) AS LAST_NAME,
        CREATED_AT,
        PHONE_NUMBER,
        UPPER(FIRST_NAME) AS FIRST_NAME,
        EMAIL,
        _FIVETRAN_SYNCED
    from {{ source('sql_server_dbo', 'users') }}
),

users_transformado as (
    select
        USER_ID,
        CONVERT_TIMEZONE('UTC', UPDATED_AT) AS UPDATED_AT_UTC,
        ADDRESS_ID,
        LAST_NAME,
        CONVERT_TIMEZONE('UTC', CREATED_AT) AS CREATED_AT_UTC,
        PHONE_NUMBER,
        FIRST_NAME,
        EMAIL,
        _FIVETRAN_SYNCED AS DATE_LOAD
    from src_users
    {% if is_incremental() %}
    --WHERE _fivetran_synced > (SELECT max_date_load FROM max_synced) -- Filtramos los registros nuevos
        WHERE DATE_LOAD > (   SELECT MAX(DATE_LOAD) FROM {{ this }})
    {% endif %}
)

select * from users_transformado