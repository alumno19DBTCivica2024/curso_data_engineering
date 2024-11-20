{{
  config(
    materialized='view'
  )
}}

with src_promos as (
    select
        USER_ID,
        UPDATED_AT,
        ADDRESS_ID,
        UPPER(LAST_NAME) AS LAST_NAME,
        CREATED_AT,
        PHONE_NUMBER,
        UPPER(FIRST_NAME) AS FIRST_NAME,
        EMAIL,
        _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED
    from {{ source('sql_server_dbo', 'users') }}
),

users_transformado as (
    select
        USER_ID,
        UPDATED_AT,
        LAST_NAME,
        CREATED_AT,
        PHONE_NUMBER,
        FIRST_NAME,
        EMAIL,
        UPPER(COALESCE(_FIVETRAN_DELETED, 'false')) as IS_DELETED,
        _FIVETRAN_SYNCED AS DATE_LOAD
    from src_promos
)

select * from users_transformado