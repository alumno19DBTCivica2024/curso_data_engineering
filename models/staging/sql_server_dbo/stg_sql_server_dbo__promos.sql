{{ config(
    materialized="view"
) }}

with src_promos as (
    select
        md5(cast(coalesce(cast(PROMO_ID as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as PROMO_ID, -- Genera un hash basado en el valor de PROMO_ID original
        PROMO_ID as PROMO_NAME, -- Renombra PROMO_ID original a PROMO_NAME
        DISCOUNT,
        STATUS,
        _FIVETRAN_DELETED as DATE_DELETE,
        _FIVETRAN_SYNCED as DATE_LOAD
    from ALUMNO19_DEV_BRONZE_DB.sql_server_dbo.promos
),

promos_transformado as (
    select
        PROMO_ID,
        PROMO_NAME,
        DISCOUNT,
        STATUS,
        DATE_DELETE,
        DATE_LOAD
    from src_promos

    union all

    -- Fila adicional con PROMO_NAME = 'sin promo' y PROMO_ID generado aleatoriamente
    select
        md5(random()) as PROMO_ID,   -- Genera un PROMO_ID aleatorio
        'sin promo' as PROMO_NAME,
        0 as DISCOUNT,               -- DISCOUNT = 0
        'inactive' as STATUS,        -- STATUS = 'inactive'
        null as DATE_DELETE,
        current_timestamp() as DATE_LOAD
)

select * from promos_transformado