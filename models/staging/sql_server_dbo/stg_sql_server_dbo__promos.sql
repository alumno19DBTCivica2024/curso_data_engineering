with src_promos as (
    select
        --md5(cast(coalesce(cast(PROMO_ID as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as PROMO_ID, -- Genera un hash basado en el valor de PROMO_ID original
        {{ dbt_utils.generate_surrogate_key(['PROMO_ID']) }} as PROMO_ID,
        PROMO_ID as PROMO_NAME, -- Renombra PROMO_ID original a PROMO_NAME
        DISCOUNT,
        STATUS,
        COALESCE(_FIVETRAN_DELETED, 'false') as IS_DELETED,
        _FIVETRAN_SYNCED as DATE_LOAD
    from {{ source('sql_server_dbo', 'promos') }}
),

promos_transformado as (
    select
        PROMO_ID,
        UPPER(PROMO_NAME) AS PROMO_NAME,
        DISCOUNT/100 as DISCOUNT,
        UPPER(STATUS) AS STATUS,
        UPPER(IS_DELETED),
        DATE_LOAD
    from src_promos

    union all

    -- Fila adicional con PROMO_NAME = 'sin promo' y PROMO_ID generado aleatoriamente
    select
        --md5(random()) as PROMO_ID,   -- Genera un PROMO_ID aleatorio
        {{ dbt_utils.generate_surrogate_key(['null']) }} as PROMO_ID,
        UPPER('sin promo') as PROMO_NAME,
        0 / 100 as DISCOUNT,               -- DISCOUNT = 0
        UPPER('inactive') as STATUS,        -- STATUS = 'inactive'
        UPPER('false') as IS_DELETED,
        convert_timezone('Europe/Berlin', current_timestamp()) as DATE_LOAD
)

select * from promos_transformado