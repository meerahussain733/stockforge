{{
    config(
        materialized='incremental',
        unique_key=['price_date', 'ticker'],
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select * from RAW.kafka_staging.kafka_stock_prices

    {% if is_incremental() %}
        -- on incremental runs, only process rows newer than what's already loaded
        where cast(date as date) > (select max(price_date) from {{ this }})
    {% endif %}

),

renamed as (

    select
        cast(date as date)      as price_date,
        ticker,
        open                    as open_price,
        high                    as high_price,
        low                     as low_price,
        close                   as close_price,
        volume,

        -- derived
        round(high - low, 4)    as daily_range,
        round((close - open) / nullif(open, 0) * 100, 4) as daily_return_pct

    from source
    where date is not null
      and ticker is not null
      and close > 0

)

select * from renamed
