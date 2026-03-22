with source as (

    select * from RAW.kafka_staging.stock_prices

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
