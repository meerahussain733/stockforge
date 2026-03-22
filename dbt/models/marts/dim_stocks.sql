with prices as (

    select * from {{ ref('stg_stock_prices') }}

),

stock_summary as (

    select
        ticker,
        min(price_date)                 as first_date,
        max(price_date)                 as last_date,
        count(*)                        as trading_days,
        round(min(low_price), 4)        as all_time_low,
        round(max(high_price), 4)       as all_time_high,
        round(avg(close_price), 4)      as avg_close_price,
        round(avg(volume), 0)           as avg_daily_volume,
        round(avg(daily_return_pct), 4) as avg_daily_return_pct

    from prices
    group by ticker

)

select * from stock_summary
