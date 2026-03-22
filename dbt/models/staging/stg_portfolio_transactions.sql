{{
    config(
        materialized='incremental',
        unique_key=['transaction_date', 'user_id', 'ticker', 'transaction_type'],
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select * from RAW.kafka_staging.kafka_portfolio_transactions

    {% if is_incremental() %}
        -- on incremental runs, only process rows newer than what's already loaded
        where cast(date as date) > (select max(transaction_date) from {{ this }})
    {% endif %}

),

renamed as (

    select
        cast(date as date)              as transaction_date,
        user_id,
        ticker,
        action                          as transaction_type,   -- BUY or SELL
        shares                          as shares_traded,
        round(cast(price as float), 4)  as price_per_share,
        round(cast(value as float), 4)  as trade_value,        -- shares * price

        -- derived
        case
            when action = 'BUY'  then -1 * round(cast(value as float), 4)
            when action = 'SELL' then      round(cast(value as float), 4)
        end                             as cash_impact

    from source
    where date is not null
      and user_id is not null
      and ticker is not null
      and shares > 0
      and price > 0

)

select * from renamed
