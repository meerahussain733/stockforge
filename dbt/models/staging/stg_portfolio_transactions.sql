with source as (

    select * from RAW.kafka_staging.portfolio_transactions

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
        end                             as cash_impact          -- negative = cash spent, positive = cash received

    from source
    where date is not null
      and user_id is not null
      and ticker is not null
      and shares > 0
      and price > 0

)

select * from renamed
