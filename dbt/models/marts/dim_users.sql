with transactions as (

    select * from {{ ref('stg_portfolio_transactions') }}

),

user_summary as (

    select
        user_id,
        min(transaction_date)           as first_trade_date,
        max(transaction_date)           as last_trade_date,
        count(*)                        as total_trades,
        count(case when transaction_type = 'BUY'  then 1 end) as total_buys,
        count(case when transaction_type = 'SELL' then 1 end) as total_sells,
        count(distinct ticker)          as unique_stocks_traded,
        round(sum(trade_value), 2)      as total_trade_volume

    from transactions
    group by user_id

)

select * from user_summary
