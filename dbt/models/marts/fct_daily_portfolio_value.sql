with transactions as (

    select * from {{ ref('stg_portfolio_transactions') }}

),

prices as (

    select * from {{ ref('stg_stock_prices') }}

),

-- Running share position per user per stock up to each date
positions as (

    select
        t.user_id,
        t.ticker,
        t.transaction_date,
        sum(
            case
                when t.transaction_type = 'BUY'  then  t.shares_traded
                when t.transaction_type = 'SELL' then -t.shares_traded
            end
        ) over (
            partition by t.user_id, t.ticker
            order by t.transaction_date
            rows between unbounded preceding and current row
        ) as cumulative_shares

    from transactions t

),

-- Join positions with closing price on each date
portfolio_value as (

    select
        p.user_id,
        p.ticker,
        p.transaction_date                              as value_date,
        p.cumulative_shares,
        sp.close_price,
        round(p.cumulative_shares * sp.close_price, 2)  as position_value

    from positions p
    inner join prices sp
        on p.ticker = sp.ticker
        and p.transaction_date = sp.price_date
    where p.cumulative_shares > 0

),

-- Total portfolio value per user per day (across all stocks)
daily_totals as (

    select
        user_id,
        value_date,
        round(sum(position_value), 2)   as total_portfolio_value,
        count(distinct ticker)          as stocks_held

    from portfolio_value
    group by user_id, value_date

)

select * from daily_totals
