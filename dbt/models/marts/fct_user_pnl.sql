with transactions as (

    select * from {{ ref('stg_portfolio_transactions') }}

),

prices as (

    select * from {{ ref('stg_stock_prices') }}

),

-- Running cost basis and share count per user per stock per day
positions as (

    select
        t.user_id,
        t.ticker,
        t.transaction_date,
        t.transaction_type,
        t.shares_traded,
        t.price_per_share,
        t.trade_value,

        -- cumulative shares held after each trade
        sum(
            case
                when t.transaction_type = 'BUY'  then  t.shares_traded
                when t.transaction_type = 'SELL' then -t.shares_traded
            end
        ) over (
            partition by t.user_id, t.ticker
            order by t.transaction_date
            rows between unbounded preceding and current row
        ) as cumulative_shares,

        -- cumulative amount spent on BUYs (cost basis)
        sum(
            case when t.transaction_type = 'BUY' then t.trade_value else 0 end
        ) over (
            partition by t.user_id, t.ticker
            order by t.transaction_date
            rows between unbounded preceding and current row
        ) as cumulative_cost_basis,

        -- cumulative cash received from SELLs (realized proceeds)
        sum(
            case when t.transaction_type = 'SELL' then t.trade_value else 0 end
        ) over (
            partition by t.user_id, t.ticker
            order by t.transaction_date
            rows between unbounded preceding and current row
        ) as cumulative_sell_proceeds

    from transactions t

),

-- Latest position per user per stock per day
latest_positions as (

    select
        user_id,
        ticker,
        transaction_date,
        cumulative_shares,
        cumulative_cost_basis,
        cumulative_sell_proceeds,
        row_number() over (
            partition by user_id, ticker, transaction_date
            order by transaction_date desc
        ) as rn

    from positions

),

-- Join with closing price to calculate unrealized value
pnl as (

    select
        p.user_id,
        p.ticker,
        p.transaction_date                                          as value_date,
        p.cumulative_shares                                         as shares_held,
        p.cumulative_cost_basis                                     as total_cost_basis,
        p.cumulative_sell_proceeds                                  as total_sell_proceeds,
        sp.close_price,

        -- unrealized P&L = current market value of held shares - cost paid for them
        round(p.cumulative_shares * sp.close_price, 2)             as current_market_value,
        round(
            p.cumulative_shares * sp.close_price - p.cumulative_cost_basis + p.cumulative_sell_proceeds
        , 2)                                                        as total_pnl,

        -- realized P&L = sell proceeds - proportional cost of sold shares
        round(p.cumulative_sell_proceeds - (
            p.cumulative_cost_basis * (1 - p.cumulative_shares / nullif(
                p.cumulative_shares + (p.cumulative_sell_proceeds / nullif(sp.close_price, 0)), 0
            ))
        ), 2)                                                       as realized_pnl,

        -- unrealized P&L = market value of remaining shares - cost of remaining shares
        round(
            p.cumulative_shares * sp.close_price - (
                p.cumulative_cost_basis * p.cumulative_shares / nullif(
                    p.cumulative_shares + (p.cumulative_sell_proceeds / nullif(sp.close_price, 0)), 0
                )
            )
        , 2)                                                        as unrealized_pnl

    from latest_positions p
    inner join prices sp
        on p.ticker = sp.ticker
        and p.transaction_date = sp.price_date
    where p.rn = 1
      and p.cumulative_shares >= 0

)

select * from pnl
