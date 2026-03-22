-- Test: all stock prices must be positive
-- Negative or zero prices indicate corrupt source data

select *
from {{ ref('stg_stock_prices') }}
where close_price <= 0
   or open_price  <= 0
   or high_price  <= 0
   or low_price   <= 0
