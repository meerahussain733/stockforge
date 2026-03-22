-- Test: daily high price must always be >= daily low price
-- A violation means the source data is corrupted or incorrectly loaded

select *
from {{ ref('stg_stock_prices') }}
where high_price < low_price
