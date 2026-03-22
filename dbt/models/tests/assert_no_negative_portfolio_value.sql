-- Test: no user should ever have a negative total portfolio value
-- A negative value would indicate a data or logic error in fct_daily_portfolio_value

select *
from {{ ref('fct_daily_portfolio_value') }}
where total_portfolio_value < 0
