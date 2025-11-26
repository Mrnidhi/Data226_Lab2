-- Test: volume should always be >= 0
select symbol, date, volume
from {{ ref('stg_stock_prices') }}
where volume < 0

