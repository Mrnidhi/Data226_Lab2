{{ config(materialized='table') }}

with stock_data as (
    select * from {{ ref('stg_stock_prices') }}
),

calculated_metrics as (
    select
        *,
        -- Simple Moving Averages
        avg(close) over (partition by symbol order by date rows between 4 preceding and current row) as sma_5,
        avg(close) over (partition by symbol order by date rows between 9 preceding and current row) as sma_10,
        avg(close) over (partition by symbol order by date rows between 19 preceding and current row) as sma_20,
        
        -- Daily Returns
        (close - lag(close) over (partition by symbol order by date)) / lag(close) over (partition by symbol order by date) as daily_return,
        
        -- Volume SMA
        avg(volume) over (partition by symbol order by date rows between 4 preceding and current row) as volume_sma_5
        
    from stock_data
)

select
    *,
    -- Relative Strength Index (RSI) placeholder
    case when sma_5 > sma_20 then 'BULLISH' else 'BEARISH' end as trend_signal
from calculated_metrics

