{{ config(materialized='table') }}

with base as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        lag(close) over (partition by symbol order by date) as prev_close,
        close - lag(close) over (partition by symbol order by date) as delta
    from {{ ref('stg_stock_prices') }}
),

with_gains_losses as (
    select
        *,
        case when delta > 0 then delta else 0 end as gain,
        case when delta < 0 then abs(delta) else 0 end as loss
    from base
),

with_indicators as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        prev_close,
        delta as price_momentum,
        delta / nullif(prev_close, 0) as daily_return,
        avg(close) over (partition by symbol order by date rows between 4 preceding and current row) as sma_5,
        avg(close) over (partition by symbol order by date rows between 9 preceding and current row) as sma_10,
        avg(close) over (partition by symbol order by date rows between 19 preceding and current row) as sma_20,
        avg(volume) over (partition by symbol order by date rows between 4 preceding and current row) as volume_sma_5,
        avg(gain) over (partition by symbol order by date rows between 13 preceding and current row) as avg_gain_14,
        avg(loss) over (partition by symbol order by date rows between 13 preceding and current row) as avg_loss_14
    from with_gains_losses
)

select
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    prev_close,
    price_momentum,
    daily_return,
    sma_5,
    sma_10,
    sma_20,
    volume_sma_5,
    avg_gain_14,
    avg_loss_14,
    round(100 - (100 / (1 + (avg_gain_14 / nullif(avg_loss_14, 0)))), 2) as rsi_14,
    case when sma_5 > sma_20 then 'BULLISH' else 'BEARISH' end as trend_signal
from with_indicators
