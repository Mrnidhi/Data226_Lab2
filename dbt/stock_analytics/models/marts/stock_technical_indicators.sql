{{ config(materialized='table') }}

/*
    Stock Technical Indicators Model
    --------------------------------
    Transforms raw stock prices into analytical metrics:
    - Moving averages (SMA 5/10/20)
    - Price momentum and daily returns
    - RSI (Relative Strength Index) over 14 periods
    - Trend signal (bullish/bearish based on SMA crossover)
*/

-- Step 1: Get base stock data with previous close and delta
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

-- Step 2: Separate gains and losses for RSI calculation
with_gains_losses as (
    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        prev_close,
        delta,
        case when delta > 0 then delta else 0 end as gain,
        case when delta < 0 then abs(delta) else 0 end as loss
    from base
),

-- Step 3: Calculate all technical indicators
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
        
        -- Price momentum (raw dollar change)
        delta as price_momentum,
        
        -- Daily return (percentage change)
        delta / nullif(prev_close, 0) as daily_return,
        
        -- Simple Moving Averages
        avg(close) over (partition by symbol order by date rows between 4 preceding and current row) as sma_5,
        avg(close) over (partition by symbol order by date rows between 9 preceding and current row) as sma_10,
        avg(close) over (partition by symbol order by date rows between 19 preceding and current row) as sma_20,
        
        -- Volume SMA
        avg(volume) over (partition by symbol order by date rows between 4 preceding and current row) as volume_sma_5,
        
        -- RSI components: 14-period average gain and loss
        avg(gain) over (partition by symbol order by date rows between 13 preceding and current row) as avg_gain_14,
        avg(loss) over (partition by symbol order by date rows between 13 preceding and current row) as avg_loss_14
        
    from with_gains_losses
)

-- Step 4: Final output with RSI and trend signal
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
    
    -- RSI formula: 100 - (100 / (1 + RS)) where RS = avg_gain / avg_loss
    round(100 - (100 / (1 + (avg_gain_14 / nullif(avg_loss_14, 0)))), 2) as rsi_14,
    
    -- Trend signal based on SMA crossover
    case 
        when sma_5 > sma_20 then 'BULLISH' 
        else 'BEARISH' 
    end as trend_signal

from with_indicators

