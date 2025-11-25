{{ config(materialized='view') }}

select
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume
from {{ source('raw', 'stock_data') }}

