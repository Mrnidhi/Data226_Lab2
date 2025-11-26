{% snapshot stock_prices_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='symbol || date',
        strategy='check',
        check_cols=['close', 'volume']
    )
}}

select * from {{ ref('stg_stock_prices') }}

{% endsnapshot %}

