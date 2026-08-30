{{ config(materialized='view') }}

with source_data as (
    select
        trade_id,
        symbol,
        price,
        volume,
        timestamp,
        conditions,
        ingested_at
    from {{ source('raw', 'trades') }}
)

select
    trade_id,
    symbol,
    price,
    volume,
    -- Convert unix millisecond timestamp to DateTime64(3)
    toDateTime64(timestamp / 1000.0, 3) as trade_timestamp,
    conditions,
    ingested_at
from source_data
