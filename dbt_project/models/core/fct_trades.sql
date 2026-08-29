{{ config(
    materialized='incremental',
    engine='MergeTree()',
    order_by=['symbol', 'trade_timestamp']
) }}

with staged_trades as (
    select * from {{ ref('stg_trades') }}
)

select
    -- Generate unique trade key using ClickHouse hash functions
    hex(MD5(concat(symbol, '-', toString(trade_timestamp), '-', toString(price), '-', toString(volume)))) as trade_key,
    symbol,
    price,
    volume,
    trade_timestamp,
    conditions,
    ingested_at
from staged_trades

{% if is_incremental() %}
    -- Incremental load filter
    where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
