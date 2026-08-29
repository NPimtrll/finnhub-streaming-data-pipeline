{{ config(materialized='view') }}

with source_data as (
    select
        symbol,
        period,
        strong_buy,
        buy,
        hold,
        sell,
        strong_sell,
        ingested_at
    from {{ source('raw', 'recommendations') }}
)

select
    symbol,
    period,
    strong_buy,
    buy,
    hold,
    sell,
    strong_sell,
    ingested_at
from source_data
