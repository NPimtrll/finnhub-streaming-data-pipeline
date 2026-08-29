{{ config(materialized='view') }}

with source_data as (
    select
        symbol,
        target_high,
        target_low,
        target_mean,
        target_median,
        number_of_analyst,
        last_updated,
        ingested_at
    from {{ source('raw', 'price_targets') }}
)

select
    symbol,
    target_high,
    target_low,
    target_mean,
    target_median,
    number_of_analyst,
    last_updated,
    ingested_at
from source_data
