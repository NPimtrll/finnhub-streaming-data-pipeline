{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['asset_category', 'symbol', 'window_start']
) }}

with trades as (
    select * from {{ ref('fct_trades') }}
),

assets as (
    select * from {{ ref('dim_assets') }}
)

select
    t.symbol,
    a.asset_name,
    a.category as asset_category,
    toStartOfMinute(t.trade_timestamp) as window_start,
    argMin(t.price, t.trade_timestamp) as open,
    max(t.price) as high,
    min(t.price) as low,
    argMax(t.price, t.trade_timestamp) as close,
    max(t.price) - min(t.price) as price_spread,
    if(open > 0, (max(t.price) - min(t.price)) / open, 0) as relative_spread,
    sum(t.volume) as volume,
    count() as trade_count,
    avg(t.price) as avg_price
from trades t
left join assets a on t.symbol = a.symbol
group by
    t.symbol,
    a.asset_name,
    a.category,
    window_start
