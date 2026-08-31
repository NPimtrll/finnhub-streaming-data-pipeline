{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['symbol']
) }}

with latest_trades as (
    select
        symbol,
        argMax(price, trade_timestamp) as current_price
    from {{ ref('stg_trades') }}
    group by symbol
),

assets as (
    select * from {{ ref('dim_assets') }}
),

latest_targets as (
    select * from (
        select
            symbol,
            target_high,
            target_low,
            target_mean,
            target_median,
            number_of_analyst,
            last_updated,
            row_number() over (partition by symbol order by last_updated desc, ingested_at desc) as rn
        from {{ ref('stg_price_targets') }}
    ) where rn = 1
),

latest_recs as (
    select * from (
        select
            symbol,
            period,
            strong_buy,
            buy,
            hold,
            sell,
            strong_sell,
            row_number() over (partition by symbol order by period desc, ingested_at desc) as rn
        from {{ ref('stg_recommendations') }}
    ) where rn = 1
)

select
    t.symbol as symbol,
    a.asset_name as asset_name,
    a.category as asset_category,
    t.current_price as current_price,
    tg.target_median as target_median,
    tg.target_mean as target_mean,
    tg.target_high as target_high,
    tg.target_low as target_low,
    tg.number_of_analyst as number_of_analyst,
    
    -- Calculate 1-Year Upside Potential based on target median
    if(t.current_price > 0 and tg.target_median > 0, 
       round(((tg.target_median - t.current_price) / t.current_price) * 100, 2), 
       0.0
    ) as upside_potential_percent,
    
    -- Calculate Estimated 3-Month Gain % (assumed as 25% of 1-Year Median Target)
    if(t.current_price > 0 and tg.target_median > 0, 
       round((((tg.target_median - t.current_price) / t.current_price) * 100) * 0.25, 2), 
       0.0
    ) as estimated_gain_3m_percent,

    -- Recommendations counts
    rc.strong_buy as strong_buy,
    rc.buy as buy,
    rc.hold as hold,
    rc.sell as sell,
    rc.strong_sell as strong_sell,
    (rc.strong_buy + rc.buy + rc.hold + rc.sell + rc.strong_sell) as total_analyst_ratings,

    -- Determine Consensus Rating
    multiIf(
        (rc.strong_buy + rc.buy) > ((rc.strong_buy + rc.buy + rc.hold + rc.sell + rc.strong_sell) * 0.70), 'Strong Buy',
        (rc.strong_buy + rc.buy) > ((rc.strong_buy + rc.buy + rc.hold + rc.sell + rc.strong_sell) * 0.50), 'Buy',
        (rc.sell + rc.strong_sell) > ((rc.strong_buy + rc.buy + rc.hold + rc.sell + rc.strong_sell) * 0.30), 'Sell',
        'Hold'
    ) as consensus_rating

from latest_trades t
left join assets a on t.symbol = a.symbol
left join latest_targets tg on t.symbol = tg.symbol
left join latest_recs rc on t.symbol = rc.symbol
