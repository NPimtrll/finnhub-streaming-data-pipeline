{{ config(materialized='table') }}

select
    symbol,
    asset_name,
    category
from {{ ref('assets') }}
