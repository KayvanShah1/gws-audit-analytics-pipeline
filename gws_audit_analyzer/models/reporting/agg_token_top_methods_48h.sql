{{ config(materialized="view") }}

with
    window_bounds as (
        select
            max(timestamp) as max_ts,
            max(timestamp) - interval '48 hours' as window_start
        from {{ ref("fact_token_event") }}
    ),

    filtered as (
        select
            b.timestamp,
            b.method_name,
            b.num_bytes
        from {{ ref("fact_token_event") }} b
        cross join window_bounds w
        where b.timestamp >= w.window_start
    ),

    agg as (
        select
            method_name,
            count(*) as event_count,
            sum(num_bytes) as total_bytes
        from filtered
        where method_name is not null
        group by method_name
    )

select
    method_name,
    event_count,
    total_bytes,
    (select window_start from window_bounds) as window_start,
    (select max_ts from window_bounds) as window_end,
    row_number() over (order by total_bytes desc, event_count desc) as rank_by_bytes
from agg
order by rank_by_bytes
