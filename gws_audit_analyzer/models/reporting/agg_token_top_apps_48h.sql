{{ config(materialized="view") }}

with
    bounds as (
        select
            max(timestamp) as max_ts,
            max(timestamp) - interval '48 hour' as window_start
        from {{ ref("fact_token_event") }}
    ),

    agg as (
        select
            app_id,
            count(*) as event_count,
            sum(num_bytes) as total_bytes,
            avg(num_bytes) as avg_bytes_per_event
        from {{ ref("fact_token_event") }} b
        cross join bounds w
        where b.timestamp >= w.window_start
        group by app_id
    )

select
    app_id,
    event_count,
    total_bytes,
    avg_bytes_per_event,
    (select window_start from bounds) as window_start,
    (select max_ts from bounds) as window_end
from agg
order by total_bytes desc nulls last, event_count desc
