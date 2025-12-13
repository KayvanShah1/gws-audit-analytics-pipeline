{{ config(materialized="view") }}

with
    base as (
        select *
        from {{ ref("fact_event_volume_hourly") }}
        where event_hour >= current_timestamp - interval '7 day'
    ),

    stats as (
        select
            source,
            avg(total_events) as mean_events,
            stddev_pop(total_events) as std_events
        from base
        group by source
    ),

    recent as (
        select * from base where event_hour >= current_timestamp - interval '48 hour'
    )

select
    r.source,
    r.event_hour,
    r.event_date,
    r.total_events,
    r.distinct_events,
    s.mean_events,
    s.std_events,
    case
        when s.std_events is null or s.std_events = 0
        then 0
        else (r.total_events - s.mean_events) / s.std_events
    end as z_score
from recent r
join stats s on s.source = r.source
where
    s.std_events is not null
    and s.std_events > 0
    and r.total_events > s.mean_events + (3 * s.std_events)
order by r.event_hour desc, r.source
