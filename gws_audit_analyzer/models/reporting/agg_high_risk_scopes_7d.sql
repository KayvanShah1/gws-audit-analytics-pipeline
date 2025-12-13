{{ config(materialized="view") }}

with
    recent as (
        select *
        from {{ ref("fact_scope_risk_daily") }}
        where event_date >= current_date - interval '7 day'
    )

select
    sum(total_scope_usages) as total_scope_usages,
    sum(distinct_events) as distinct_events,
    sum(distinct_users) as distinct_users,
    sum(high_risk_scope_usages) as high_risk_scope_usages,
    sum(high_risk_events) as high_risk_events,
    case
        when sum(total_scope_usages) = 0
        then null
        else sum(high_risk_scope_usages) * 1.0 / sum(total_scope_usages)
    end as pct_high_risk_scopes,
    case
        when sum(distinct_events) = 0
        then null
        else sum(high_risk_events) * 1.0 / sum(distinct_events)
    end as pct_high_risk_events,
    min(event_date) as window_start,
    max(event_date) as window_end
from recent
