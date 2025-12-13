{{ config(materialized="view") }}

select
    event_date,
    caller_type,
    impersonation,
    total_events,
    distinct_events,
    distinct_users
from {{ ref("fact_admin_action_breakdown_daily") }}
where event_date >= current_date - interval '2 day'
order by event_date desc, total_events desc
