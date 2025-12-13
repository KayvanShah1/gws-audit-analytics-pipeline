{{ config(materialized="view") }}

select
    event_date,
    region_code,
    subdivision_code,
    asn,
    total_events,
    distinct_events,
    distinct_users,
    token_events,
    admin_events,
    login_events,
    drive_events,
    saml_events
from {{ ref("fact_geo_activity_daily") }}
where event_date >= current_date - interval '2 day'
order by event_date desc, total_events desc
