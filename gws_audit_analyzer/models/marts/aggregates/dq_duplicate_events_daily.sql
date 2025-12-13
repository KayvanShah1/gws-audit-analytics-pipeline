{{ config(materialized="view") }}

with
    all_events as (
        select event_date, 'token' as source, unique_id
        from {{ ref("fact_token_event") }}
        union all
        select event_date, 'admin', unique_id
        from {{ ref("fact_admin_event") }}
        union all
        select event_date, 'login', unique_id
        from {{ ref("fact_login_event") }}
        union all
        select event_date, 'drive', unique_id
        from {{ ref("fact_drive_event") }}
        union all
        select event_date, 'saml', unique_id
        from {{ ref("fact_saml_event") }}
    ),

    agg as (
        select
            event_date,
            source,
            count(*) as total_rows,
            count(distinct unique_id) as distinct_event_ids,
            count(*) - count(distinct unique_id) as duplicate_rows
        from all_events
        group by event_date, source
    )

select *
from agg
where duplicate_rows > 0
