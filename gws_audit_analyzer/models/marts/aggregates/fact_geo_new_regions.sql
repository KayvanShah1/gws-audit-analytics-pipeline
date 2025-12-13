{{ config(materialized="view") }}

with
    events as (
        select event_date, user_key, region_code, subdivision_code
        from {{ ref("fact_token_event") }}
        union all
        select event_date, user_key, region_code, subdivision_code
        from {{ ref("fact_admin_event") }}
        union all
        select event_date, user_key, region_code, subdivision_code
        from {{ ref("fact_login_event") }}
        union all
        select event_date, user_key, region_code, subdivision_code
        from {{ ref("fact_drive_event") }}
        union all
        select event_date, user_key, region_code, subdivision_code
        from {{ ref("fact_saml_event") }}
    ),

    filtered as (select * from events where region_code is not null),

    first_seen as (
        select
            user_key, region_code, subdivision_code, min(event_date) as first_seen_date
        from filtered
        group by user_key, region_code, subdivision_code
    )

select f.user_key, f.region_code, f.subdivision_code, f.first_seen_date
from first_seen f
where f.first_seen_date >= current_date - interval '30 day'
