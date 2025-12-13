{{
    config(
        materialized="incremental",
        unique_key="admin_breakdown_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

with
    base as (
        select event_date, caller_type, impersonation, unique_id as event_id, user_key
        from {{ ref("fact_admin_event") }}

        {% if is_incremental() %}
            where
                event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval 1 day
        {% endif %}
    ),

    agg as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "coalesce(caller_type, 'unknown')",
                        "event_date",
                        "impersonation::varchar",
                    ]
                )
            }} as admin_breakdown_day_key,

            event_date,
            caller_type,
            impersonation,
            count(*) as total_events,
            count(distinct event_id) as distinct_events,
            count(distinct user_key) as distinct_users
        from base
        group by event_date, caller_type, impersonation
    )

select *
from agg
