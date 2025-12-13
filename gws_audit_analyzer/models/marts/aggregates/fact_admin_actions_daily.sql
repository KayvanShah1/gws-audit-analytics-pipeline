{{
    config(
        materialized="incremental",
        unique_key="admin_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

with
    base as (
        select
            event_date,
            app_id,
            user_key,
            unique_id as event_id,
            impersonation,
            caller_type
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
                    ["coalesce(app_id, 'unknown')", "event_date"]
                )
            }} as admin_day_key,

            app_id,
            event_date,

            count(*) as total_events,
            count(distinct user_key) as distinct_users,
            count(distinct event_id) as distinct_events,
            count_if(impersonation) as impersonation_events,
            count_if(caller_type is not null) as caller_type_present
        from base
        group by app_id, event_date
    )

select *
from agg
