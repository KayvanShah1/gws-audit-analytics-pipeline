{{
    config(
        materialized="incremental",
        unique_key="app_scope_drift_scope_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

{% set drift_lookback_days = 7 %}

-- Scope-level drift per app/day (added/removed scope keys)
with
    app_scope_daily as (
        select te.app_id, ts.scope_key, ts.event_date
        from {{ ref("fact_token_scope") }} ts
        join
            {{ ref("fact_token_event") }} te
            on te.unique_id = ts.unique_id
            and te.event_date = ts.event_date
        where
            te.app_id is not null and ts.scope_key is not null

            {% if is_incremental() %}
                and ts.event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval '{{ drift_lookback_days }} day'
            {% endif %}

        group by te.app_id, ts.scope_key, ts.event_date
    ),

    added as (
        select c.app_id, c.event_date, c.scope_key, 'added' as change_type
        from app_scope_daily c
        left join
            app_scope_daily p
            on p.app_id = c.app_id
            and p.scope_key = c.scope_key
            and p.event_date = c.event_date - interval 1 day
        where p.scope_key is null
    ),

    removed as (
        select
            p.app_id,
            p.event_date + interval 1 day as event_date,
            p.scope_key,
            'removed' as change_type
        from app_scope_daily p
        left join
            app_scope_daily c
            on c.app_id = p.app_id
            and c.scope_key = p.scope_key
            and c.event_date = p.event_date + interval 1 day
        where c.scope_key is null
    ),

    combined as (
        select *
        from added
        union all
        select *
        from removed
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            ["app_id", "event_date", "scope_key", "change_type"]
        )
    }} as app_scope_drift_scope_day_key, app_id, event_date, scope_key, change_type
from combined
where app_id is not null and event_date is not null and scope_key is not null
;
