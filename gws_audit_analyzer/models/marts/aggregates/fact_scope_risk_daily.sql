{{
    config(
        materialized="incremental",
        unique_key="scope_risk_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

with
    scopes as (
        select
            s.event_date,
            s.unique_id as event_id,
            s.user_key,
            s.product_bucket,
            s.service
        from {{ ref("fact_token_scope") }} s

        {% if is_incremental() %}
            where
                s.event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval 1 day
        {% endif %}
    ),

    flagged as (
        select
            event_date,
            event_id,
            user_key,
            product_bucket,
            service,
            case
                when product_bucket in ('GSUITE_ADMIN', 'DRIVE', 'GMAIL')
                then true
                when service in ('admin', 'drive', 'gmail')
                then true
                else false
            end as is_high_risk
        from scopes
    ),

    agg as (
        select
            {{ dbt_utils.generate_surrogate_key(["event_date"]) }}
            as scope_risk_day_key,
            event_date,
            count(*) as total_scope_usages,
            count(distinct event_id) as distinct_events,
            count(distinct user_key) as distinct_users,
            count_if(is_high_risk) as high_risk_scope_usages,
            count(distinct case when is_high_risk then event_id end) as high_risk_events
        from flagged
        group by event_date
    )

select *
from agg
