{{
    config(
        materialized="incremental",
        unique_key="user_baseline_day_key",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

-- Daily user baseline (14d/30d) + novelty flags (NEW in last 60d)
-- Notes:
-- 1) Bytes are token-only metrics; all-source metrics are explicitly labeled.
-- 2) "New_*_count" is "first seen in the last 60 days" (incremental-safe).
{% set novelty_lookback_days = 60 %}
{% set rebuild_lookback_days = 75 %}  {# >= novelty_lookback + baseline window + buffer #}

with
    -- spine (all-source daily counts already modeled)
    user_events_daily as (
        select user_key, event_date, total_events
        from {{ ref("fact_user_activity_daily") }}
        where
            user_key is not null

            {% if is_incremental() %}
                and event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval '{{ rebuild_lookback_days }} day'
            {% endif %}
    ),

    -- token-only bytes + token-only distinct apps
    user_token_daily as (
        select
            user_key,
            event_date,
            sum(coalesce(num_bytes, 0)) as token_total_bytes,
            count(distinct app_id) as token_distinct_apps
        from {{ ref("fact_token_event") }}
        where
            user_key is not null

            {% if is_incremental() %}
                and event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval '{{ rebuild_lookback_days }} day'
            {% endif %}
        group by user_key, event_date
    ),

    -- all-source distinct apps (token/admin/drive; extend if other sources carry
    -- app_id)
    user_app_daily as (
        select user_key, event_date, count(distinct app_id) as all_source_distinct_apps
        from
            (
                select user_key, event_date, app_id
                from {{ ref("fact_token_event") }}
                where user_key is not null and app_id is not null

                union all
                select user_key, event_date, app_id
                from {{ ref("fact_admin_event") }}
                where user_key is not null and app_id is not null

                union all
                select user_key, event_date, app_id
                from {{ ref("fact_drive_event") }}
                where user_key is not null and app_id is not null
            ) x

        {% if is_incremental() %}
            where
                event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval '{{ rebuild_lookback_days }} day'
        {% endif %}
        group by user_key, event_date
    ),

    user_scope_daily as (
        select user_key, event_date, count(distinct scope_key) as token_distinct_scopes
        from {{ ref("fact_token_scope") }}
        where
            user_key is not null

            {% if is_incremental() %}
                and event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval '{{ rebuild_lookback_days }} day'
            {% endif %}
        group by user_key, event_date
    ),

    -- all-source ASN breadth (token/admin/login/drive/saml)
    user_asn_daily as (
        select
            e.user_key, e.event_date, count(distinct ip.asn) as all_source_distinct_asns
        from
            (
                select user_key, event_date, ip_key
                from {{ ref("fact_token_event") }}
                union all
                select user_key, event_date, ip_key
                from {{ ref("fact_admin_event") }}
                union all
                select user_key, event_date, ip_key
                from {{ ref("fact_login_event") }}
                union all
                select user_key, event_date, ip_key
                from {{ ref("fact_drive_event") }}
                union all
                select user_key, event_date, ip_key
                from {{ ref("fact_saml_event") }}
            ) e
        left join {{ ref("dim_ip_address") }} ip on ip.ip_key = e.ip_key
        where
            e.user_key is not null and ip.asn is not null

            {% if is_incremental() %}
                and e.event_date
                >= (select coalesce(max(event_date), date '1970-01-01') from {{ this }})
                - interval '{{ rebuild_lookback_days }} day'
            {% endif %}
        group by e.user_key, e.event_date
    ),

    user_daily as (
        select
            {{ dbt_utils.generate_surrogate_key(["u.user_key", "u.event_date"]) }}
            as user_baseline_day_key,
            u.user_key,
            u.event_date,

            u.total_events,

            coalesce(t.token_total_bytes, 0) as token_total_bytes,
            coalesce(t.token_distinct_apps, 0) as token_distinct_apps,

            coalesce(a.all_source_distinct_apps, 0) as all_source_distinct_apps,
            coalesce(s.token_distinct_scopes, 0) as token_distinct_scopes,
            coalesce(n.all_source_distinct_asns, 0) as all_source_distinct_asns
        from user_events_daily u
        left join
            user_token_daily t
            on t.user_key = u.user_key
            and t.event_date = u.event_date
        left join
            user_app_daily a on a.user_key = u.user_key and a.event_date = u.event_date
        left join
            user_scope_daily s
            on s.user_key = u.user_key
            and s.event_date = u.event_date
        left join
            user_asn_daily n on n.user_key = u.user_key and n.event_date = u.event_date
    ),

    -- novelty within last N days (incremental-safe)
    user_app_first_seen_n as (
        select user_key, app_id, min(event_date) as first_seen_date
        from
            (
                select user_key, app_id, event_date
                from {{ ref("fact_token_event") }}
                where user_key is not null and app_id is not null

                union all
                select user_key, app_id, event_date
                from {{ ref("fact_admin_event") }}
                where user_key is not null and app_id is not null

                union all
                select user_key, app_id, event_date
                from {{ ref("fact_drive_event") }}
                where user_key is not null and app_id is not null
            ) x
        where event_date >= (current_date - interval '{{ novelty_lookback_days }} day')
        group by user_key, app_id
    ),

    user_scope_first_seen_n as (
        select user_key, scope_key, min(event_date) as first_seen_date
        from {{ ref("fact_token_scope") }}
        where
            user_key is not null
            and event_date
            >= (current_date - interval '{{ novelty_lookback_days }} day')
        group by user_key, scope_key
    ),

    user_asn_first_seen_n as (
        select e.user_key, ip.asn, min(e.event_date) as first_seen_date
        from
            (
                select user_key, event_date, ip_key
                from {{ ref("fact_token_event") }}
                union all
                select user_key, event_date, ip_key
                from {{ ref("fact_admin_event") }}
                union all
                select user_key, event_date, ip_key
                from {{ ref("fact_login_event") }}
                union all
                select user_key, event_date, ip_key
                from {{ ref("fact_drive_event") }}
                union all
                select user_key, event_date, ip_key
                from {{ ref("fact_saml_event") }}
            ) e
        left join {{ ref("dim_ip_address") }} ip on ip.ip_key = e.ip_key
        where
            e.user_key is not null
            and ip.asn is not null
            and e.event_date
            >= (current_date - interval '{{ novelty_lookback_days }} day')
        group by e.user_key, ip.asn
    ),

    new_app_days as (
        select
            user_key,
            first_seen_date as event_date,
            count(distinct app_id) as new_apps_count_{{ novelty_lookback_days }}d
        from user_app_first_seen_n
        group by user_key, first_seen_date
    ),

    new_scope_days as (
        select
            user_key,
            first_seen_date as event_date,
            count(distinct scope_key) as new_scopes_count_{{ novelty_lookback_days }}d
        from user_scope_first_seen_n
        group by user_key, first_seen_date
    ),

    new_asn_days as (
        select
            user_key,
            first_seen_date as event_date,
            count(distinct asn) as new_asns_count_{{ novelty_lookback_days }}d
        from user_asn_first_seen_n
        group by user_key, first_seen_date
    ),

    windowed as (
        select
            u.*,

            coalesce(
                na.new_apps_count_{{ novelty_lookback_days }}d, 0
            ) as new_apps_count_{{ novelty_lookback_days }}d,
            coalesce(
                ns.new_scopes_count_{{ novelty_lookback_days }}d, 0
            ) as new_scopes_count_{{ novelty_lookback_days }}d,
            coalesce(
                nx.new_asns_count_{{ novelty_lookback_days }}d, 0
            ) as new_asns_count_{{ novelty_lookback_days }}d,

            avg(u.total_events) over (
                partition by u.user_key
                order by u.event_date
                rows between 14 preceding and 1 preceding
            ) as mean_events_14d,
            stddev_pop(u.total_events) over (
                partition by u.user_key
                order by u.event_date
                rows between 14 preceding and 1 preceding
            ) as std_events_14d,
            avg(u.total_events) over (
                partition by u.user_key
                order by u.event_date
                rows between 30 preceding and 1 preceding
            ) as mean_events_30d,
            stddev_pop(u.total_events) over (
                partition by u.user_key
                order by u.event_date
                rows between 30 preceding and 1 preceding
            ) as std_events_30d,

            avg(u.token_total_bytes) over (
                partition by u.user_key
                order by u.event_date
                rows between 14 preceding and 1 preceding
            ) as mean_token_bytes_14d,
            stddev_pop(u.token_total_bytes) over (
                partition by u.user_key
                order by u.event_date
                rows between 14 preceding and 1 preceding
            ) as std_token_bytes_14d,
            avg(u.token_total_bytes) over (
                partition by u.user_key
                order by u.event_date
                rows between 30 preceding and 1 preceding
            ) as mean_token_bytes_30d,
            stddev_pop(u.token_total_bytes) over (
                partition by u.user_key
                order by u.event_date
                rows between 30 preceding and 1 preceding
            ) as std_token_bytes_30d
        from user_daily u
        left join
            new_app_days na on na.user_key = u.user_key and na.event_date = u.event_date
        left join
            new_scope_days ns
            on ns.user_key = u.user_key
            and ns.event_date = u.event_date
        left join
            new_asn_days nx on nx.user_key = u.user_key and nx.event_date = u.event_date
    )

select
    user_baseline_day_key,
    user_key,
    event_date,

    total_events,

    token_total_bytes,
    token_distinct_apps,
    token_distinct_scopes,

    all_source_distinct_apps,
    all_source_distinct_asns,

    new_apps_count_{{ novelty_lookback_days }}d,
    new_scopes_count_{{ novelty_lookback_days }}d,
    new_asns_count_{{ novelty_lookback_days }}d,

    mean_events_14d,
    std_events_14d,
    case
        when std_events_14d is null or std_events_14d = 0
        then null
        else (total_events - mean_events_14d) / std_events_14d
    end as event_z_score_14d,
    case
        when mean_events_14d is null or mean_events_14d = 0
        then null
        else (total_events - mean_events_14d) / mean_events_14d
    end as event_pct_change_14d,

    mean_events_30d,
    std_events_30d,
    case
        when std_events_30d is null or std_events_30d = 0
        then null
        else (total_events - mean_events_30d) / std_events_30d
    end as event_z_score_30d,
    case
        when mean_events_30d is null or mean_events_30d = 0
        then null
        else (total_events - mean_events_30d) / mean_events_30d
    end as event_pct_change_30d,

    mean_token_bytes_14d,
    std_token_bytes_14d,
    case
        when std_token_bytes_14d is null or std_token_bytes_14d = 0
        then null
        else (token_total_bytes - mean_token_bytes_14d) / std_token_bytes_14d
    end as token_bytes_z_score_14d,
    case
        when mean_token_bytes_14d is null or mean_token_bytes_14d = 0
        then null
        else (token_total_bytes - mean_token_bytes_14d) / mean_token_bytes_14d
    end as token_bytes_pct_change_14d,

    mean_token_bytes_30d,
    std_token_bytes_30d,
    case
        when std_token_bytes_30d is null or std_token_bytes_30d = 0
        then null
        else (token_total_bytes - mean_token_bytes_30d) / std_token_bytes_30d
    end as token_bytes_z_score_30d,
    case
        when mean_token_bytes_30d is null or mean_token_bytes_30d = 0
        then null
        else (token_total_bytes - mean_token_bytes_30d) / mean_token_bytes_30d
    end as token_bytes_pct_change_30d
from windowed
;
