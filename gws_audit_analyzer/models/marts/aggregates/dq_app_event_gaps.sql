{{ config(materialized="view") }}

with
    date_range as (
        select date_day
        from {{ ref("dim_time") }}
        where date_day >= current_date - interval '30 day'
    ),

    apps as (select distinct app_id from {{ ref("fact_app_activity_daily") }}),

    cal as (
        select a.app_id, d.date_day as event_date from apps a cross join date_range d
    ),

    observed as (
        select app_id, event_date, total_events
        from {{ ref("fact_app_activity_daily") }}
        where event_date >= current_date - interval '30 day'
    )

select c.app_id, c.event_date, coalesce(o.total_events, 0) as total_events
from cal c
left join observed o on o.app_id = c.app_id and o.event_date = c.event_date
where coalesce(o.total_events, 0) = 0
