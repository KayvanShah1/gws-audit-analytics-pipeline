{{ config(materialized="view") }}

with
    scoped as (
        select s.scope_name, s.event_date
        from {{ ref("fact_token_scope") }} s
        join {{ ref("dim_scope") }} d on d.scope_name = s.scope_name
        where
            s.event_date >= current_date - interval '7 day'
            and (
                d.product_bucket in ('GSUITE_ADMIN', 'DRIVE', 'GMAIL')
                or d.service in ('admin', 'drive', 'gmail')
            )
    )

select scope_name, count(*) as scope_usages, count(distinct event_date) as active_days
from scoped
group by scope_name
order by scope_usages desc
