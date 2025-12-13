{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

with
    base as (

        select *
        from {{ ref("stg_saml_events") }}

        {% if is_incremental() %}

            where
                timestamp >= (
                    select coalesce(max(timestamp), timestamp '1970-01-01')
                    from {{ this }}
                )
                - interval 5 minute

        {% endif %}
    ),

    joined as (
        select
            -- event grain / time
            b.unique_id,
            b.timestamp,
            b.event_date,
            b.event_hour,
            extract(hour from b.timestamp) as hour_of_day,

            -- foreign keys
            u.user_key,
            i.ip_key,

            -- natural keys / debugging
            b.actor_profile_id,
            b.actor_email,
            b.ip_address,
            b.customer_id,

            -- geo
            b.region_code,
            b.subdivision_code,

            -- SAML-specific attributes
            b.orgunit_path,
            b.initiated_by,
            b.saml_sp_name,
            b.saml_status_code,
            b.saml_second_level_status_code,
            b.saml_failure_type,

            -- degenerate event attributes
            b.event_type,
            b.event_name
        from base b
        left join
            {{ ref("dim_user") }} u
            on u.actor_profile_id = b.actor_profile_id
            and u.actor_email = b.actor_email
        left join {{ ref("dim_ip_address") }} i on i.ip_address = b.ip_address
    )

select *
from joined
