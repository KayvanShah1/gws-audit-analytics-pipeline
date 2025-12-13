{{ config(materialized="view") }}

with src as (select * from {{ source("processed", "saml_events") }})
select
    timestamp,
    date_trunc('day', timestamp) as event_date,
    date_trunc('hour', timestamp) as event_hour,
    unique_id,
    application_name,
    customer_id,
    actor_email,
    actor_profile_id,
    ip_address,
    asn,
    asn_list,
    region_code,
    subdivision_code,
    event_type,
    event_name,
    saml_sp_name,
    orgunit_path,
    initiated_by,
    saml_status_code,
    saml_second_level_status_code,
    saml_failure_type
from src
