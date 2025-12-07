{{ config(materialized="table") }}

with
    union_app as (
        select client_id as app_id, app_name, client_type, api_name, product_buckets
        from {{ ref("stg_token_events") }}

        union
        select
            oauth_client_id as app_id,
            oauth_app_name as app_name,
            null as client_type,
            null as api_name,
            null as product_buckets
        from {{ ref("stg_admin_events") }}

        union
        select oauth_client_id as app_id, oauth_app_name as app_name, null, null, null
        from {{ ref("stg_drive_events") }}
    )
select
    {{ dbt_utils.generate_surrogate_key(["app_id"]) }} as app_key,
    app_id,
    max(app_name) as app_name,
    max(client_type) as client_type,
    max(api_name) as api_name,
{# max(product_buckets) AS product_buckets #}
from union_app
where app_id is not null
group by app_id
