{{ config(materialized="table") }}

with
    union_ip as (
        select ip_address, asn, asn_list, region_code, subdivision_code
        from {{ ref("stg_token_events") }}
        union
        select ip_address, asn, asn_list, region_code, subdivision_code
        from {{ ref("stg_admin_events") }}
        union
        select ip_address, asn, asn_list, region_code, subdivision_code
        from {{ ref("stg_login_events") }}
        union
        select ip_address, asn, asn_list, region_code, subdivision_code
        from {{ ref("stg_drive_events") }}
        union
        select ip_address, asn, asn_list, region_code, subdivision_code
        from {{ ref("stg_saml_events") }}
    ),

    -- Explode all ASN candidates (from asn_list or fallback to asn)
    candidates as (
        select
            ip_address, region_code, subdivision_code, asn as primary_asn, asn_candidate
        from union_ip
        cross join
            unnest(
                case
                    when asn_list is not null
                    then asn_list  -- list from event
                    when asn is not null
                    then [asn]  -- fallback: single ASN
                    else []  -- no ASN info
                end
            ) as t(asn_candidate)
    )

select
    {{ dbt_utils.generate_surrogate_key(["ip_address"]) }} as ip_key,
    ip_address,

    -- canonical ASN: prefer any non-zero candidate; otherwise fallback to primary_asn
    -- if non-zero; else NULL
    coalesce(
        max(case when asn_candidate <> 0 then asn_candidate end),
        nullif(max(primary_asn), 0)
    ) as asn,

    max(region_code) as region_code,
    max(subdivision_code) as subdivision_code,

    -- flag if we ever saw ASN=0 (unknown) for this IP
    bool_or(
        asn_candidate = 0 or primary_asn = 0 or primary_asn is null
    ) as has_unknown_asn
from candidates
where ip_address is not null
group by ip_address
