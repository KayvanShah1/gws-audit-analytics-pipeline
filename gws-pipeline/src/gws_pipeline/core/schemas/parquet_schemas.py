from __future__ import annotations

import pyarrow as pa
from gws_pipeline.core.schemas.fetcher import Application

# Reusable field definitions (all columns are nullable to match API variability)
_FIELDS = {
    # Base audit metadata
    "timestamp": pa.field("timestamp", pa.timestamp("us", tz="UTC")),
    "unique_id": pa.field("unique_id", pa.string()),
    "application_name": pa.field("application_name", pa.string()),
    "customer_id": pa.field("customer_id", pa.string()),
    "actor_email": pa.field("actor_email", pa.string()),
    "actor_profile_id": pa.field("actor_profile_id", pa.string()),
    "caller_type": pa.field("caller_type", pa.string()),
    "oauth_client_id": pa.field("oauth_client_id", pa.string()),
    "oauth_app_name": pa.field("oauth_app_name", pa.string()),
    "impersonation": pa.field("impersonation", pa.bool_()),
    "ip_address": pa.field("ip_address", pa.string()),
    "asn": pa.field("asn", pa.int64()),
    "asn_list": pa.field("asn_list", pa.list_(pa.int64())),
    "region_code": pa.field("region_code", pa.string()),
    "subdivision_code": pa.field("subdivision_code", pa.string()),
    "event_type": pa.field("event_type", pa.string()),
    "event_name": pa.field("event_name", pa.string()),
    "resource_ids": pa.field("resource_ids", pa.list_(pa.string())),
    "resource_detail_count": pa.field("resource_detail_count", pa.int64()),
    # Token-specific
    "method_name": pa.field("method_name", pa.string()),
    "num_bytes": pa.field("num_bytes", pa.int64()),
    "api_name": pa.field("api_name", pa.string()),
    "client_id": pa.field("client_id", pa.string()),
    "app_name": pa.field("app_name", pa.string()),
    "client_type": pa.field("client_type", pa.string()),
    "scope_count": pa.field("scope_count", pa.int64()),
    "product_buckets": pa.field("product_buckets", pa.list_(pa.string())),
    "has_drive_scope": pa.field("has_drive_scope", pa.bool_()),
    "has_gmail_scope": pa.field("has_gmail_scope", pa.bool_()),
    "has_admin_scope": pa.field("has_admin_scope", pa.bool_()),
    # Login-specific
    "login_type": pa.field("login_type", pa.string()),
    "login_challenge_methods": pa.field("login_challenge_methods", pa.list_(pa.string())),
    "is_suspicious": pa.field("is_suspicious", pa.bool_()),
    # Drive-specific
    "user_query": pa.field("user_query", pa.string()),
    "parsed_query": pa.field("parsed_query", pa.string()),
    "primary_event": pa.field("primary_event", pa.bool_()),
    "billable": pa.field("billable", pa.bool_()),
    "originating_app_id": pa.field("originating_app_id", pa.string()),
    "actor_is_collaborator_account": pa.field("actor_is_collaborator_account", pa.bool_()),
    "resource_id_count": pa.field("resource_id_count", pa.int64()),
    "applied_label_count": pa.field("applied_label_count", pa.int64()),
    "applied_label_titles": pa.field("applied_label_titles", pa.list_(pa.string())),
    # SAML-specific
    "orgunit_path": pa.field("orgunit_path", pa.string()),
    "initiated_by": pa.field("initiated_by", pa.string()),
    "saml_status_code": pa.field("saml_status_code", pa.string()),
    "saml_second_level_status_code": pa.field("saml_second_level_status_code", pa.string()),
    "saml_failure_type": pa.field("saml_failure_type", pa.string()),
}


def _schema(names: list[str]) -> pa.Schema:
    return pa.schema([_FIELDS[name] for name in names])


# Event schemas by application
TOKEN_EVENT_SCHEMA = _schema(
    [
        "timestamp",
        "unique_id",
        "application_name",
        "customer_id",
        "actor_email",
        "actor_profile_id",
        "ip_address",
        "asn",
        "asn_list",
        "region_code",
        "subdivision_code",
        "event_type",
        "event_name",
        "resource_detail_count",
        "method_name",
        "num_bytes",
        "api_name",
        "client_id",
        "app_name",
        "client_type",
        "scope_count",
        "product_buckets",
        "has_drive_scope",
        "has_gmail_scope",
        "has_admin_scope",
    ]
)

ADMIN_EVENT_SCHEMA = _schema(
    [
        "timestamp",
        "unique_id",
        "application_name",
        "customer_id",
        "actor_email",
        "actor_profile_id",
        "caller_type",
        "oauth_client_id",
        "oauth_app_name",
        "impersonation",
        "ip_address",
        "asn",
        "asn_list",
        "region_code",
        "subdivision_code",
        "event_type",
        "event_name",
        "resource_detail_count",
    ]
)

LOGIN_EVENT_SCHEMA = _schema(
    [
        "timestamp",
        "unique_id",
        "application_name",
        "customer_id",
        "actor_email",
        "actor_profile_id",
        "ip_address",
        "asn",
        "asn_list",
        "region_code",
        "subdivision_code",
        "event_type",
        "event_name",
        "resource_ids",
        "resource_detail_count",
        "login_type",
        "login_challenge_methods",
        "is_suspicious",
    ]
)

DRIVE_EVENT_SCHEMA = _schema(
    [
        "timestamp",
        "unique_id",
        "application_name",
        "customer_id",
        "actor_email",
        "actor_profile_id",
        "oauth_client_id",
        "oauth_app_name",
        "impersonation",
        "ip_address",
        "asn",
        "asn_list",
        "region_code",
        "subdivision_code",
        "event_type",
        "event_name",
        "resource_ids",
        "resource_detail_count",
        "user_query",
        "parsed_query",
        "primary_event",
        "billable",
        "originating_app_id",
        "actor_is_collaborator_account",
        "resource_id_count",
        "applied_label_count",
        "applied_label_titles",
    ]
)

SAML_EVENT_SCHEMA = _schema(
    [
        "timestamp",
        "unique_id",
        "application_name",
        "customer_id",
        "actor_email",
        "actor_profile_id",
        "ip_address",
        "asn",
        "asn_list",
        "region_code",
        "subdivision_code",
        "event_type",
        "event_name",
        "resource_detail_count",
        "orgunit_path",
        "initiated_by",
        "saml_status_code",
        "saml_second_level_status_code",
        "saml_failure_type",
    ]
)


EVENT_SCHEMA_BY_APP: dict[Application, pa.Schema] = {
    Application.TOKEN: TOKEN_EVENT_SCHEMA,
    Application.ADMIN: ADMIN_EVENT_SCHEMA,
    Application.LOGIN: LOGIN_EVENT_SCHEMA,
    Application.DRIVE: DRIVE_EVENT_SCHEMA,
    Application.SAML: SAML_EVENT_SCHEMA,
}


# Scope schemas (token is the only app that emits per-scope rows)
TOKEN_SCOPES_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("unique_id", pa.string()),
        ("scope_name", pa.string()),
        ("scope_family", pa.string()),
        ("product_bucket", pa.string()),
        ("service", pa.string()),
        ("is_readonly", pa.bool_()),
    ]
)

SCOPE_SCHEMA_BY_APP: dict[Application, pa.Schema] = {
    Application.TOKEN: TOKEN_SCOPES_SCHEMA,
}


__all__ = [
    "ADMIN_EVENT_SCHEMA",
    "DRIVE_EVENT_SCHEMA",
    "EVENT_SCHEMA_BY_APP",
    "LOGIN_EVENT_SCHEMA",
    "SAML_EVENT_SCHEMA",
    "SCOPE_SCHEMA_BY_APP",
    "TOKEN_EVENT_SCHEMA",
    "TOKEN_SCOPES_SCHEMA",
]
