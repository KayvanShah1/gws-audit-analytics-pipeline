{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set target_schema = target.schema -%}
    {%- if custom_schema_name is none or custom_schema_name | trim == '' -%}
        {{ target_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
