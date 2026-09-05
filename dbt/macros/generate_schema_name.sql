{#
  Standard dbt-labs override: use the model's custom schema exactly (e.g.
  "staging", "silver", "gold") instead of dbt's default of appending it to
  the connection's target schema ("<target_schema>_staging"). Keeps the
  warehouse's schema names matching the bronze/silver/gold layout the
  README documents, regardless of which dbt target schema is configured.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
