{#-- Fabric classic (non-schema) lakehouse: every model materializes into the
     single lakehouse database, so collapse all custom per-layer schemas
     (bronze/silver/gold/work) onto target.schema (== the lakehouse name).
     Model names are unique across layers, so there are no collisions. --#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ target.schema }}
{%- endmacro %}
