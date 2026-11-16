-- catalog-override: Materialize cannot parse dates using Spark's yyyyMMdd format argument.
{% macro materialize_finwire_date(value) %}
try_cast(substring({{ value }}, 1, 4) || '-' || substring({{ value }}, 5, 2) || '-' || substring({{ value }}, 7, 2) as date)
{% endmacro %}
