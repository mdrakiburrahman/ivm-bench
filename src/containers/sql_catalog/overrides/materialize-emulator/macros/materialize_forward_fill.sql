-- catalog-override: Materialize needs an explicitly ordered filtered array to implement Spark's null-aware LAST_VALUE.
{% macro materialize_forward_fill(column) %}
array_agg({{ column }} order by effective_timestamp nulls first, status nulls first, account_id nulls first)
    filter (where {{ column }} is not null) over (
        partition by customer_id order by effective_timestamp nulls first, status nulls first, account_id nulls first
    )
{% endmacro %}
