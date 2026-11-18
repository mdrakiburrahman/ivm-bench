-- catalog-override: Match DuckDB timestamp text for RisingWave surrogate keys
{% macro risingwave__generate_surrogate_key(field_list) %}
    {%- if var('surrogate_key_treat_nulls_as_empty_strings', False) -%}
        {%- set default_null_value = "" -%}
    {%- else -%}
        {%- set default_null_value = '_dbt_utils_surrogate_key_null_' -%}
    {%- endif -%}

    {%- set fields = [] -%}

    {%- for field in field_list -%}
        {%- if 'effective_timestamp' in (field | lower) -%}
            {%- if model.name in ['dim_account', 'dim_customer'] -%}
                {%- set cast_field = "rtrim(rtrim(to_char((" ~ field ~ " at time zone 'UTC'), 'YYYY-MM-DD HH24:MI:SS.US'), '0'), '.') || '+00'" -%}
            {%- else -%}
                {%- set cast_field = "rtrim(rtrim(to_char(" ~ field ~ ", 'YYYY-MM-DD HH24:MI:SS.US'), '0'), '.')" -%}
            {%- endif -%}
        {%- else -%}
            {%- set cast_field = "cast(" ~ field ~ " as " ~ dbt.type_string() ~ ")" -%}
        {%- endif -%}

        {%- do fields.append("coalesce(" ~ cast_field ~ ", '" ~ default_null_value ~ "')") -%}
        {%- if not loop.last %}
            {%- do fields.append("'-'") -%}
        {%- endif -%}
    {%- endfor -%}

    {{ dbt.hash(dbt.concat(fields)) }}
{% endmacro %}
