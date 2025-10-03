{% macro safe_source(source_name, table_name) %}
    {# zjisti relation v DWH #}
    {% set rel = adapter.get_relation(
        database=target.database,
        schema=source_name,
        identifier=table_name
    ) %}

    {% if rel %}
        {{ source(source_name, table_name) }}
    {% else %}
        (
            select 1 as dummy
            where 1=0
        )
    {% endif %}
{% endmacro %}