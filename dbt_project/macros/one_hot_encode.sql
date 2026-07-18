{% macro one_hot_encode(column_name, prefix, source_relation) %}
    {% if execute %}
        {% set query %}
            select distinct {{ column_name }} as val
            from {{ source_relation }}
            where {{ column_name }} is not null and trim({{ column_name }}) != ''
            order by 1
        {% endset %}
        {% set results = run_query(query) %}
        {% set values = results.columns[0].values() %}
    {% else %}
        {% set values = [] %}
    {% endif %}

    {% for v in values %}
        {% set safe = v | lower
                         | replace(' ', '_')
                         | replace('-', '_')
                         | replace(',', '')
                         | replace('/', '_')
                         | replace('(', '')
                         | replace(')', '')
                         | replace('.', '') %}
    case when trim({{ column_name }}) = '{{ v | replace("'", "''") }}'
         then 1 else 0 end as `{{ prefix }}_{{ safe }}`
    {{ "," if not loop.last }}
    {% endfor %}
{% endmacro %}