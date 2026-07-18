{#
    Generates a UNION ALL that unpivots the 22 wide competency columns into
    long form: one row per (respondent, competency). This is the SQL
    equivalent of the PySpark notebook's DataFrame.melt() call.

    Unlike the PySpark version, we don't need a runtime `split(col, ": ")` to
    recover dimension/competency, because Jinja already knows the exact
    strings at COMPILE time (from the competency_columns var) - so we just
    literal them directly. One fewer moving part than the Spark version.

    Usage:  {{ unpivot_scores(ref('stg_student'), 'student') }}
#}
{% macro unpivot_scores(source_relation, respondent_type) %}
    {% for col in var('competency_columns') %}
    select
        respondent_id,
        '{{ respondent_type }}' as respondent_type,
        supervisory_org,
        vp_org,
        _batch_id,
        _ingested_at,
        '{{ col.split(": ", 1)[0] | trim }}' as dimension,
        '{{ col.split(": ", 1)[1] | trim }}' as competency,
        case
            when try_cast(`{{ col }}` as double) between {{ var('scale_min') }} and {{ var('scale_max') }}
            then try_cast(`{{ col }}` as double)
            else null
        end as score
    from {{ source_relation }}
    {{ "union all" if not loop.last }}
    {% endfor %}
{% endmacro %}
