{#
    The keystone table, exactly as in the PySpark version: one row per
    respondent x competency. Everything downstream (aggregations, rankings,
    the perception gap) is a single query against this one table.

    This replaces silver_scores. Note what's ABSENT compared to the PySpark
    notebook: no safe-rename layer, no col_000 -> raw mapping, no runtime
    split(competency_full, ": "). Backtick-quoted identifiers and
    compile-time Jinja string literals make the whole workaround unnecessary.
#}

{{ unpivot_scores(ref('stg_student'), 'student') }}

union all

{{ unpivot_scores(ref('stg_observer'), 'observer') }}
