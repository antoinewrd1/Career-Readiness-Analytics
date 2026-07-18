{#
    Staging: 1:1 with the Bronze source, light cleaning only (dedup, key
    generation, column trimming). No joins, no aggregation - that discipline
    is what keeps staging models easy to reason about.

    QUALIFY is Databricks SQL's shortcut for "filter on a window function
    without wrapping the query in a subquery" - it replaces the PySpark
    version's separate row_number() -> filter(rn == 1) -> drop(rn) sequence
    with one clause.
#}

with source as (
    select * from {{ source('bronze', 'bronze_student') }}
),

hashed as (
    select
        *,
        sha2(concat_ws('||',
            {% for col in var('competency_columns') %}
            coalesce(cast(`{{ col }}` as string), ''),
            {% endfor %}
            coalesce(cast(`Supervisory Organization` as string), ''),
            coalesce(cast(`VP/Supervisory Org` as string), ''),
            coalesce(cast(`Experience Description: Please specify the type of assessment this submission should be considered.` as string), ''),
            coalesce(cast(`Experience Description: If taking this assessment as part of a learning opportunity, please specify its identity. For example if the opportunity is a course, specify the course ID.` as string), ''),
            coalesce(cast(`Experience Description: Which of the following best describes your current or most recent experiential learning opportunity?` as string), ''),
            coalesce(cast(`Experience Description: Was/is your experiential learning opportunity paid?` as string), ''),
            coalesce(cast(`Experience Description: Was/is your experiential learning opportunity for credit?` as string), ''),
            coalesce(cast(`Experience Description: On average, how many hours per week do (or did) you participate in your experiential learning opportunity?` as string), ''),
            coalesce(cast(`Experience Description: What is the total number of weeks you will (or did) participate in your experiential learning opportunity?` as string), ''),
            coalesce(cast(`Please select one of the following. Are you...` as string), '')
        ), 256) as row_hash
    from source
),

deduped as (
    select *
    from hashed
    qualify row_number() over (partition by row_hash order by _ingested_at asc) = 1
)

select
    concat('S_', lpad(cast(row_number() over (order by row_hash) as string), 5, '0')) as respondent_id,
    trim(`Supervisory Organization`) as supervisory_org,
    trim(`VP/Supervisory Org`)       as vp_org,
    _batch_id,
    _ingested_at,

    -- pass through the 22 score columns unchanged; validation happens in int_scores_long
    {% for col in var('competency_columns') %}
    `{{ col }}`{{ "," if not loop.last }}
    {% endfor %},

    -- experience attributes, kept as raw text; int_student_features cleans/encodes them
    `Experience Description: Which of the following best describes your current or most recent experiential learning opportunity?` as exp_type_raw,
    `Experience Description: Was/is your experiential learning opportunity paid?`                                                    as exp_paid_raw,
    `Experience Description: Was/is your experiential learning opportunity for credit?`                                             as exp_credit_raw,
    `Experience Description: On average, how many hours per week do (or did) you participate in your experiential learning opportunity?` as exp_hours_raw,
    `Experience Description: What is the total number of weeks you will (or did) participate in your experiential learning opportunity?`  as exp_weeks_raw,
    `Please select one of the following. Are you...` as student_status_raw

from deduped
