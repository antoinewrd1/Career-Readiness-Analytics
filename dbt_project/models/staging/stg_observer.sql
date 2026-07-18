{#
    Staging: observer side. Same dedup pattern as stg_student.
    Note the trailing period on `VP/Supervisory Org.` - the real column name
    in the observer file, distinct from the student file's `VP/Supervisory Org`
    (no period). This mismatch is exactly what caused the case/exact-match
    debugging earlier in the project; here it's just one backtick-quoted
    literal, no ambiguity possible.
#}

with source as (
    select * from {{ source('bronze', 'bronze_observer') }}
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
            coalesce(cast(`Number of years you have been supervising student employees?` as string), '')
        ), 256) as row_hash
    from source
),

deduped as (
    select *
    from hashed
    qualify row_number() over (partition by row_hash order by _ingested_at asc) = 1
)

select
    concat('O_', lpad(cast(row_number() over (order by row_hash) as string), 5, '0')) as respondent_id,
    trim(`Supervisory Organization`) as supervisory_org,
    trim(`VP/Supervisory Org`)      as vp_org,
    _batch_id,
    _ingested_at,

    {% for col in var('competency_columns') %}
    `{{ col }}`{{ "," if not loop.last }}
    {% endfor %},

    `Number of years you have been supervising student employees?` as years_supervising_raw

from deduped
