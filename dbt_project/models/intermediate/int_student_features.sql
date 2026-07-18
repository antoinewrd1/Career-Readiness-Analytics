{#
    Replaces silver_student: cleaned experience attributes plus one-hot
    encoded categoricals. The category values themselves (exp_type,
    student_status) are discovered dynamically via one_hot_encode()'s
    run_query() call rather than hardcoded, so a new category appearing in a
    future survey wave gets its own column automatically on the next `dbt run`.
#}

select
    respondent_id,
    supervisory_org,
    vp_org,

    case
        when upper(trim(exp_paid_raw)) in ('YES', 'Y', 'TRUE', '1') then 1
        when upper(trim(exp_paid_raw)) in ('NO', 'N', 'FALSE', '0') then 0
        else null
    end as exp_paid,

    case
        when upper(trim(exp_credit_raw)) in ('YES', 'Y', 'TRUE', '1') then 1
        when upper(trim(exp_credit_raw)) in ('NO', 'N', 'FALSE', '0') then 0
        else null
    end as exp_for_credit,

    try_cast(exp_hours_raw as double) as exp_hours_per_week,
    try_cast(exp_weeks_raw as double) as exp_weeks,
    trim(exp_type_raw)      as exp_type,
    trim(student_status_raw) as student_status,

    {{ one_hot_encode('exp_type_raw', 'type', ref('stg_student')) }},
    {{ one_hot_encode('student_status_raw', 'status', ref('stg_student')) }}

from {{ ref('stg_student') }}
