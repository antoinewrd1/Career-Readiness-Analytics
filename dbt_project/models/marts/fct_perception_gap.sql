-- Replaces gold_perception_gap: student mean minus supervisor mean per
-- dimension, with pooled-SD Cohen's d and a direction label.

with student as (
    select dimension, mean_score as student_mean, sd_score as student_sd, n_respondents as student_n
    from {{ ref('fct_dimension_summary') }}
    where respondent_type = 'student'
),

observer as (
    select dimension, mean_score as observer_mean, sd_score as observer_sd, n_respondents as observer_n
    from {{ ref('fct_dimension_summary') }}
    where respondent_type = 'observer'
),

joined as (
    select
        s.dimension,
        s.student_mean, s.student_sd, s.student_n,
        o.observer_mean, o.observer_sd, o.observer_n,
        round(s.student_mean - o.observer_mean, 3) as gap,
        sqrt(
            ((s.student_n - 1) * power(s.student_sd, 2) + (o.observer_n - 1) * power(o.observer_sd, 2))
            / (s.student_n + o.observer_n - 2)
        ) as pooled_sd
    from student s
    join observer o on s.dimension = o.dimension
)

select
    dimension,
    student_mean, student_sd, student_n,
    observer_mean, observer_sd, observer_n,
    gap,
    pooled_sd,
    round(gap / pooled_sd, 3) as cohens_d,
    case
        when gap > 0 then 'student over-rates'
        when gap < 0 then 'student under-rates'
        else 'aligned'
    end as direction
from joined
order by abs(gap) desc
