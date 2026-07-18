-- Replaces gold_dimension_summary: mean/median/sd/n per dimension, aggregated
-- from the respondent-level mart (not directly from int_scores_long) so the
-- unit of analysis stays the respondent.

select
    respondent_type,
    dimension,
    count(*)                                                    as n_respondents,
    round(avg(dim_score), 3)                                    as mean_score,
    round(stddev(dim_score), 3)                                 as sd_score,
    round(percentile_approx(dim_score, 0.5), 3)                 as median_score,
    round(avg(dim_score) / {{ var('scale_max') }} * 100, 1)     as pct_of_max
from {{ ref('fct_respondent_dimension') }}
group by respondent_type, dimension
