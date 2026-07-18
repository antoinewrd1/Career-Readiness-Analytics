-- Replaces gold_competency_summary: mean/median/sd/n per competency.

select
    respondent_type,
    dimension,
    competency,
    count(*)                                    as n,
    round(avg(score), 3)                        as mean_score,
    round(stddev(score), 3)                     as sd_score,
    round(percentile_approx(score, 0.5), 3)     as median_score,
    min(score)                                  as min_score,
    max(score)                                  as max_score
from {{ ref('int_scores_long') }}
where score is not null
group by respondent_type, dimension, competency
