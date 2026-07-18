-- Replaces gold_respondent_dimension. Averaging within respondent BEFORE
-- aggregating across respondents is what makes downstream effect sizes
-- (Cohen's d) count independent people rather than inflated item counts.

select
    respondent_id,
    respondent_type,
    supervisory_org,
    vp_org,
    dimension,
    avg(score) as dim_score
from {{ ref('int_scores_long') }}
where score is not null
group by respondent_id, respondent_type, supervisory_org, vp_org, dimension
